from __future__ import annotations

"""Data quality validators for PySpark DataFrames."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    isnan,
    isnull,
    lit,
    avg,
    stddev,
    max as spark_max,
    min as spark_min,
    current_timestamp,
    abs as spark_abs,
)

logger = logging.getLogger("validators")


@dataclass
class CheckResult:
    """Result of a single quality check."""
    table_name: str
    check_name: str
    check_type: str
    status: str          # PASS, FAIL, WARN
    records_checked: int
    records_failed: int
    failure_percentage: float
    details: str
    severity: str        # critical, warning, info

    def is_critical_failure(self) -> bool:
        return self.status == "FAIL" and self.severity == "critical"


class DataValidator:
    """Runs data quality checks against PySpark DataFrames.
    Each check method takes (df, config) and returns a list of CheckResult."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def check_not_null(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that specified columns have no NULL values."""
        results = []
        columns = config["columns"]
        table = config.get("_table", "unknown")
        severity = config.get("severity", "critical")

        total = df.count()

        for column in columns:
            null_count = df.filter(col(column).isNull()).count()
            pct = (null_count / total * 100) if total > 0 else 0

            results.append(CheckResult(
                table_name=table,
                check_name=f"not_null_{column}",
                check_type="not_null",
                status="FAIL" if null_count > 0 else "PASS",
                records_checked=total,
                records_failed=null_count,
                failure_percentage=round(pct, 2),
                details=f"Column '{column}': {null_count} NULL values out of {total} rows",
                severity=severity,
            ))

        return results

    def check_accepted_values(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that a column only contains values from an allowed list."""
        column = config["column"]
        values = config["values"]
        table = config.get("_table", "unknown")
        severity = config.get("severity", "warning")

        total = df.count()
        invalid_count = df.filter(
            ~col(column).isin(values) & col(column).isNotNull()
        ).count()
        pct = (invalid_count / total * 100) if total > 0 else 0

        # grab a few bad values for the log message
        invalid_sample = []
        if invalid_count > 0:
            samples = (
                df.filter(~col(column).isin(values) & col(column).isNotNull())
                .select(column).distinct().limit(5).collect()
            )
            invalid_sample = [row[0] for row in samples]

        return [CheckResult(
            table_name=table,
            check_name=f"accepted_values_{column}",
            check_type="accepted_values",
            status="FAIL" if invalid_count > 0 else "PASS",
            records_checked=total,
            records_failed=invalid_count,
            failure_percentage=round(pct, 2),
            details=f"Column '{column}': {invalid_count} invalid values. Samples: {invalid_sample}",
            severity=severity,
        )]

    def check_uniqueness(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that a column has no duplicate values."""
        column = config["column"]
        table = config.get("_table", "unknown")
        severity = config.get("severity", "critical")

        total = df.count()
        distinct_count = df.select(column).distinct().count()
        duplicate_count = total - distinct_count
        pct = (duplicate_count / total * 100) if total > 0 else 0

        return [CheckResult(
            table_name=table,
            check_name=f"uniqueness_{column}",
            check_type="uniqueness",
            status="FAIL" if duplicate_count > 0 else "PASS",
            records_checked=total,
            records_failed=duplicate_count,
            failure_percentage=round(pct, 2),
            details=f"Column '{column}': {distinct_count} distinct / {total} total ({duplicate_count} duplicates)",
            severity=severity,
        )]

    def check_range(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that numeric values fall within min/max bounds."""
        column = config["column"]
        min_val = config.get("min")
        max_val = config.get("max")
        table = config.get("_table", "unknown")
        severity = config.get("severity", "warning")

        total = df.filter(col(column).isNotNull()).count()

        conditions = []
        if min_val is not None:
            conditions.append(col(column) < min_val)
        if max_val is not None:
            conditions.append(col(column) > max_val)

        if not conditions:
            return []

        from functools import reduce
        from operator import or_
        combined = reduce(or_, conditions)

        out_of_range = df.filter(col(column).isNotNull()).filter(combined).count()
        pct = (out_of_range / total * 100) if total > 0 else 0

        return [CheckResult(
            table_name=table,
            check_name=f"range_{column}",
            check_type="range_check",
            status="FAIL" if out_of_range > 0 else "PASS",
            records_checked=total,
            records_failed=out_of_range,
            failure_percentage=round(pct, 2),
            details=f"Column '{column}': {out_of_range} values outside [{min_val}, {max_val}]",
            severity=severity,
        )]

    def check_freshness(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that the most recent record is within the freshness SLA."""
        column = config["column"]
        max_age_hours = config["max_age_hours"]
        table = config.get("_table", "unknown")
        severity = config.get("severity", "warning")

        total = df.count()
        max_ts_row = df.select(spark_max(col(column)).alias("max_ts")).collect()

        if not max_ts_row or max_ts_row[0]["max_ts"] is None:
            return [CheckResult(
                table_name=table,
                check_name=f"freshness_{column}",
                check_type="freshness",
                status="FAIL",
                records_checked=total,
                records_failed=total,
                failure_percentage=100.0,
                details=f"Column '{column}': no data found (table empty)",
                severity=severity,
            )]

        max_ts = max_ts_row[0]["max_ts"]
        now = datetime.now(timezone.utc)

        # handle both datetime and date objects from JDBC
        if hasattr(max_ts, 'hour'):
            age_hours = (now - max_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        else:
            from datetime import datetime as dt
            max_dt = dt.combine(max_ts, dt.min.time()).replace(tzinfo=timezone.utc)
            age_hours = (now - max_dt).total_seconds() / 3600

        is_fresh = age_hours <= max_age_hours

        return [CheckResult(
            table_name=table,
            check_name=f"freshness_{column}",
            check_type="freshness",
            status="PASS" if is_fresh else "FAIL",
            records_checked=total,
            records_failed=0 if is_fresh else total,
            failure_percentage=0.0 if is_fresh else 100.0,
            details=f"Column '{column}': latest record is {age_hours:.1f}h old (SLA: {max_age_hours}h)",
            severity=severity,
        )]

    def check_referential_integrity(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that FK values exist in the referenced table."""
        column = config["column"]
        ref_table = config["reference_table"]
        ref_column = config["reference_column"]
        table = config.get("_table", "unknown")
        severity = config.get("severity", "warning")

        from src.common.db_utils import get_jdbc_url, get_jdbc_properties
        ref_df = (
            self.spark.read.format("jdbc")
            .option("url", get_jdbc_url())
            .option("dbtable", ref_table)
            .options(**get_jdbc_properties())
            .load()
            .select(col(ref_column).alias("_ref_value"))
            .distinct()
        )

        total = df.select(column).distinct().count()
        orphans = df.select(column).distinct().join(
            ref_df, df[column] == ref_df["_ref_value"], "left_anti"
        ).count()
        pct = (orphans / total * 100) if total > 0 else 0

        return [CheckResult(
            table_name=table,
            check_name=f"ref_integrity_{column}_to_{ref_table}.{ref_column}",
            check_type="referential_integrity",
            status="FAIL" if orphans > 0 else "PASS",
            records_checked=total,
            records_failed=orphans,
            failure_percentage=round(pct, 2),
            details=f"Column '{column}': {orphans} values not found in {ref_table}.{ref_column}",
            severity=severity,
        )]

    def check_statistical_outlier(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Flag values beyond N standard deviations from the mean."""
        column = config["column"]
        n_stddev = config.get("n_stddev", 3)
        group_by = config.get("group_by")  # optional: detect outliers per group
        table = config.get("_table", "unknown")
        severity = config.get("severity", "warning")

        non_null_df = df.filter(col(column).isNotNull())
        total = non_null_df.count()

        if total == 0:
            return [CheckResult(
                table_name=table, check_name=f"outlier_{column}",
                check_type="statistical_outlier", status="PASS",
                records_checked=0, records_failed=0, failure_percentage=0.0,
                details=f"Column '{column}': no data to check", severity=severity,
            )]

        stats = non_null_df.select(
            avg(col(column)).alias("mean"),
            stddev(col(column)).alias("sd"),
        ).collect()[0]

        mean_val = float(stats["mean"]) if stats["mean"] is not None else 0.0
        sd_val = float(stats["sd"]) if stats["sd"] is not None else 0.0

        if sd_val == 0:
            return [CheckResult(
                table_name=table, check_name=f"outlier_{column}",
                check_type="statistical_outlier", status="PASS",
                records_checked=total, records_failed=0, failure_percentage=0.0,
                details=f"Column '{column}': stddev is 0, no outliers possible",
                severity=severity,
            )]

        lower = mean_val - (n_stddev * sd_val)
        upper = mean_val + (n_stddev * sd_val)

        outlier_count = non_null_df.filter(
            (col(column) < lower) | (col(column) > upper)
        ).count()
        pct = (outlier_count / total * 100) if total > 0 else 0

        return [CheckResult(
            table_name=table,
            check_name=f"outlier_{column}",
            check_type="statistical_outlier",
            status="WARN" if outlier_count > 0 else "PASS",
            records_checked=total,
            records_failed=outlier_count,
            failure_percentage=round(pct, 2),
            details=f"Column '{column}': {outlier_count} outliers outside [{lower:.2f}, {upper:.2f}] (mean={mean_val:.2f}, sd={sd_val:.2f})",
            severity=severity,
        )]

    def check_row_count(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Check that the table has at least min_rows records."""
        min_rows = config.get("min_rows", 1)
        table = config.get("_table", "unknown")
        severity = config.get("severity", "critical")

        total = df.count()
        passed = total >= min_rows

        return [CheckResult(
            table_name=table,
            check_name="row_count",
            check_type="row_count",
            status="PASS" if passed else "FAIL",
            records_checked=total,
            records_failed=0 if passed else total,
            failure_percentage=0.0 if passed else 100.0,
            details=f"Row count: {total} (minimum required: {min_rows})",
            severity=severity,
        )]

    def run_check(self, df: DataFrame, config: dict) -> list[CheckResult]:
        """Dispatch to the right check method based on config['check']."""
        check_type = config["check"]
        dispatch = {
            "not_null": self.check_not_null,
            "accepted_values": self.check_accepted_values,
            "uniqueness": self.check_uniqueness,
            "range_check": self.check_range,
            "freshness": self.check_freshness,
            "referential_integrity": self.check_referential_integrity,
            "statistical_outlier": self.check_statistical_outlier,
            "row_count": self.check_row_count,
        }

        handler = dispatch.get(check_type)
        if handler is None:
            logger.warning(f"Unknown check type: {check_type}")
            return []

        return handler(df, config)
