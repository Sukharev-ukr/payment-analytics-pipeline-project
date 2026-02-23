"""Generates realistic payment lifecycle events for the simulator."""

import random
import string
import uuid
from collections import deque
from datetime import datetime, timezone, timedelta
from typing import Optional

from src.common.schemas import (
    ACQUIRER_RESPONSE_CODES,
    CURRENCY_FX_RATES,
    EVENT_TYPES,
    PAYMENT_METHODS,
    SHOPPER_INTERACTIONS,
    PaymentEvent,
)
from src.generator.config import (
    AMOUNT_MAX,
    AMOUNT_MIN,
    AMOUNT_STD_RATIO,
    AUTH_SUCCESS_RATE,
    AUTH_TO_CAPTURE_DELAY,
    CAPTURE_RATE,
    CAPTURE_TO_CHARGEBACK_DELAY,
    CAPTURE_TO_REFUND_DELAY,
    CAPTURE_TO_SETTLEMENT_DELAY,
    CHARGEBACK_RATE,
    COUNTRY_CURRENCY,
    ISSUER_COUNTRIES,
    MERCHANTS,
    REFUND_RATE,
    SETTLEMENT_RATE,
)


class PaymentGenerator:
    """Generates payment events following AUTH -> CAPTURE -> SETTLEMENT lifecycle.
    Keeps state per psp_reference so follow-ups reference the right authorization."""

    def __init__(self) -> None:
        self._merchants = MERCHANTS
        self._pending_events: deque = deque()  # (scheduled_time, event_dict)
        self._active_transactions: dict[str, dict] = {}

    def generate_event(self) -> PaymentEvent:
        """Return a follow-up if one is due, otherwise create a new authorization."""
        now = datetime.now(timezone.utc)

        if self._pending_events and self._pending_events[0][0] <= now:
            _, event_data = self._pending_events.popleft()
            return self._build_event(event_data)

        return self._generate_authorization(now)

    def _generate_authorization(self, now: datetime) -> PaymentEvent:
        merchant = random.choice(self._merchants)
        psp_reference = self._generate_psp_reference()
        payment_method, variant = self._pick_payment_method()
        shopper_ref = f"shopper_{random.randint(1000, 99999)}"
        currency = COUNTRY_CURRENCY[merchant["country"]]
        amount = self._generate_amount(merchant["avg_amount"])

        # cross-border: 25% chance the shopper pays in a different currency
        original_currency: Optional[str] = None
        original_amount: Optional[float] = None
        if random.random() < 0.25:
            original_currency = random.choice(
                [c for c in CURRENCY_FX_RATES if c != currency]
            )
            original_amount = round(
                amount / CURRENCY_FX_RATES[original_currency] * CURRENCY_FX_RATES[currency],
                2,
            )

        card_bin: Optional[str] = None
        card_last_four: Optional[str] = None
        is_card = payment_method in ("visa", "mastercard", "amex")
        if is_card:
            card_bin = self._generate_card_bin(payment_method)
            card_last_four = f"{random.randint(0, 9999):04d}"

        issuer_country = random.choice(ISSUER_COUNTRIES)
        shopper_interaction = random.choice(SHOPPER_INTERACTIONS)
        is_3ds = random.random() < 0.70 if is_card else None
        risk_score = random.randint(0, 100)

        response_code = self._pick_response_code()
        is_approved = response_code == "00"

        event_data = {
            "psp_reference": psp_reference,
            "merchant_id": merchant["id"],
            "merchant_name": merchant["name"],
            "merchant_country": merchant["country"],
            "merchant_category_code": merchant["mcc"],
            "event_type": "AUTHORIZATION",
            "event_timestamp": now.replace(tzinfo=None).isoformat(timespec="milliseconds") + "Z",
            "amount": amount,
            "currency": currency,
            "original_amount": original_amount,
            "original_currency": original_currency,
            "payment_method": payment_method,
            "payment_method_variant": variant,
            "card_bin": card_bin,
            "card_last_four": card_last_four,
            "issuer_country": issuer_country,
            "shopper_interaction": shopper_interaction,
            "shopper_reference": shopper_ref,
            "acquirer_response_code": response_code,
            "is_3ds_authenticated": is_3ds,
            "risk_score": risk_score,
        }

        if is_approved:
            self._active_transactions[psp_reference] = event_data.copy()
            self._schedule_follow_ups(psp_reference, now, event_data)

        return self._build_event(event_data)

    def _schedule_follow_ups(
        self, psp_reference: str, auth_time: datetime, auth_data: dict
    ) -> None:
        """Schedule follow-up events (capture, settlement, refund, chargeback) probabilistically."""
        if random.random() > CAPTURE_RATE:
            return

        capture_delay = random.randint(*AUTH_TO_CAPTURE_DELAY)
        capture_time = auth_time + timedelta(seconds=capture_delay)

        capture_data = self._make_follow_up(auth_data, "CAPTURE", capture_time)
        self._pending_events.append((capture_time, capture_data))

        if random.random() < SETTLEMENT_RATE:
            settle_delay = random.randint(*CAPTURE_TO_SETTLEMENT_DELAY)
            settle_time = capture_time + timedelta(seconds=settle_delay)
            settle_data = self._make_follow_up(auth_data, "SETTLEMENT", settle_time)
            self._pending_events.append((settle_time, settle_data))

        if random.random() < REFUND_RATE:
            refund_delay = random.randint(*CAPTURE_TO_REFUND_DELAY)
            refund_time = capture_time + timedelta(seconds=refund_delay)
            refund_data = self._make_follow_up(auth_data, "REFUND", refund_time)
            self._pending_events.append((refund_time, refund_data))

        if random.random() < CHARGEBACK_RATE:
            cb_delay = random.randint(*CAPTURE_TO_CHARGEBACK_DELAY)
            cb_time = capture_time + timedelta(seconds=cb_delay)
            cb_data = self._make_follow_up(auth_data, "CHARGEBACK", cb_time)
            self._pending_events.append((cb_time, cb_data))

        sorted_events = sorted(self._pending_events, key=lambda x: x[0])
        self._pending_events = deque(sorted_events)

    def _make_follow_up(
        self, auth_data: dict, event_type: str, event_time: datetime
    ) -> dict:
        """Build a follow-up event reusing the same psp_reference, merchant, amount, etc."""
        follow_up = auth_data.copy()
        follow_up["event_type"] = event_type
        follow_up["event_timestamp"] = event_time.replace(tzinfo=None).isoformat(timespec="milliseconds") + "Z"
        # Follow-up events don't have their own acquirer response
        follow_up["acquirer_response_code"] = "00"
        # partial refund ~30% of the time
        if event_type == "REFUND" and random.random() < 0.30:
            follow_up["amount"] = round(auth_data["amount"] * random.uniform(0.1, 0.9), 2)
        return follow_up

    def _build_event(self, data: dict) -> PaymentEvent:
        event_id = f"evt_{uuid.uuid4().hex[:12]}"
        return PaymentEvent(event_id=event_id, **data)

    def _pick_payment_method(self) -> tuple[str, str]:
        methods = list(PAYMENT_METHODS.keys())
        weights = [PAYMENT_METHODS[m]["weight"] for m in methods]
        method = random.choices(methods, weights=weights, k=1)[0]
        variant = random.choice(PAYMENT_METHODS[method]["variants"])
        return method, variant

    def _pick_response_code(self) -> str:
        codes = list(ACQUIRER_RESPONSE_CODES.keys())
        weights = list(ACQUIRER_RESPONSE_CODES.values())
        return random.choices(codes, weights=weights, k=1)[0]

    def _generate_amount(self, avg: float) -> float:
        """Normal distribution around the merchant's average amount."""
        std = avg * AMOUNT_STD_RATIO
        amount = random.gauss(avg, std)
        return round(max(AMOUNT_MIN, min(AMOUNT_MAX, amount)), 2)

    @staticmethod
    def _generate_psp_reference() -> str:
        """Adyen-style PSP reference: PSP + 13 digits."""
        digits = "".join(random.choices(string.digits, k=13))
        return f"PSP{digits}"

    @staticmethod
    def _generate_card_bin(payment_method: str) -> str:
        """Pick a realistic BIN prefix for the given card network."""
        bin_prefixes = {
            "visa": ["411111", "431940", "400000", "422222"],
            "mastercard": ["520000", "545454", "510000", "540000"],
            "amex": ["371449", "340000", "378282", "374245"],
        }
        return random.choice(bin_prefixes.get(payment_method, ["000000"]))
