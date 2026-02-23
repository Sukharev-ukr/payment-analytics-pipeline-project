"""Configuration for the payment event generator.
All tunable parameters live here; override via environment variables."""

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "payment.events.raw")

EVENTS_PER_SECOND = int(os.getenv("EVENTS_PER_SECOND", "10"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# merchant pool: ~20 merchants across NL, DE, FR, UK, US
MERCHANTS = [
    {"id": "merchant_001", "name": "TechStore Amsterdam",       "country": "NL", "mcc": "5732", "avg_amount": 149.99},
    {"id": "merchant_002", "name": "Fashion Hub Rotterdam",     "country": "NL", "mcc": "5651", "avg_amount": 79.50},
    {"id": "merchant_003", "name": "Albert Heijn Online",       "country": "NL", "mcc": "5411", "avg_amount": 45.00},
    {"id": "merchant_004", "name": "Bol.com Marketplace",       "country": "NL", "mcc": "5999", "avg_amount": 62.00},
    {"id": "merchant_005", "name": "Berlin Electronics GmbH",   "country": "DE", "mcc": "5732", "avg_amount": 199.00},
    {"id": "merchant_006", "name": "Munich Auto Parts",         "country": "DE", "mcc": "5533", "avg_amount": 85.00},
    {"id": "merchant_007", "name": "Zalando DE",                "country": "DE", "mcc": "5651", "avg_amount": 95.00},
    {"id": "merchant_008", "name": "MediaMarkt Online",         "country": "DE", "mcc": "5732", "avg_amount": 250.00},
    {"id": "merchant_009", "name": "Paris Luxe Boutique",       "country": "FR", "mcc": "5944", "avg_amount": 320.00},
    {"id": "merchant_010", "name": "Lyon Gourmet Market",       "country": "FR", "mcc": "5411", "avg_amount": 55.00},
    {"id": "merchant_011", "name": "Carrefour Online",          "country": "FR", "mcc": "5411", "avg_amount": 72.00},
    {"id": "merchant_012", "name": "London Digital Services",   "country": "GB", "mcc": "7372", "avg_amount": 29.99},
    {"id": "merchant_013", "name": "UK Travel Agency",          "country": "GB", "mcc": "4722", "avg_amount": 450.00},
    {"id": "merchant_014", "name": "Manchester Sports Direct",  "country": "GB", "mcc": "5941", "avg_amount": 65.00},
    {"id": "merchant_015", "name": "Edinburgh Subscriptions",   "country": "GB", "mcc": "5968", "avg_amount": 14.99},
    {"id": "merchant_016", "name": "NYC Coffee Roasters",       "country": "US", "mcc": "5812", "avg_amount": 35.00},
    {"id": "merchant_017", "name": "SF SaaS Platform",          "country": "US", "mcc": "7372", "avg_amount": 99.00},
    {"id": "merchant_018", "name": "Chicago Marketplace",       "country": "US", "mcc": "5999", "avg_amount": 110.00},
    {"id": "merchant_019", "name": "Austin Gaming Store",       "country": "US", "mcc": "5945", "avg_amount": 59.99},
    {"id": "merchant_020", "name": "Seattle Health Supplements", "country": "US", "mcc": "5912", "avg_amount": 42.00},
]

# country -> primary currency
COUNTRY_CURRENCY = {
    "NL": "EUR",
    "DE": "EUR",
    "FR": "EUR",
    "GB": "GBP",
    "US": "USD",
}

ISSUER_COUNTRIES = ["NL", "DE", "FR", "GB", "US", "BE", "SE", "NO", "DK", "PL", "CH"]

# transaction lifecycle probabilities
AUTH_SUCCESS_RATE = 0.85
CAPTURE_RATE = 0.92
SETTLEMENT_RATE = 0.98
REFUND_RATE = 0.10
CHARGEBACK_RATE = 0.005

# delays between lifecycle stages (seconds, for simulation speed)
AUTH_TO_CAPTURE_DELAY = (60, 7200)
CAPTURE_TO_SETTLEMENT_DELAY = (3600, 86400)
CAPTURE_TO_REFUND_DELAY = (3600, 604800)
CAPTURE_TO_CHARGEBACK_DELAY = (86400, 2592000)

# amount distribution: normal around each merchant's avg_amount
AMOUNT_STD_RATIO = 0.4
AMOUNT_MIN = 0.50
AMOUNT_MAX = 9999.99
