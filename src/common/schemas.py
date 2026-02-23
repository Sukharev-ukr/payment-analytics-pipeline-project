"""Shared schema definitions used by the generator, streaming consumer, and batch jobs."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


REQUIRED_FIELDS = frozenset({
    "event_id",
    "psp_reference",
    "merchant_id",
    "event_type",
    "event_timestamp",
    "amount",
    "currency",
})

EVENT_TYPES = frozenset({
    "AUTHORIZATION",
    "CAPTURE",
    "SETTLEMENT",
    "REFUND",
    "CHARGEBACK",
    "CANCEL",
})

# approximate EUR exchange rates for the simulation
CURRENCY_FX_RATES = {
    "EUR": 1.0,
    "USD": 0.92,
    "GBP": 1.17,
    "SEK": 0.088,
    "DKK": 0.134,
    "NOK": 0.089,
    "CHF": 1.05,
    "PLN": 0.23,
}

SUPPORTED_CURRENCIES = frozenset(CURRENCY_FX_RATES.keys())

PAYMENT_METHODS = {
    "visa":         {"variants": ["visa_credit", "visa_debit"], "weight": 0.30},
    "mastercard":   {"variants": ["mc_credit", "mc_debit"], "weight": 0.25},
    "amex":         {"variants": ["amex_credit"], "weight": 0.08},
    "ideal":        {"variants": ["ideal"], "weight": 0.15},
    "klarna":       {"variants": ["klarna_paynow", "klarna_paylater"], "weight": 0.08},
    "bancontact":   {"variants": ["bancontact"], "weight": 0.07},
    "sofort":       {"variants": ["sofort"], "weight": 0.07},
}

SHOPPER_INTERACTIONS = ["ecommerce", "pos", "moto", "recurring"]

# acquirer response codes: "00" = approved, rest are declines
ACQUIRER_RESPONSE_CODES = {
    "00": 0.85,   # Approved
    "05": 0.06,   # Do not honor
    "51": 0.04,   # Insufficient funds
    "14": 0.02,   # Invalid card number
    "54": 0.015,  # Expired card
    "61": 0.01,   # Exceeds withdrawal limit
    "65": 0.005,  # Activity count limit exceeded
}


@dataclass
class PaymentEvent:
    """Single payment event matching the Kafka message schema."""

    event_id: str
    psp_reference: str
    merchant_id: str
    merchant_name: str
    merchant_country: str
    merchant_category_code: str
    event_type: str
    event_timestamp: str
    amount: float
    currency: str
    original_amount: Optional[float] = None
    original_currency: Optional[str] = None
    payment_method: Optional[str] = None
    payment_method_variant: Optional[str] = None
    card_bin: Optional[str] = None
    card_last_four: Optional[str] = None
    issuer_country: Optional[str] = None
    shopper_interaction: Optional[str] = None
    shopper_reference: Optional[str] = None
    acquirer_response_code: Optional[str] = None
    is_3ds_authenticated: Optional[bool] = None
    risk_score: Optional[int] = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}
