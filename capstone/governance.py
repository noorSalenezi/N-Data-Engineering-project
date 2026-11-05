"""Personal data handling for the Qafila Lakehouse (PDPL, NDMO classification levels).

One place that says how each personal-data column is classified and protected. Ingestion uses
`pseudonymize` and `normalize_mobile`; the README's governance note is generated from CLASSIFICATION.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re

# Dev default so the demo runs out of the box. In production the key lives in a secret manager and is
# passed in QAFILA_PSEUDO_KEY; rotating it re-keys every token (plan a re-tokenization run).
_DEV_KEY = "capstone-dev-key-replace-me"
PSEUDO_KEY = os.environ.get("QAFILA_PSEUDO_KEY", _DEV_KEY).encode()
USING_DEV_KEY = PSEUDO_KEY == _DEV_KEY.encode()

# column -> (NDMO level, protection, who sees the raw value)
CLASSIFICATION: dict[str, tuple[str, str, str]] = {
    "customers.national_id": ("Secret", "Pseudonymized at ingestion (HMAC-SHA256 with a secret key) -> person_token. "
                              "Raw value only in the PII vault (separate database file).", "Nobody in analytics; vault owner only"),
    "customers.iban": ("Secret", "Dropped at ingestion; kept only in the PII vault.", "Vault owner only (finance/payments domain)"),
    "customers.full_name": ("Restricted", "Kept in raw/staging; removed from marts; visible only in the support view.", "Support"),
    "customers.email": ("Restricted", "Kept in raw/staging; masked in the support view (a***@domain); analysts see the domain only.", "Support (masked)"),
    "customers.mobile": ("Restricted", "Normalized to E.164 in staging; masked in the support view (+9665•••••12).", "Support (masked)"),
    "customers.district": ("Restricted", "Kept: coarse location, needed for delivery analytics.", "Analysts"),
    "payments.card_last4": ("Restricted", "Provider already truncates to 4 digits; never shown in marts.", "Payments team"),
    "delivery_events.lat/lon": ("Restricted", "Courier and customer location. Stays in staging; marts aggregate per store/day.", "Ops engineers"),
    "delivery_events.courier_id": ("Restricted", "Employee identifier; ops mart aggregates per store, not per courier.", "Ops managers"),
}


def pseudonymize(value: str | None, key: bytes = PSEUDO_KEY) -> str | None:
    """Keyed, deterministic token: the same person gets the same token, and without the key a leaked
    list of IDs can't be matched (a plain hash can, by hashing the list)."""
    if value is None or str(value).strip() == "":
        return None
    return hmac.new(key, str(value).strip().encode(), hashlib.sha256).hexdigest()[:20]


def normalize_mobile(value: str | None) -> str | None:
    """Any of the app's formats (05..., 9665..., +966 5...) -> +9665XXXXXXXX; None if unrecognisable."""
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if re.fullmatch(r"(966|0)?5\d{8}", digits):
        return "+966" + digits[-9:]
    return None


def is_saudi_national_id(s: str) -> bool:
    """10 digits starting with 1 (citizen) or 2 (resident), with a valid Luhn check digit."""
    if not re.fullmatch(r"[12]\d{9}", s or ""):
        return False
    total = 0
    for i, ch in enumerate(s):
        d = int(ch)
        if i % 2 == 0:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0
