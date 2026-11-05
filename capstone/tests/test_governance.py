"""Unit tests for the privacy helpers (no warehouse needed): uv run --with pytest pytest capstone/tests -q"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import governance as G  # noqa: E402


def test_token_is_stable_and_hides_the_id():
    t = G.pseudonymize("1278681299")
    assert t == G.pseudonymize("1278681299")          # same person, same token: joins still work
    assert "1278681299" not in t and len(t) == 20


def test_token_depends_on_the_key():
    # a plain hash can be reversed by hashing a leaked list of IDs; without the key the tokens don't match
    assert G.pseudonymize("1278681299", key=b"another-key") != G.pseudonymize("1278681299")


def test_empty_values_stay_empty():
    assert G.pseudonymize(None) is None and G.pseudonymize("  ") is None


def test_mobile_formats_normalize_to_e164():
    for raw in ["0549730524", "966549730524", "+966 54 973 0524", "+966549730524"]:
        assert G.normalize_mobile(raw) == "+966549730524"
    assert G.normalize_mobile("12345") is None


def test_national_id_check_digit():
    assert G.is_saudi_national_id("1278681299")
    assert not G.is_saudi_national_id("1278681298")    # wrong check digit: e.g. a tracking number
    assert not G.is_saudi_national_id("3278681299")    # must start with 1 or 2


def test_every_identifier_is_classified():
    assert G.CLASSIFICATION["customers.national_id"][0] == "Secret"
    assert G.CLASSIFICATION["customers.iban"][0] == "Secret"
