"""Pruebas de la limpieza de silver.

La idea central: generamos datos limpios (la verdad), los corrompemos, los limpiamos,
y comprobamos que el resultado coincide con la verdad. Así la calidad se MIDE.
"""
import copy
from pathlib import Path

import pytest

from clean import clean_record, clean_records, clean_text, load_rules, parse_date, recover_product, to_int
from corrupt import corrupt
from generate import generate_sales

RULES = load_rules(Path(__file__).resolve().parent.parent / "schemas" / "ventas.yaml")
FORMATS = RULES["fields"]["order_date"]["formats"]
NOISE = RULES["noise_patterns"]


# ---------- piezas pequeñas ----------

@pytest.mark.parametrize(
    "text, expected",
    [
        ("2026-10-09 14:30:00", "2026-10-09 14:30:00"),
        ("2026/09/10", "2026-10-09 00:00:00"),   # AAAA/DD/MM
        ("09-10-2026", "2026-10-09 00:00:00"),   # DD-MM-AAAA
        ("09/10/26", "2026-10-09 00:00:00"),     # DD/MM/AA
        ("2026.10.09", "2026-10-09 00:00:00"),   # AAAA.MM.DD
    ],
)
def test_parse_date_known_formats(text, expected):
    dt, fmt = parse_date(text, FORMATS)
    assert dt.strftime("%Y-%m-%d %H:%M:%S") == expected
    assert fmt is not None


@pytest.mark.parametrize("text", ["ayer", "", None, "31/31/2026"])
def test_parse_date_rejects_garbage(text):
    assert parse_date(text, FORMATS) == (None, None)


def test_clean_text_removes_noise_and_keeps_real_accents():
    assert clean_text("Felipe Suárez???", NOISE, noise=True) == ("Felipe Suárez", True)
    assert clean_text("Medellín�", NOISE, noise=True) == ("Medellín", True)
    assert clean_text("MaríaÃ±", NOISE, noise=True) == ("María", True)
    assert clean_text("Bogotá", NOISE, noise=True) == ("Bogotá", False)


def test_clean_text_lowercases_and_turns_empty_into_none():
    assert clean_text("  ANA@Example.ORG ", NOISE, lowercase=True) == ("ana@example.org", False)
    assert clean_text("   ", NOISE) == (None, False)


@pytest.mark.parametrize("value, expected", [(5, 5), ("7", 7), (3.0, 3), (3.5, None), ("abc", None), (None, None), (True, None)])
def test_to_int(value, expected):
    assert to_int(value) == expected


def test_recover_product_fills_exactly_one_missing_value():
    rec = {"total": 251700, "quantity": 3, "unit_price": None}
    assert recover_product(rec, "total", "quantity", "unit_price") == "unit_price"
    assert rec["unit_price"] == 83900

    rec = {"total": 251700, "quantity": None, "unit_price": 83900}
    assert recover_product(rec, "total", "quantity", "unit_price") == "quantity"
    assert rec["quantity"] == 3

    rec = {"total": None, "quantity": 2, "unit_price": 1000}
    assert recover_product(rec, "total", "quantity", "unit_price") == "total"
    assert rec["total"] == 2000


def test_recover_product_refuses_to_guess():
    two_missing = {"total": 100, "quantity": None, "unit_price": None}
    assert recover_product(two_missing, "total", "quantity", "unit_price") is None
    not_divisible = {"total": 100, "quantity": None, "unit_price": 30}
    assert recover_product(not_divisible, "total", "quantity", "unit_price") is None
    assert not_divisible["quantity"] is None


def test_record_with_unreadable_date_goes_to_quarantine():
    raw = generate_sales(1, seed=1)[0]
    raw["order_date"] = "ayer"
    rec, reason = clean_record(raw, RULES)
    assert rec is None and reason.startswith("invalid_date")


def test_record_without_key_goes_to_quarantine():
    clean, quarantine, report = clean_records([{"city": "Cali"}, "no soy un dict"], RULES)
    assert clean == [] and len(quarantine) == 2
    assert report["quarantine_reasons"] == {"missing_key": 2}


# ---------- el escenario completo contra la verdad ----------

@pytest.fixture(scope="module")
def scenario():
    truth = generate_sales(1000, seed=123)
    dirty, corrupt_report = corrupt(truth, seed=123)
    dirty_before = copy.deepcopy(dirty)
    clean, quarantine, report = clean_records(dirty, RULES)
    return {
        "truth": {r["event_id"]: r for r in truth},
        "dirty": dirty,
        "dirty_before": dirty_before,
        "clean": clean,
        "quarantine": quarantine,
        "report": report,
        "corrupt_report": corrupt_report,
    }


def test_input_is_not_modified(scenario):
    assert scenario["dirty"] == scenario["dirty_before"]


def test_duplicates_are_removed(scenario):
    ids = [r["event_id"] for r in scenario["clean"]]
    assert len(ids) == len(set(ids))
    assert scenario["report"]["duplicates_removed"] == scenario["corrupt_report"]["duplicates"]


def test_every_event_is_accounted_for(scenario):
    assert len(scenario["clean"]) + len(scenario["quarantine"]) == len(scenario["truth"])


def test_only_unreadable_dates_are_quarantined(scenario):
    reasons = {q["reason"].split(":")[0] for q in scenario["quarantine"]}
    assert reasons <= {"invalid_date"}
    assert len(scenario["quarantine"]) <= scenario["corrupt_report"]["bad_dates"]


def test_dates_match_the_truth(scenario):
    for rec in scenario["clean"]:
        truth = scenario["truth"][rec["event_id"]]
        assert rec["order_date"][:10] == truth["order_date"][:10]
        if "order_date_time_unknown" not in rec["dq_flags"]:
            assert rec["order_date"] == truth["order_date"]


def test_text_noise_is_gone_and_matches_truth(scenario):
    for rec in scenario["clean"]:
        truth = scenario["truth"][rec["event_id"]]
        for field in ("customer_name", "city"):
            if rec[field] is not None:
                assert rec[field] == truth[field]


def test_missing_numbers_are_recovered(scenario):
    for rec in scenario["clean"]:
        truth = scenario["truth"][rec["event_id"]]
        for field in ("quantity", "unit_price", "total"):
            assert rec[field] == truth[field]


def test_never_invents_data(scenario):
    """Todo valor presente en silver debe ser el valor verdadero (los nulos irrecuperables siguen nulos)."""
    for rec in scenario["clean"]:
        truth = scenario["truth"][rec["event_id"]]
        for field, value in rec.items():
            if field in ("dq_flags", "order_date"):
                continue
            assert value is None or value == truth[field], f"{field} no coincide con la verdad"


def test_quality_report_is_consistent(scenario):
    report = scenario["report"]
    assert report["rows_out"] == len(scenario["clean"])
    assert report["rows_in"] == report["rows_out"] + report["quarantined"] + report["duplicates_removed"]
