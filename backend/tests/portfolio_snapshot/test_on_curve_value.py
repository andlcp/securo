"""Pin the coupon-aware na-curva valuation in rf_tasks._on_curve_value.

The carrego of a Juros-Semestrais title must subtract coupons already
paid (they leave the title as cash), accreted forward from their payment
date. Without the subtraction a cupom bond is overstated by ~30% (JS
2030: R$ 60 k full-accrual vs R$ 46 k coupon-aware). Uses the PRE indexer
so the accrual is deterministic (business-day compounding, no BCB series).
"""
from datetime import date
from types import SimpleNamespace

from app.tasks.rf_tasks import _on_curve_value, _compound_pre


def _asset(rate=10.0):
    return SimpleNamespace(
        rf_indexer="PRE",
        rf_rate_pct=rate,
        purchase_date=date(2024, 1, 2),
        purchase_price=100.0,
        units=10.0,  # invested = 1000
    )


def test_principal_only_is_plain_accrual():
    a = _asset()
    on = date(2026, 1, 2)
    got = _on_curve_value(a, on, {}, {}, coupons=None)
    expected = 1000.0 * _compound_pre(a.purchase_date, on, a.rf_rate_pct)
    assert got == expected


def test_coupons_are_subtracted_accreted_forward():
    a = _asset()
    on = date(2026, 1, 2)
    coupons = [(date(2024, 7, 1), 30.0), (date(2025, 1, 1), 30.0)]
    got = _on_curve_value(a, on, {}, {}, coupons=coupons)

    base = 1000.0 * _compound_pre(a.purchase_date, on, a.rf_rate_pct)
    sub = sum(c * _compound_pre(d, on, a.rf_rate_pct) for d, c in coupons)
    assert got == base - sub
    # The coupon-aware value must be strictly below the full accrual.
    assert got < base


def test_coupons_outside_window_ignored():
    a = _asset()
    on = date(2026, 1, 2)
    # One coupon before purchase, one after on_date — both must be skipped.
    coupons = [(date(2023, 1, 1), 30.0), (date(2027, 1, 1), 30.0)]
    got = _on_curve_value(a, on, {}, {}, coupons=coupons)
    expected = 1000.0 * _compound_pre(a.purchase_date, on, a.rf_rate_pct)
    assert got == expected


def test_missing_metadata_returns_none():
    a = _asset()
    a.rf_rate_pct = None
    assert _on_curve_value(a, date(2026, 1, 2), {}, {}, coupons=None) is None
