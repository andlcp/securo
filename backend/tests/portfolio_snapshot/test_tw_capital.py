"""Pin _tw_capital — the Modified Dietz denominator behind the per-asset
rent %.

Why this exists: dividing P&L by the raw sum of buys treats a dollar
deployed for 2 days the same as one deployed for 15 months. The user's
NVDA position (two lots bought, one flipped near cost 2 days later, the
survivor doubled) showed +51% under profit/total-deployed while the
honest experience was ~+100%. Time-weighting the flows fixes it without
breaking loans or single-buy positions.
"""
from datetime import date, timedelta

import pytest

from app.services.asset_service import _tw_capital


D0 = date(2025, 4, 8)


def test_single_buy_full_weight():
    # One buy held the whole period: tw == invested, so Dietz degenerates
    # to plain gain/invested — single-buy positions are unchanged.
    tw = _tw_capital([(D0, "BUY", 1000.0)], D0 + timedelta(days=365))
    assert tw == pytest.approx(1000.0)


def test_nvda_two_day_flip_barely_dilutes():
    # The reference case: 2,078.90 in at day 0, 1,097.05 out at day 2,
    # measured at day 469. The flipped lot's weight is 467/469 ≈ 0.996,
    # leaving ~986 of time-weighted capital — so the surviving lot's
    # doubling shows as ~+108%, not +51%.
    as_of = D0 + timedelta(days=469)
    tw = _tw_capital(
        [(D0, "BUY", 2078.90), (D0 + timedelta(days=2), "SELL", 1097.05)],
        as_of,
    )
    expected = 2078.90 - 1097.05 * (467 / 469)
    assert tw == pytest.approx(expected, rel=1e-9)
    gain = 1071.85  # current 2,053.20 + returned 1,097.55 − invested 2,078.90
    assert gain / tw * 100 == pytest.approx(108.7, abs=0.5)


def test_loan_amortization_reduces_weighted_capital():
    # Principal 100k at day 0, 25k amortized at half-period: the second
    # half runs on 75k, so tw = 100k − 25k×0.5 = 87.5k.
    as_of = D0 + timedelta(days=400)
    tw = _tw_capital(
        [(D0, "BUY", 100_000.0),
         (D0 + timedelta(days=200), "WITHDRAWAL", 25_000.0)],
        as_of,
    )
    assert tw == pytest.approx(100_000.0 - 25_000.0 * (200 / 400))


def test_empty_flows_returns_none():
    assert _tw_capital([], date(2026, 1, 1)) is None


def test_only_outflows_returns_none():
    # No BUY/DEPOSIT ever recorded — no meaningful basis; caller falls
    # back to purchase_price × units.
    tw = _tw_capital([(D0, "SELL", 500.0)], D0 + timedelta(days=100))
    assert tw is None


def test_position_opened_today_gives_zero():
    # as_of == buy date → weight 0 → tw 0. The caller treats tw <= 0 as
    # "fall back to plain invested" so a brand-new position never
    # divides by zero.
    tw = _tw_capital([(D0, "BUY", 1000.0)], D0)
    assert tw == pytest.approx(0.0)


def test_deposit_counts_as_inflow():
    tw = _tw_capital([(D0, "DEPOSIT", 500.0)], D0 + timedelta(days=100))
    assert tw == pytest.approx(500.0)


def test_closed_position_capped_at_sell_date_gives_honest_return():
    """The bug the adversarial review caught: a position bought at 1,000
    and fully sold at 1,150 a year later must show +15% FOREVER. The
    caller caps as_of at sell_date; at period end the sell's weight is 0,
    so tw == invested and Dietz == money-on-money. Without the cap, tw
    kept shrinking every calendar day after the close (+203% four years
    later, drifting daily, then a discontinuous snap when tw crossed 0)."""
    buy = date(2021, 6, 1)
    sell = date(2022, 6, 1)
    flows = [(buy, "BUY", 1000.0), (sell, "SELL", 1150.0)]
    tw = _tw_capital(flows, sell)  # caller passes as_of=sell_date when closed
    assert tw == pytest.approx(1000.0)
    gain = 150.0
    assert gain / tw * 100 == pytest.approx(15.0)


def test_sell_predating_first_buy_is_clamped():
    """Ghost rows on reconciled/archived assets can carry a SELL dated
    before the first BUY. It must not define the window start nor get a
    weight above 1 (which would over-subtract from the basis)."""
    stray_sell = D0 - timedelta(days=300)
    flows = [(stray_sell, "SELL", 200.0), (D0, "BUY", 1000.0)]
    tw = _tw_capital(flows, D0 + timedelta(days=100))
    # Window starts at the BUY; the stray sell's weight clamps to 1.0.
    assert tw == pytest.approx(1000.0 * 1.0 - 200.0 * 1.0)
