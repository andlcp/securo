"""Pin _market_native_value: today's daily-chart point uses the live
cached quote (last_price) so the Investimentos V_END matches the
dashboard Patrimônio; past days use the yfinance close.

This closes the R$ 721 gap the user spotted between the two pages — the
difference was entirely USD assets (Bitcoin especially, 24/7) valued at
last_price on the dashboard vs yfinance close in the snapshot.
"""
from datetime import date

from app.services.portfolio_timeseries_service import _market_native_value

TODAY = date(2026, 6, 27)


def test_today_uses_live_last_price():
    # last_price 12.10, close 12.00 — today must use the live quote.
    v = _market_native_value(
        on=TODAY, today=TODAY, units=100.0,
        last_price=12.10, close_price=12.00, units_at_on=100.0,
    )
    assert v == 100.0 * 12.10


def test_future_date_also_uses_last_price():
    # on > today (shouldn't normally happen, but the >= guard must hold).
    v = _market_native_value(
        on=date(2026, 6, 28), today=TODAY, units=10.0,
        last_price=5.0, close_price=4.0, units_at_on=10.0,
    )
    assert v == 50.0


def test_past_day_uses_close():
    v = _market_native_value(
        on=date(2026, 6, 20), today=TODAY, units=100.0,
        last_price=12.10, close_price=12.00, units_at_on=80.0,
    )
    # Past day: yfinance close × units held that day (units_at_on, not units).
    assert v == 80.0 * 12.00


def test_today_without_last_price_falls_to_close():
    v = _market_native_value(
        on=TODAY, today=TODAY, units=100.0,
        last_price=None, close_price=12.00, units_at_on=100.0,
    )
    assert v == 100.0 * 12.00


def test_no_prices_returns_none():
    v = _market_native_value(
        on=TODAY, today=TODAY, units=100.0,
        last_price=None, close_price=None, units_at_on=100.0,
    )
    assert v is None
