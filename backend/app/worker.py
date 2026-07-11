from celery import Celery
from celery.schedules import crontab

from app.core.config import get_settings

settings = get_settings()

celery_app = Celery(
    "securo",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

celery_app.conf.beat_schedule = {
    "sync-all-connections-hourly": {
        "task": "app.tasks.sync_tasks.sync_all_connections",
        "schedule": 60 * 60,  # every hour; task itself skips connections synced < 4h ago
    },
    "generate-recurring-daily": {
        "task": "app.tasks.recurring_tasks.generate_all_recurring",
        "schedule": 60 * 60,  # every hour; generate_pending is idempotent (advances next_occurrence)
    },
    "apply-asset-growth-daily": {
        "task": "app.tasks.asset_tasks.apply_asset_growth_rules",
        "schedule": 60 * 60,  # every hour; idempotent (checks last value date)
    },
    # Market prices (stocks / ETFs / FIIs / crypto via yfinance). Three
    # complementary schedules — all fixed-hour crontabs, never intervals:
    # interval schedules fire relative to celery-beat's boot, so a
    # mid-morning deploy once made the refresh land at 09:58 BRT
    # (mid-session) and the cached last_price missed the rest of the
    # day's +3% IBOV rally, painting a phantom weekend drop.
    #
    # Load: ~80 tickers/run. Intraday = 16 runs on weekdays + closes +
    # weekend crypto ≈ 1.5 k quote calls/day, spread out — well under
    # Yahoo's unofficial caps, and refresh_all_market_prices halts itself
    # on MarketPriceRateLimitedError. Each run drops today's snapshot
    # rows, so the charts re-materialize on the next read.
    "refresh-market-prices-intraday": {
        "task": "app.tasks.asset_tasks.refresh_market_prices",
        # Every 30 min through the B3 + NYSE sessions (13:00-20:30 UTC =
        # 10:00-17:30 BRT), weekdays only.
        "schedule": crontab(minute="0,30", hour="13-20", day_of_week="mon-fri"),
    },
    "refresh-market-prices-close": {
        "task": "app.tasks.asset_tasks.refresh_market_prices",
        # End-of-day capture at 21:30 UTC = 18:30 BRT, after B3 (~17:55
        # BRT) and NYSE (20:00/21:00 UTC depending on DST) have closed —
        # guarantees the cached quote is the official close overnight.
        "schedule": crontab(hour=21, minute=30),
    },
    "refresh-market-prices-weekend": {
        "task": "app.tasks.asset_tasks.refresh_market_prices",
        # Crypto trades 24/7 — refresh every 3 h on weekends so Bitcoin
        # doesn't freeze at Friday's quote. Stocks return their Friday
        # close from Yahoo, which is a harmless no-op for them.
        "schedule": crontab(minute=0, hour="*/3", day_of_week="sat,sun"),
    },
    "sync-dividends-daily": {
        "task": "app.tasks.asset_tasks.sync_dividends",
        # Yahoo dividend events. Daily is enough — ex-dates only change
        # once per asset per quarter or so, and the task is idempotent
        # (dedupes by external_id and same-date guard).
        "schedule": 60 * 60 * 24,
    },
    "refresh-tesouro-daily": {
        "task": "app.tasks.rf_tasks.refresh_tesouro_assets",
        # Tesouro Transparente publishes the daily PU. Once a day is fine —
        # the task downloads the full historical CSV (~13 MB) on each run
        # but only keeps the latest row per (titulo, vencimento).
        # Fixed evening hour for the same reason as refresh-market-prices.
        "schedule": crontab(hour=22, minute=0),
    },
    "refresh-cdb-daily": {
        "task": "app.tasks.rf_tasks.refresh_cdb_assets",
        # CDB MTM via CDI compound at the contracted rate. Daily, right
        # after the Tesouro pull (fixed hour, same rationale).
        "schedule": crontab(hour=22, minute=15),
    },
    "sync-fx-rates-daily": {
        "task": "app.tasks.fx_rate_tasks.sync_fx_rates",
        "schedule": 60 * 60 * 12,  # twice daily (~60 API calls/month)
    },
    "restamp-recurring-fx-daily": {
        "task": "app.tasks.fx_rate_tasks.restamp_recurring_fx",
        "schedule": 60 * 60 * 12,  # twice daily, after FX rate sync
    },
}

celery_app.conf.include = [
    "app.tasks.sync_tasks",
    "app.tasks.recurring_tasks",
    "app.tasks.asset_tasks",
    "app.tasks.fx_rate_tasks",
    "app.tasks.rf_tasks",
    # Optional agents module — registering the import is harmless when
    # AGENTS_ENABLED=false (the task just won't be dispatched).
    "app.agents.tasks.ingest",
]
