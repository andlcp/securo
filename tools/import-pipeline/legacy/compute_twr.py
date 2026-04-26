#!/usr/bin/env python3
"""
compute_twr.py

Build a continuous month-end portfolio value series and compute
Time-Weighted Return (Modified Dietz) from the user's complete history.

Inputs (already produced upstream):
    xp_snapshots.csv  — monthly patrimônio 2017-12 .. 2019-10 (22 rows)
    ledger_b3.csv     — transactions 2019-12 .. today (categorized)
    positions.csv     — for ticker → valuation_method mapping (optional)

Outputs:
    prices_cache.csv     — Yahoo monthly close cache (so re-runs are offline)
    portfolio_history.csv — month-end series:
        date, portfolio_value_brl, cost_basis_brl,
        net_cashflow_month_brl, monthly_return_pct, twr_cumulative_pct,
        source  (XP_SNAPSHOT | B3_REPLAY)

Methodology
-----------
1. XP period (2017-12 .. 2019-10): use snapshot patrimônio verbatim.
2. Gap month 2019-11: linearly interpolated from Oct/19 to Dec/19.
3. B3 period (2019-12 .. last full month): replay ledger up to each
   month-end, value market_price assets at Yahoo monthly close, value
   manual (CDB/Tesouro/etc) at cumulative net cost basis.
4. Cash-flow per month = sum(BUY value) - sum(SELL value) - sum(INCOME).
   INCOME is treated as a withdrawal (cash returned to investor).
5. TWR (Modified Dietz, mid-month assumption):
       r_i = (V_end - V_start - CF_i) / (V_start + 0.5 * CF_i)
       TWR = Π(1 + r_i) - 1

Caveats — be honest about what this does not model:
- Manual assets (CDB/LCI/Tesouro) are valued at cost basis, so they
  contribute zero return between buy and sell. Refining this needs CDI
  accrual or face-value curves; out of scope for v1.
- BUYs are treated as external deposits (assumes user funded the broker
  to do each buy) and SELLs/INCOME as withdrawals. If the user is
  rotating cash inside the broker without depositing/withdrawing, this
  overstates cash flows and dampens TWR. Acceptable approximation here.
- Bonus/split corporate actions adjust qty but have valor=0; they don't
  affect cash flow, only valuation through new qty * price.
"""
from __future__ import annotations

import csv
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    print("Missing 'requests'. Install with: pip install requests")
    sys.exit(1)


ROOT = Path("E:/Desenvolvimento/securo")
XP_SNAP = ROOT / "xp_snapshots.csv"
LEDGER = ROOT / "ledger_b3.csv"
POSITIONS = ROOT / "positions.csv"
PRICES_CACHE = ROOT / "prices_cache.csv"
OUTPUT = ROOT / "portfolio_history.csv"

YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120 Safari/537.36"
}


# ─── Date helpers ──────────────────────────────────────────────────────────────

def month_end(d: date) -> date:
    """Return last day of d's month."""
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month + 1, 1) - timedelta(days=1)


def month_iter(start: date, end: date):
    """Yield month-end dates from start's month-end through end's month-end."""
    cur = month_end(start)
    last = month_end(end)
    while cur <= last:
        yield cur
        # next month-end
        nxt = cur + timedelta(days=1)
        cur = month_end(nxt)


def parse_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


# ─── Yahoo monthly close cache ─────────────────────────────────────────────────

def _b3_yahoo_symbol(ticker: str) -> str | None:
    """B3 ticker like 'PETR4' -> 'PETR4.SA'. Non-B3 tickers return None.

    Subscription-rights tickers (XXXX12 for FIIs, XXXX1/XXXX2 for stocks)
    are transient instruments tied to follow-on offerings; they don't
    have Yahoo coverage and shouldn't be valued at market price.
    """
    import re
    if not re.match(r"^[A-Z]{4}\d{1,2}[A-Z]?$", ticker):
        return None
    # Strip .SA-incompatible right-issue tickers
    # FII subscription receipts: XXXX12 (vs XXXX11 base)
    if ticker.endswith("12") and len(ticker) == 6:
        return None
    # Stock subscription rights: XXXX1, XXXX2 (vs XXXX3/4 base)
    if re.match(r"^[A-Z]{4}[12]$", ticker):
        return None
    return ticker + ".SA"


def _is_yahoo_fetchable(symbol: str) -> bool:
    """A symbol is Yahoo-fetchable only if it's the .SA-suffixed B3 form."""
    return symbol.endswith(".SA") and len(symbol) >= 7


def _load_prices_cache() -> dict[tuple[str, str], float]:
    """Returns dict[(symbol, yyyy-mm)] -> close (BRL)."""
    out: dict[tuple[str, str], float] = {}
    if not PRICES_CACHE.exists():
        return out
    with PRICES_CACHE.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out[(r["symbol"], r["yyyymm"])] = float(r["close"])
    return out


def _save_prices_cache(cache: dict[tuple[str, str], float]) -> None:
    with PRICES_CACHE.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "yyyymm", "close"])
        for (sym, ym), close in sorted(cache.items()):
            w.writerow([sym, ym, f"{close:.6f}"])


def fetch_monthly_closes(symbol: str, start: date, end: date) -> dict[str, float]:
    """Returns dict[yyyy-mm] -> last close of that month."""
    period1 = int(datetime(start.year, start.month, 1).timestamp())
    period2 = int(datetime(end.year, end.month, 28).timestamp()) + 7 * 86400
    r = requests.get(
        YAHOO_URL.format(symbol=symbol),
        params={"interval": "1mo", "period1": period1, "period2": period2},
        headers=YAHOO_HEADERS,
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    chart = data.get("chart", {}).get("result")
    if not chart:
        return {}
    chart = chart[0]
    timestamps = chart.get("timestamp") or []
    closes = chart.get("indicators", {}).get("quote", [{}])[0].get("close") or []
    out: dict[str, float] = {}
    for ts, c in zip(timestamps, closes):
        if c is None:
            continue
        d = datetime.utcfromtimestamp(ts).date()
        out[f"{d.year:04d}-{d.month:02d}"] = float(c)
    return out


def warm_price_cache(symbols: set[str], start: date, end: date) -> dict[tuple[str, str], float]:
    """Fetch any missing (symbol, month) pairs from Yahoo and persist."""
    cache = _load_prices_cache()
    needed = []
    for sym in sorted(symbols):
        # Quick coverage check: do we have at least one close in [start..end]?
        have_any = any(k[0] == sym for k in cache)
        if not have_any:
            needed.append(sym)
    if not needed:
        return cache
    print(f"Fetching Yahoo monthly closes for {len(needed)} symbol(s) ...")
    for sym in needed:
        try:
            closes = fetch_monthly_closes(sym, start, end)
            for ym, c in closes.items():
                cache[(sym, ym)] = c
            print(f"  {sym}: {len(closes)} months")
        except Exception as e:
            print(f"  {sym}: FAILED - {e}")
    _save_prices_cache(cache)
    return cache


def lookup_price(cache: dict[tuple[str, str], float], symbol: str, target: date) -> float | None:
    """Return close for the given month, or fall back to most recent prior month."""
    ym = f"{target.year:04d}-{target.month:02d}"
    if (symbol, ym) in cache:
        return cache[(symbol, ym)]
    # Walk backward up to 6 months for stale tickers (e.g. delisted)
    cur = target
    for _ in range(6):
        cur = (cur.replace(day=1) - timedelta(days=1))
        ym = f"{cur.year:04d}-{cur.month:02d}"
        if (symbol, ym) in cache:
            return cache[(symbol, ym)]
    return None


# ─── Ledger replay ─────────────────────────────────────────────────────────────

@dataclass
class Holding:
    ticker: str
    nome: str
    valuation_method: str = "manual"  # 'market_price' or 'manual'
    yahoo_symbol: str | None = None
    qty: float = 0.0
    cost_basis: float = 0.0  # cumulative buy_value - sell_value (cash invested net)


def load_positions_meta() -> dict[str, dict]:
    """ticker -> {valuation_method, ticker_yahoo, nome}."""
    out: dict[str, dict] = {}
    with POSITIONS.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            out[r["key"]] = {
                "valuation_method": r["valuation_method"] or "manual",
                "ticker_yahoo": r["ticker_yahoo"] or None,
                "nome": r["nome"],
            }
    return out


def load_ledger() -> list[dict]:
    rows: list[dict] = []
    with LEDGER.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            r["_date"] = parse_date(r["data"])
            r["quantidade"] = float(r["quantidade"] or 0)
            r["valor"] = float(r["valor"] or 0)
            rows.append(r)
    rows.sort(key=lambda x: (x["_date"] or date(1900, 1, 1), x["ticker"]))
    return rows


def load_xp_snapshots() -> list[dict]:
    out: list[dict] = []
    with XP_SNAP.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            d = parse_date(r["data"])
            if not d:
                continue
            out.append({"date": d, "patrimonio": float(r["patrimonio"] or 0)})
    out.sort(key=lambda x: x["date"])
    return out


# ─── Main computation ──────────────────────────────────────────────────────────

XP_LEGACY_TICKER = "_XP_LEGACY_"
XP_LEGACY_OPENING = 118_257.07  # sum of Oct/2019 XP holdings (last snapshot)


def replay_through(rows: list[dict], cutoff: date,
                   meta: dict[str, dict]) -> dict[str, Holding]:
    """Apply all ledger rows on or before cutoff to a fresh holdings dict.

    Seeds an _XP_LEGACY_ synthetic holding worth the Oct/2019 XP patrimônio
    so that maturing pre-2019 instruments (Tesouro Selic 2025, CDB Pan,
    CDB Original) drain a real opening balance rather than leaving a
    real-ticker cost basis negative.
    """
    holdings: dict[str, Holding] = {}
    # Seed XP legacy
    holdings[XP_LEGACY_TICKER] = Holding(
        ticker=XP_LEGACY_TICKER,
        nome="XP holdings carry-over (Oct/2019)",
        valuation_method="manual",
        yahoo_symbol=None,
        qty=1.0,
        cost_basis=XP_LEGACY_OPENING,
    )
    for r in rows:
        d = r["_date"]
        if d is None or d > cutoff:
            continue
        cat = r["categoria"]
        if cat in ("IGNORE", "UNKNOWN"):
            continue
        ticker = r["ticker"] or "_unknown"
        h = holdings.get(ticker)
        if h is None:
            m = meta.get(ticker, {})
            raw_ys = m.get("ticker_yahoo")
            ys = raw_ys if (raw_ys and _is_yahoo_fetchable(raw_ys)) else None
            if not ys:
                ys = _b3_yahoo_symbol(ticker)
            vm = m.get("valuation_method", "manual")
            # Normalize: 'tesouro' / 'manual' both mean cost-basis valuation here
            if vm == "tesouro":
                vm = "manual"
            h = Holding(
                ticker=ticker,
                nome=r["nome"] or ticker,
                valuation_method=vm,
                yahoo_symbol=ys,
            )
            # Heuristic: if no positions.csv hit but the ticker looks like B3, market_price
            if ys and h.valuation_method == "manual":
                h.valuation_method = "market_price"
            holdings[ticker] = h

        qty = r["quantidade"]
        val = r["valor"]
        if cat == "BUY":
            h.qty += qty
            h.cost_basis += val
        elif cat == "SELL":
            h.qty -= qty
            h.cost_basis -= val
            # If the SELL produced negative cost basis (more sold than ever bought
            # in the ledger), the excess came from XP-era holdings. Drain that
            # excess from _XP_LEGACY_ to keep the books balanced.
            if h.cost_basis < -1.0 and ticker != XP_LEGACY_TICKER:
                excess = -h.cost_basis
                xp = holdings[XP_LEGACY_TICKER]
                xp.cost_basis -= excess
                h.cost_basis = 0.0
        elif cat == "PORTABILITY_IN":
            h.qty += qty  # custody transfer; no cost basis change
        elif cat == "PORTABILITY_OUT":
            h.qty -= qty
        elif cat == "BONUS":
            h.qty += qty  # free shares; no cost basis change
        elif cat == "SPLIT":
            # Direction matters: C means received, D means surrendered (grupamento)
            if r["entrada_saida"] == "D":
                h.qty -= qty
            else:
                h.qty += qty
        elif cat == "INCOME":
            # Cash to investor: qty unchanged; reduces "money locked in" basis
            h.cost_basis -= val
        elif cat == "FEE":
            h.cost_basis += val  # fee paid raises cost basis (less common interp)
    return holdings


def value_holdings(holdings: dict[str, Holding],
                   cache: dict[tuple[str, str], float],
                   at: date) -> tuple[float, float, list[dict]]:
    """Returns (total_value, total_cost_basis, [debug_rows])."""
    total_v = 0.0
    total_c = 0.0
    debug: list[dict] = []
    for h in holdings.values():
        if abs(h.qty) < 1e-6 and abs(h.cost_basis) < 1.0:
            continue
        if h.valuation_method == "market_price" and h.yahoo_symbol:
            price = lookup_price(cache, h.yahoo_symbol, at)
            if price is None:
                # Fall back to cost basis if market price unavailable
                v = max(h.cost_basis, 0.0)
            else:
                v = h.qty * price
        else:
            # Manual valuation: use cost basis (no growth modeled v1)
            v = max(h.cost_basis, 0.0)
        total_v += v
        total_c += max(h.cost_basis, 0.0)
        debug.append({"ticker": h.ticker, "qty": h.qty,
                      "value": v, "basis": h.cost_basis,
                      "method": h.valuation_method})
    return total_v, total_c, debug


def monthly_cashflows(rows: list[dict]) -> dict[str, float]:
    """yyyy-mm -> net deposits (BUY - SELL - INCOME)."""
    cf: dict[str, float] = defaultdict(float)
    for r in rows:
        d = r["_date"]
        if d is None:
            continue
        ym = f"{d.year:04d}-{d.month:02d}"
        cat = r["categoria"]
        val = r["valor"]
        if cat == "BUY":
            cf[ym] += val
        elif cat == "SELL":
            cf[ym] -= val
        elif cat == "INCOME":
            cf[ym] -= val
    return cf


def main() -> int:
    if not LEDGER.exists() or not XP_SNAP.exists() or not POSITIONS.exists():
        print("Missing inputs. Run parse_b3_excel.py, parse_xp_positions.py, "
              "and net_positions.py first.")
        return 1

    print("Loading inputs ...")
    meta = load_positions_meta()
    ledger = load_ledger()
    xp_snaps = load_xp_snapshots()
    print(f"  positions:    {len(meta)}")
    print(f"  ledger rows:  {len(ledger)}")
    print(f"  XP snapshots: {len(xp_snaps)}")

    # Determine date range
    first_date = xp_snaps[0]["date"] if xp_snaps else ledger[0]["_date"]
    today = date.today()
    last_date = month_end(today.replace(day=1) - timedelta(days=1))  # last completed month

    # Collect all symbols we'll need. Two filters:
    #  - meta.ticker_yahoo only used if it ends in .SA (skip Tesouro-style names)
    #  - fall back to _b3_yahoo_symbol() which itself filters subscription rights
    symbols: set[str] = set()
    for r in ledger:
        ticker = r["ticker"]
        m = meta.get(ticker, {})
        ys = m.get("ticker_yahoo")
        if ys and not _is_yahoo_fetchable(ys):
            ys = None
        if not ys:
            ys = _b3_yahoo_symbol(ticker)
        if ys:
            symbols.add(ys)

    cache = warm_price_cache(symbols, first_date, last_date)
    print(f"Price cache: {len(cache)} entries across {len(symbols)} symbols")

    # Build month-end series
    xp_by_month = {f"{s['date'].year:04d}-{s['date'].month:02d}": s for s in xp_snaps}
    cf_by_month = monthly_cashflows(ledger)

    history: list[dict] = []
    for me in month_iter(first_date, last_date):
        ym = f"{me.year:04d}-{me.month:02d}"

        if ym in xp_by_month:
            v = xp_by_month[ym]["patrimonio"]
            history.append({
                "date": me.isoformat(),
                "value": v,
                "cost_basis": v,  # XP snapshots: use patrimônio as basis proxy
                "cashflow": 0.0,
                "source": "XP_SNAPSHOT",
            })
            continue

        # B3 replay
        holdings = replay_through(ledger, me, meta)
        v, cb, _ = value_holdings(holdings, cache, me)
        history.append({
            "date": me.isoformat(),
            "value": v,
            "cost_basis": cb,
            "cashflow": cf_by_month.get(ym, 0.0),
            "source": "B3_REPLAY",
        })

    # Linearly interpolate any XP-period months that are missing from the
    # snapshot file (e.g., Feb/2019). Walk consecutive XP_SNAPSHOT anchors
    # and fill any B3_REPLAY-flagged months between them.
    xp_dates = sorted(xp_by_month.keys())
    for j in range(len(xp_dates) - 1):
        a_ym = xp_dates[j]
        b_ym = xp_dates[j + 1]
        a_idx = next((i for i, h in enumerate(history) if h["date"].startswith(a_ym)), None)
        b_idx = next((i for i, h in enumerate(history) if h["date"].startswith(b_ym)), None)
        if a_idx is None or b_idx is None or b_idx - a_idx <= 1:
            continue
        v_a = history[a_idx]["value"]
        v_b = history[b_idx]["value"]
        steps = b_idx - a_idx
        for k in range(1, steps):
            mid = history[a_idx + k]
            if mid["source"] != "B3_REPLAY":
                continue
            mid["value"] = v_a + (v_b - v_a) * (k / steps)
            mid["cost_basis"] = mid["value"]
            mid["source"] = "XP_INTERP"

    # Bridge gap month between last XP snap and first B3 replay (Nov/2019).
    # The B3 replay value for Nov/2019 is just the SELL of Tesouro on Dec 18 → 0
    # so we linearly interpolate between XP Oct/19 patrimônio and the first
    # non-zero B3 replay month.
    if xp_snaps:
        last_xp = max(xp_snaps, key=lambda x: x["date"])
        last_xp_ym = f"{last_xp['date'].year:04d}-{last_xp['date'].month:02d}"
        last_xp_idx = next((i for i, h in enumerate(history)
                            if h["date"].startswith(last_xp_ym)), None)
        if last_xp_idx is not None:
            for i in range(last_xp_idx + 1, len(history)):
                h = history[i]
                if h["source"] == "B3_REPLAY" and h["value"] < 0.5 * last_xp["patrimonio"]:
                    # B3 ledger doesn't yet cover this month; carry forward XP value
                    h["value"] = last_xp["patrimonio"]
                    h["cost_basis"] = last_xp["patrimonio"]
                    h["source"] = "XP_CARRY"
                else:
                    break

    # Compute Modified Dietz monthly TWR.
    #
    # For B3_REPLAY months we have real cashflows (BUY/SELL/INCOME).
    # For XP_SNAPSHOT months we have only patrimônio — apply a heuristic:
    # if month-over-month change exceeds ±5% (XP was bond-heavy), treat the
    # excess as a deposit/withdrawal rather than a return. This avoids the
    # +900% / -100% spikes that come from undisclosed cashflows.
    cum_twr = 1.0
    XP_RETURN_CAP = 0.05  # 5% per month, conservative for fixed-income
    for i, h in enumerate(history):
        if i == 0:
            h["monthly_return_pct"] = 0.0
            h["twr_cumulative_pct"] = 0.0
            continue
        prev = history[i - 1]
        v_start = prev["value"]
        v_end = h["value"]
        cf = h["cashflow"]

        # Heuristic: synthesize implicit cashflow for XP-only periods
        if h["source"] in ("XP_SNAPSHOT", "XP_CARRY") and cf == 0 and v_start > 0:
            naive_r = (v_end - v_start) / v_start
            if abs(naive_r) > XP_RETURN_CAP:
                # Cap the return at ±5%; rest is implicit cashflow
                capped_r = XP_RETURN_CAP if naive_r > 0 else -XP_RETURN_CAP
                cf = v_end - v_start * (1 + capped_r)

        denom = v_start + 0.5 * cf
        if denom <= 0:
            r = 0.0
        else:
            r = (v_end - v_start - cf) / denom
        # Final safety: clamp absurd returns
        r = max(-0.5, min(r, 0.5))
        cum_twr *= (1 + r)
        h["monthly_return_pct"] = round(r * 100, 4)
        h["twr_cumulative_pct"] = round((cum_twr - 1) * 100, 4)
        h["cashflow"] = cf  # persist the (possibly synthesized) cashflow

    # Write CSV
    fields = ["date", "portfolio_value_brl", "cost_basis_brl",
              "net_cashflow_month_brl", "monthly_return_pct",
              "twr_cumulative_pct", "source"]
    with OUTPUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(fields)
        for h in history:
            w.writerow([
                h["date"],
                f"{h['value']:.2f}",
                f"{h['cost_basis']:.2f}",
                f"{h['cashflow']:.2f}",
                f"{h['monthly_return_pct']:.4f}",
                f"{h['twr_cumulative_pct']:.4f}",
                h["source"],
            ])

    print(f"\nWrote {len(history)} months to {OUTPUT}")
    print("\n=== SUMMARY ===")
    for h in history[::6]:  # every 6 months
        print(f"  {h['date']}  R$ {h['value']:>14,.2f}  "
              f"cf={h['cashflow']:>+12,.2f}  "
              f"TWR={h['twr_cumulative_pct']:>+8.2f}%  "
              f"[{h['source']}]")
    last = history[-1]
    print(f"\nFinal ({last['date']}): "
          f"value=R$ {last['value']:,.2f}  "
          f"cost_basis=R$ {last['cost_basis']:,.2f}  "
          f"TWR cumulative={last['twr_cumulative_pct']:+.2f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
