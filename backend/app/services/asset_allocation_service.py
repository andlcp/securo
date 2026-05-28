"""Asset allocation targets — group every active asset under a fixed set
of "buckets" the user actually thinks in (Ações, FIIs, Renda Fixa,
Reserva de Valor, Stocks e ETFs Americanos, Outros, R. Emergência) and
compute current % + deficit-vs-target for each.

The buckets were chosen to mirror the Bastter System "Patrimônio
Consolidado" widget the user wants to replicate. The mapping is purely
derived from existing asset metadata (asset_class + type); nothing has
to be tagged manually:

    R_EMERGENCIA    — no auto-mapping (manual reserve placeholder)
    ACOES           — RENDA_VARIAVEL_BR AND type='stock'
    FIIS            — asset_class='FIIS'
    OUTROS          — FUNDOS + OUTRO (loans, misc)
    RENDA_FIXA      — RENDA_FIXA
    RESERVA_VALOR   — CRIPTO
    STOCKS          — STOCKS_US AND type='stock', OR type='etf' (label
                      "Stocks e ETFs Americanos" — user holds only
                      US-exposure ETFs today: IVVB11 BR-listed +
                      QQQM/SCHD US-listed, treated together). When a
                      BR-market ETF (e.g. BOVA11) joins the portfolio,
                      split this back into a separate ETF bucket.

Targets live in user.preferences['asset_allocation_targets'] as a flat
dict { 'ACOES': 20.0, ... }. Missing keys default to 0.

"Onde aportar" math:
    deficit_brl = max(0, target_pct/100 * total_brl - current_brl)

Soma dos deficits positivos = quanto aportar pra rebalancear (cobrindo
só as classes deficitárias, sem vender as que estão acima). O share %
por classe é deficit_brl / sum(deficits) — útil pra split de aportes
quando o user só consegue aportar parte do total.
"""
import uuid
from decimal import Decimal
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.asset import Asset
from app.models.asset_value import AssetValue
from app.models.user import User
from app.services.fx_rate_service import get_rate


# Bucket order matches the Bastter layout the user referenced — alphabetical
# inside two groups: actively-managed first (Ações → Stocks), reserves last.
# ETFS bucket was merged into STOCKS as "Stocks e ETFs Americanos" — keep
# STOCKS as the bucket id (preserves the user's existing target) and just
# updates the label + classifier.
BUCKETS: list[tuple[str, str]] = [
    ("R_EMERGENCIA",  "R. Emergência"),
    ("ACOES",         "Ações"),
    ("FIIS",          "FIIs"),
    ("OUTROS",        "Outros"),
    ("RENDA_FIXA",    "Renda Fixa"),
    ("RESERVA_VALOR", "Reserva de Valor"),
    ("STOCKS",        "Stocks e ETFs Americanos"),
]


def _bucket_for(asset: Asset) -> Optional[str]:
    """Return the bucket id for an asset, or None if it doesn't fit any.
    The classification mirrors the docstring at the top — change there if
    you tweak the mapping."""
    ac = asset.asset_class
    t = asset.type
    # All ETFs currently in the user's portfolio are American-exposure
    # (IVVB11 tracks SP500 in BRL; QQQM and SCHD are US-listed). They
    # share the STOCKS bucket. Reintroduce a separate ETFS bucket only
    # when a non-American-exposure ETF lands in the portfolio.
    if t == "etf":
        return "STOCKS"
    if ac == "RENDA_VARIAVEL_BR" and t == "stock":
        return "ACOES"
    if ac == "STOCKS_US" and t == "stock":
        return "STOCKS"
    if ac == "FIIS":
        return "FIIS"
    if ac == "RENDA_FIXA":
        return "RENDA_FIXA"
    if ac == "CRIPTO":
        return "RESERVA_VALOR"
    if ac in ("FUNDOS", "OUTRO"):
        return "OUTROS"
    return None


def _read_targets(user: User) -> dict[str, float]:
    """Pull stored targets from user.preferences. Tolerates missing keys
    and bad types — anything unparseable becomes 0."""
    prefs = user.preferences or {}
    raw = prefs.get("asset_allocation_targets") or {}
    out: dict[str, float] = {bid: 0.0 for bid, _ in BUCKETS}
    if isinstance(raw, dict):
        for bid, label in BUCKETS:
            v = raw.get(bid)
            try:
                if v is not None:
                    out[bid] = float(v)
            except (TypeError, ValueError):
                pass
    return out


def _read_excluded(user: User) -> set[str]:
    """Bucket ids the user opted to leave OUT of the allocation math.

    Use case: "Outros" holds family loans the user wants gone — counting
    them distorts every other class's % and the aporte plan. Excluded
    buckets still appear in the payload (so the checkbox is visible and
    re-checkable) but contribute nothing to the considered total, the
    deficits, or the targets sum."""
    prefs = user.preferences or {}
    raw = prefs.get("asset_allocation_excluded") or []
    valid = {bid for bid, _ in BUCKETS}
    if isinstance(raw, list):
        return {str(x) for x in raw if x in valid}
    return set()


async def compute_allocation(
    session: AsyncSession, user: User
) -> dict:
    """Build the full allocation payload the frontend renders.

    Returns:
        {
          "primary_currency": "BRL",
          "total_brl": <float>,
          "categories": [
            { "id": "ACOES", "label": "Ações",
              "total_brl": 236264.85, "current_pct": 20.64,
              "target_pct": 20.0, "delta_pp": -0.64,
              "deficit_brl": 0.0, "deficit_share_pct": 0.0 },
            ...
          ],
          "targets_sum": 100.0,
          "deficit_total_brl": <sum of positive deficits>
        }
    """
    primary = user.primary_currency or "BRL"

    # Pull every active, non-sold asset for the user (across all wallets —
    # this is a global view by design, per the user's request).
    assets = (await session.execute(
        select(Asset).where(
            Asset.user_id == user.id,
            Asset.is_archived == False,    # noqa: E712
            Asset.sell_date.is_(None),
        )
    )).scalars().all()

    # Bulk-load latest AVs in one round-trip.
    asset_ids = [a.id for a in assets]
    latest_by_asset: dict[uuid.UUID, Decimal] = {}
    if asset_ids:
        latest_rows = (await session.execute(
            select(AssetValue.asset_id, AssetValue.amount)
            .where(AssetValue.asset_id.in_(asset_ids))
            .order_by(AssetValue.asset_id, AssetValue.date.desc())
            .distinct(AssetValue.asset_id)
        )).all()
        for aid, amt in latest_rows:
            latest_by_asset[aid] = Decimal(str(amt))

    # Pre-load FX rates for every foreign currency in one shot.
    foreign = {a.currency for a in assets if a.currency != primary}
    rate_by_ccy: dict[str, Decimal] = {}
    for ccy in foreign:
        rate_by_ccy[ccy] = await get_rate(session, ccy, primary)

    # Aggregate per bucket.
    by_bucket: dict[str, Decimal] = {bid: Decimal("0") for bid, _ in BUCKETS}
    for asset in assets:
        bucket = _bucket_for(asset)
        if bucket is None:
            continue
        amt = latest_by_asset.get(asset.id)
        if amt is None:
            # Fall back to cost basis (purchase_price × units) so a freshly
            # created asset without AV history still shows up.
            if asset.purchase_price is None or asset.units is None:
                continue
            amt = Decimal(str(asset.purchase_price)) * Decimal(str(asset.units))
        if asset.currency != primary:
            rate = rate_by_ccy.get(asset.currency)
            if rate is not None:
                amt = amt * rate
        by_bucket[bucket] += amt

    targets = _read_targets(user)
    excluded = _read_excluded(user)
    # The considered total is the rebalancing universe — excluded buckets
    # (e.g. family loans parked in "Outros") sit outside it so they don't
    # skew every other class's % or the aporte split.
    total = sum(
        (v for bid, v in by_bucket.items() if bid not in excluded),
        Decimal("0"),
    )
    full_total = sum(by_bucket.values(), Decimal("0"))

    # First pass: compute current_pct, target_pct, delta_pp, raw deficit.
    raw_deficits: dict[str, Decimal] = {}
    cats_partial: list[dict] = []
    for bid, label in BUCKETS:
        v = by_bucket[bid]
        is_excluded = bid in excluded
        if is_excluded:
            # Outside the rebalancing universe: keep the R$ value visible
            # but no %, no target pressure, no deficit.
            raw_deficits[bid] = Decimal("0")
            cats_partial.append({
                "id": bid,
                "label": label,
                "total_brl": float(v),
                "current_pct": 0.0,
                "target_pct": float(targets.get(bid, 0.0)),
                "delta_pp": 0.0,
                "deficit_brl": 0.0,
                "excluded": True,
                "_above_target": False,
            })
            continue
        current_pct = float(v / total * 100) if total > 0 else 0.0
        target_pct = float(targets.get(bid, 0.0))
        deficit = (Decimal(str(target_pct)) / Decimal("100")) * total - v
        # Negative deficit = above target. We only treat positive ones
        # as "needs aporte"; clamp negatives to 0 for the share/total math.
        positive_deficit = deficit if deficit > 0 else Decimal("0")
        raw_deficits[bid] = positive_deficit
        cats_partial.append({
            "id": bid,
            "label": label,
            "total_brl": float(v),
            "current_pct": round(current_pct, 4),
            "target_pct": round(target_pct, 4),
            "delta_pp": round(current_pct - target_pct, 4),
            "deficit_brl": float(positive_deficit),
            "excluded": False,
            "_above_target": deficit < 0,
        })

    # Second pass: share = positive_deficit / sum(positive_deficits).
    sum_positive_deficits = sum(raw_deficits.values(), Decimal("0"))
    for cat in cats_partial:
        d = Decimal(str(cat["deficit_brl"]))
        share = (d / sum_positive_deficits * 100) if sum_positive_deficits > 0 else Decimal("0")
        cat["deficit_share_pct"] = round(float(share), 4)
        # Above-target marker is informational only — keep delta_pp; drop
        # the flag from the wire format (frontend infers from sign).
        cat.pop("_above_target", None)

    # Targets sum is checked only over the INCLUDED buckets — excluding a
    # bucket with a non-zero target should make the "soma 100%" warning
    # fire so the user re-normalizes (excluding a 0%-target bucket like
    # Outros leaves the sum untouched).
    targets_sum = sum(
        (targets.get(bid, 0.0) for bid, _ in BUCKETS if bid not in excluded),
        0.0,
    )

    return {
        "primary_currency": primary,
        "total_brl": float(total),
        "full_total_brl": float(full_total),
        "categories": cats_partial,
        "targets_sum": round(targets_sum, 4),
        "deficit_total_brl": float(sum_positive_deficits),
        "excluded_ids": sorted(excluded),
    }


async def compute_aporte_plan(
    session: AsyncSession, user: User, aporte_brl: float
) -> dict:
    """Distribute a one-shot contribution `aporte_brl` across the
    under-target buckets and report the resulting allocation.

    Why this exists: the plain `compute_allocation` deficit is measured
    against the *current* total, so "aporte R$ 87 k em RF" doesn't
    actually take RF to 60 % — the act of contributing grows the total
    and pushes the 60 % line up with it. This function answers the
    question the user actually has each month ("tenho R$ X, como divido
    e onde isso me deixa?") by computing everything against the
    POST-aporte total (T + X) and showing the real resulting %.

    Algorithm (deliberately matches the widget's mental model):
      1. TA = current_total + aporte
      2. deficit_i = max(0, target_i % × TA − current_i)   ← vs post total
      3. if aporte <= Σdeficit: split proportional to deficit_i
         else: fill every under-target bucket to target, then spread the
         leftover proportional to the under-target buckets' target weights
         (keeps them at target ratios instead of dumping it all in one).

    Proportional-to-deficit (rather than greedy water-fill) is chosen for
    consistency with the existing "% do aporte" column; the resulting-%
    readout makes the outcome honest regardless of the split rule.
    """
    base = await compute_allocation(session, user)
    primary = base["primary_currency"]
    T = Decimal(str(base["total_brl"]))
    X = Decimal(str(max(aporte_brl, 0.0)))
    TA = T + X

    # Per-bucket deficit measured against the POST-aporte total. Excluded
    # buckets (carried through from compute_allocation) never receive an
    # aporte — they're outside the rebalancing universe.
    cats = base["categories"]
    deficits: dict[str, Decimal] = {}
    for cat in cats:
        if cat.get("excluded"):
            deficits[cat["id"]] = Decimal("0")
            continue
        target_amt = (Decimal(str(cat["target_pct"])) / Decimal("100")) * TA
        deficits[cat["id"]] = max(Decimal("0"), target_amt - Decimal(str(cat["total_brl"])))
    total_deficit = sum(deficits.values(), Decimal("0"))

    alloc: dict[str, Decimal] = {cat["id"]: Decimal("0") for cat in cats}
    if X > 0 and total_deficit > 0:
        if X <= total_deficit:
            for cid, d in deficits.items():
                alloc[cid] = X * d / total_deficit
        else:
            # Everything under-target gets filled to target…
            for cid, d in deficits.items():
                alloc[cid] = d
            # …then the surplus rides along the under-target buckets'
            # target weights so they stay in proportion to each other.
            leftover = X - total_deficit
            under = {cid: Decimal(str(next(c["target_pct"] for c in cats if c["id"] == cid)))
                     for cid, d in deficits.items() if d > 0}
            wsum = sum(under.values(), Decimal("0"))
            for cid, w in under.items():
                if wsum > 0:
                    alloc[cid] += leftover * w / wsum

    out_cats = []
    for cat in cats:
        add = alloc[cat["id"]]
        new_val = Decimal(str(cat["total_brl"])) + add
        new_pct = (new_val / TA * 100) if TA > 0 else Decimal("0")
        out_cats.append({
            "id": cat["id"],
            "label": cat["label"],
            "current_brl": cat["total_brl"],
            "current_pct": cat["current_pct"],
            "target_pct": cat["target_pct"],
            "aporte_brl": round(float(add), 2),
            "aporte_share_pct": round(float(add / X * 100), 4) if X > 0 else 0.0,
            "result_brl": round(float(new_val), 2),
            "result_pct": round(float(new_pct), 4),
            "result_delta_pp": round(float(new_pct) - cat["target_pct"], 4),
        })

    return {
        "primary_currency": primary,
        "total_brl": float(T),
        "aporte_brl": float(X),
        "total_after_brl": float(TA),
        # Sum of positive deficits vs the post-aporte total — the amount
        # that would still be missing to fully hit targets after this aporte.
        "remaining_deficit_brl": round(float(max(total_deficit - X, Decimal("0"))), 2),
        "categories": out_cats,
    }


async def save_targets(
    session: AsyncSession, user: User, targets: dict[str, float]
) -> User:
    """Persist new target percentages to user.preferences. Unknown bucket
    ids are dropped; missing ones default to 0 on read.

    Doesn't enforce that the targets sum to 100 — the frontend renders a
    warning when they don't, but the user can save partial drafts."""
    valid_ids = {bid for bid, _ in BUCKETS}
    cleaned: dict[str, float] = {}
    for k, v in (targets or {}).items():
        if k not in valid_ids:
            continue
        try:
            n = float(v)
        except (TypeError, ValueError):
            continue
        if n < 0:
            n = 0.0
        if n > 100:
            n = 100.0
        cleaned[k] = round(n, 4)

    prefs = dict(user.preferences or {})
    prefs["asset_allocation_targets"] = cleaned
    user.preferences = prefs
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user


async def save_excluded(
    session: AsyncSession, user: User, excluded: list[str]
) -> User:
    """Persist the set of buckets to exclude from the allocation math.
    Unknown bucket ids are dropped silently."""
    valid_ids = {bid for bid, _ in BUCKETS}
    cleaned = sorted({x for x in (excluded or []) if x in valid_ids})

    prefs = dict(user.preferences or {})
    prefs["asset_allocation_excluded"] = cleaned
    user.preferences = prefs
    session.add(user)
    await session.commit()
    await session.refresh(user)
    return user
