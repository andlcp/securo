#!/usr/bin/env python3
"""
replay_renda_fixa.py

Reconstrói posições mensais de Tesouro Direto e CDB e calcula MTM.

Inputs:
    rf_trades.csv                -- gerado por parse_b3_renda_fixa.py
    tesouro_initial_positions.csv-- posições anteriores ao período do extrato
    tesouro_prices_cache.csv     -- gerado por fetch_tesouro_prices.py
    cdi_daily_factors.csv        -- série diária do CDI (gerada aqui se ausente)

Lógica de MTM:
    Tesouro: V = qty * PU(month_end)  -- preços públicos do Tesouro Transparente.
             Se um título já venceu antes do month_end, considera-se que o
             dinheiro saiu da carteira (V_RF = 0 a partir do mês seguinte) e
             o resgate aparece como "venda" no fluxo de caixa.
    CDB:     V = sum(per-position) qty * pu_buy * compound_cdi_factor *
             cdi_pct (default 110 % do CDI ao dia, configurável).
             No "vencimento" (toda Venda de CDB pela política do usuário)
             usa o pu observado na transação.

Saídas:
    rf_holdings_monthly.csv  -- (month_end, titulo, codigo, tipo, vencimento,
                                 qty, valor_mtm, valor_bruto_estimado)
    rf_cashflow_monthly.csv  -- (month_end, buy, sell_liquido, sell_bruto,
                                 ir_estimado_mes)
    rf_final.csv             -- snapshot da posição atual

A coluna sell_bruto faz o gross-up via tabela regressiva pelo holding period.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta


# ---------- helpers ----------

def parse_iso(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def month_end(y: int, m: int) -> date:
    if m == 12:
        return date(y, 12, 31)
    return date(y, m + 1, 1) - timedelta(days=1)


def iter_months(start_iso: str, end_iso: str):
    sy, sm = int(start_iso[:4]), int(start_iso[5:7])
    ey, em = int(end_iso[:4]), int(end_iso[5:7])
    y, m = sy, sm
    while (y, m) <= (ey, em):
        yield (y, m)
        m += 1
        if m > 12:
            y, m = y + 1, 1


# ---------- IR table ----------

def ir_aliquota(holding_days: int) -> float:
    if holding_days <= 180:
        return 0.225
    if holding_days <= 360:
        return 0.20
    if holding_days <= 720:
        return 0.175
    return 0.15


# ---------- CDI ----------

def fetch_bcb_cdi_daily(start: str, end: str) -> dict[str, float]:
    """Fetches BCB SGS series 12 (CDI diária % a.d.) in given range.
    Returns {YYYY-MM-DD: factor (1 + rate/100)} for each business day reported.
    """
    sd = datetime.fromisoformat(start).strftime("%d/%m/%Y")
    ed = datetime.fromisoformat(end).strftime("%d/%m/%Y")
    url = (f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.12/dados"
           f"?formato=json&dataInicial={sd}&dataFinal={ed}")
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=120) as r:
        rows = json.load(r)
    out = {}
    for row in rows:
        d = datetime.strptime(row["data"], "%d/%m/%Y").date()
        rate = float(row["valor"]) / 100.0
        out[d.isoformat()] = 1.0 + rate
    return out


def load_or_fetch_cdi_factors(path: str, start: str, end: str) -> dict[str, float]:
    if os.path.exists(path):
        out: dict[str, float] = {}
        for r in csv.DictReader(open(path, encoding="utf-8")):
            out[r["data"]] = float(r["fator"])
        # Check if covers needed range; refetch if start is before earliest cached
        if out and min(out) <= start and max(out) >= end[:7] + "-01":
            return out
    print(f"  Fetching BCB CDI {start} -> {end}...")
    out = fetch_bcb_cdi_daily(start, end)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data", "fator"])
        w.writeheader()
        for d, fa in sorted(out.items()):
            w.writerow({"data": d, "fator": round(fa, 10)})
    print(f"  Cached {len(out)} CDI daily factors -> {path}")
    return out


def cdi_compound_factor(daily: dict[str, float],
                        from_iso: str, to_iso: str,
                        pct: float = 1.0) -> float:
    """Compound factor of pct * CDI from from_iso (exclusive) to to_iso (inclusive)."""
    if from_iso >= to_iso:
        return 1.0
    f = 1.0
    for d_iso, fac in daily.items():
        if d_iso > from_iso and d_iso <= to_iso:
            r = (fac - 1.0) * pct
            f *= (1.0 + r)
    return f


# ---------- I/O ----------

def load_rf_trades(path: str) -> list[dict]:
    out = []
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out.append({
            "data": r["data"],
            "titulo": r["titulo"],
            "codigo": r["codigo"],
            "tipo": r["tipo"],
            "vencimento": r["vencimento"] or None,
            "operacao": r["operacao"],
            "qty": float(r["qty"] or 0),
            "pu": float(r["pu"] or 0),
            "valor": float(r["valor"] or 0),
            "instituicao": r.get("instituicao", ""),
        })
    return out


def load_initial_positions(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    out = []
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out.append({
            "data": r["data"],
            "titulo": r["titulo"],
            "codigo": r["titulo"],  # Tesouro: titulo == codigo
            "tipo": r["tipo"],
            "vencimento": r.get("vencimento") or None,
            "operacao": r["operacao"],
            "qty": float(r["qty"]),
            "pu": float(r["pu"]),
            "valor": float(r["valor"]),
            "instituicao": r.get("instituicao", ""),
        })
    return out


def load_tesouro_prices(path: str) -> dict[tuple[str, str], dict[str, float]]:
    """Returns {(tipo, vencimento_iso) -> {month_end_iso: pu}}."""
    out: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    for r in csv.DictReader(open(path, encoding="utf-8")):
        out[(r["titulo_tipo"], r["vencimento"])][r["month_end"]] = \
            float(r["pu_close"])
    return out


# ---------- replay ----------

class Lot:
    """A single buy lot — used for FIFO accounting on sells."""
    __slots__ = ("titulo", "codigo", "tipo", "vencimento", "qty", "pu_buy",
                 "valor_buy", "date_buy", "qty_remaining")

    def __init__(self, t: dict):
        self.titulo = t["titulo"]
        self.codigo = t["codigo"]
        self.tipo = t["tipo"]
        self.vencimento = t["vencimento"]
        self.qty = t["qty"]
        self.pu_buy = t["pu"]
        self.valor_buy = t["valor"]
        self.date_buy = t["data"]
        self.qty_remaining = t["qty"]


def get_pu_tesouro(prices: dict, tipo: str, venc: str, me_iso: str) -> float | None:
    """Look up PU at month_end. Carry-forward if missing for that month."""
    series = prices.get((tipo, venc))
    if not series:
        return None
    if me_iso in series:
        return series[me_iso]
    past = [m for m in series if m <= me_iso]
    if past:
        return series[max(past)]
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--trades", default="rf_trades.csv")
    ap.add_argument("--initial", default="tesouro_initial_positions.csv")
    ap.add_argument("--tesouro-prices", default="tesouro_prices_cache.csv")
    ap.add_argument("--cdi-cache", default="cdi_daily_factors.csv")
    ap.add_argument("--cdb-pct", type=float, default=1.05,
                    help="Multiplicador do CDI para MTM de CDBs em aberto "
                         "(default 1.05 = 105%%, taxa mínima típica do "
                         "investidor). Usado quando não há venda de "
                         "referência ainda.")
    ap.add_argument("--ignore-codes", default="CDB322JW5Z4",
                    help="Códigos a ignorar (csv). Default ignora o CDB Modal "
                         "negativo do extrato.")
    ap.add_argument("--monthly-out", default="rf_holdings_monthly.csv")
    ap.add_argument("--cashflow-out", default="rf_cashflow_monthly.csv")
    ap.add_argument("--final-out", default="rf_final.csv")
    args = ap.parse_args()

    ignore = {c.strip() for c in args.ignore_codes.split(",") if c.strip()}

    # Load
    trades = load_rf_trades(args.trades)
    initial = load_initial_positions(args.initial)
    tesouro_prices = load_tesouro_prices(args.tesouro_prices)
    print(f"Loaded {len(trades)} RF trades + {len(initial)} initial positions")

    # Apply ignore filter
    trades = [t for t in trades if t["codigo"] not in ignore]
    if args.ignore_codes:
        print(f"Ignoring codes: {sorted(ignore)}")

    # Combine initial + trades, sort by date
    all_events = sorted(initial + trades, key=lambda t: (t["data"], t["operacao"]))

    if not all_events:
        print("No events; aborting.")
        return 1

    start = all_events[0]["data"]
    today_iso = date.today().isoformat()
    end = today_iso

    # CDI factors
    cdi = load_or_fetch_cdi_factors(args.cdi_cache, start, end)

    # ---------- Auto-resgate de vencimento ----------
    # B3 nao registra "Vencimento" como evento. Para Tesouro, sempre que um
    # titulo vence sem ter sido vendido em sua totalidade, simulamos um SELL
    # automatico na data de vencimento ao PU oficial daquele dia.
    # Para CDB, a politica do usuario e: toda Venda = vencimento (i.e. nao
    # antecipa). Logo, CDBs em aberto na data limite ainda estao ativos.

    # Compute net qty per (titulo, codigo) at any point — to know which Tesouros
    # need an auto-resgate. We'll check on the maturity date.
    auto_resgates = []
    qty_track: dict[tuple[str, str], float] = defaultdict(float)
    for ev in all_events:
        key = (ev["titulo"], ev["codigo"])
        delta = ev["qty"] if ev["operacao"] == "BUY" else -ev["qty"]
        qty_track[key] += delta

    # Need cumulative qty by date per title to decide auto-resgate.
    # Strategy: scan events chronologically, at each point check if any title's
    # vencimento fell strictly between the previous event and the next while
    # qty > 0. Simpler: pre-compute per (titulo, codigo) the maturity, then
    # walk events to see if qty>0 at vencimento.

    # Group by titulo
    events_by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for ev in all_events:
        events_by_key[(ev["titulo"], ev["codigo"])].append(ev)

    for key, evs in events_by_key.items():
        sample = evs[0]
        if not sample["tipo"].startswith("TESOURO"):
            continue
        venc = sample["vencimento"]
        if not venc:
            continue
        # Skip if vencimento ainda no futuro — auto-resgate só faz sentido
        # para vencimentos passados (a posição segue ativa, com PU pública)
        if venc > today_iso:
            continue
        # qty at vencimento = sum of buys before venc - sum of sells before venc
        qty_at_venc = 0.0
        avg_pu_buy = 0.0
        cost_open = 0.0
        for ev in sorted(evs, key=lambda x: x["data"]):
            if ev["data"] > venc:
                break
            if ev["operacao"] == "BUY":
                qty_at_venc += ev["qty"]
                cost_open += ev["valor"]
            else:
                # FIFO partial cost out is approximated as proportional
                if qty_at_venc > 1e-9:
                    take = min(ev["qty"], qty_at_venc)
                    cost_out = (cost_open / qty_at_venc) * take
                    cost_open -= cost_out
                    qty_at_venc -= take
        if qty_at_venc < 1e-3:
            continue
        # Look up PU at vencimento (or last available before)
        pu_venc = get_pu_tesouro(tesouro_prices, sample["tipo"], venc, venc)
        if pu_venc is None:
            print(f"  WARN: sem PU para {sample['titulo']} no vencimento {venc};"
                  f" usando PU 1000 (face) como fallback")
            pu_venc = 1000.0
        valor_venc = qty_at_venc * pu_venc
        auto_resgates.append({
            "data": venc,
            "titulo": sample["titulo"],
            "codigo": sample["codigo"],
            "tipo": sample["tipo"],
            "vencimento": venc,
            "operacao": "SELL",
            "qty": qty_at_venc,
            "pu": pu_venc,
            "valor": valor_venc,
            "instituicao": sample.get("instituicao", ""),
            "_auto": True,
        })
        print(f"  AUTO-RESGATE: {sample['titulo']} qty={qty_at_venc:.2f} "
              f"@ {venc}  PU=R${pu_venc:,.2f}  valor=R${valor_venc:,.2f}")

    if auto_resgates:
        all_events = sorted(all_events + auto_resgates,
                            key=lambda t: (t["data"], t["operacao"]))

    # FIFO lots per (titulo, codigo): list[Lot]
    lots_by_key: dict[tuple[str, str], list[Lot]] = defaultdict(list)
    # Track aggregate qty (for sanity)
    qty_by_key: dict[tuple[str, str], float] = defaultdict(float)
    # Cashflows per month for downstream consumers
    cf_by_month: dict[str, dict] = defaultdict(
        lambda: {"buy": 0.0, "sell_liquido": 0.0, "sell_bruto": 0.0,
                 "ir_estimado": 0.0, "ganho_bruto_realizado": 0.0,
                 "ganho_liquido_realizado": 0.0})

    # Index events by date for replay
    events_by_date: dict[str, list[dict]] = defaultdict(list)
    for ev in all_events:
        events_by_date[ev["data"]].append(ev)

    # Apply each event in order; when SELL, FIFO consume lots
    for ev in all_events:
        key = (ev["titulo"], ev["codigo"])
        ym = ev["data"][:7]
        me_iso = month_end(int(ym[:4]), int(ym[5:7])).isoformat()
        if ev["operacao"] == "BUY":
            lots_by_key[key].append(Lot(ev))
            qty_by_key[key] += ev["qty"]
            cf_by_month[me_iso]["buy"] += ev["valor"]
        else:  # SELL
            qty_to_sell = ev["qty"]
            valor_recebido_liq = ev["valor"]
            valor_per_unit = ev["pu"]  # PU pago pela corretora
            sell_date = ev["data"]
            ganho_bruto_total = 0.0
            ganho_liq_total = 0.0
            ir_total = 0.0
            valor_bruto_total = 0.0  # gross-up reverso
            valor_recebido_acum = 0.0
            qty_remaining = qty_to_sell
            lots = lots_by_key[key]
            while qty_remaining > 1e-9 and lots:
                lot = lots[0]
                take = min(qty_remaining, lot.qty_remaining)
                lot_cost = take * lot.pu_buy
                lot_recv = take * valor_per_unit  # líquido por unidade já no PU
                # holding period
                d_buy = parse_iso(lot.date_buy)
                d_sell = parse_iso(sell_date)
                hold = (d_sell - d_buy).days
                aliq = ir_aliquota(hold)
                # gross-up: lot_recv = ganho_liq + lot_cost
                # ganho_liq = ganho_bruto * (1 - aliq)
                ganho_liq = lot_recv - lot_cost
                if ganho_liq > 0:
                    ganho_bruto = ganho_liq / (1 - aliq) if aliq < 0.99 else ganho_liq
                    ir_lot = ganho_bruto - ganho_liq
                else:
                    ganho_bruto = ganho_liq
                    ir_lot = 0.0
                lot_gross = lot_cost + ganho_bruto

                ganho_bruto_total += ganho_bruto
                ganho_liq_total += ganho_liq
                ir_total += ir_lot
                valor_bruto_total += lot_gross
                valor_recebido_acum += lot_recv

                lot.qty_remaining -= take
                qty_remaining -= take
                qty_by_key[key] -= take
                if lot.qty_remaining < 1e-9:
                    lots.pop(0)

            if qty_remaining > 1e-3:
                print(f"  WARN: SELL sem cobertura por {ev['titulo']} "
                      f"em {sell_date}: faltam {qty_remaining} unidades "
                      f"(extrato truncado?)")
                qty_by_key[key] -= qty_remaining

            cf_by_month[me_iso]["sell_liquido"] += valor_recebido_acum
            cf_by_month[me_iso]["sell_bruto"] += valor_bruto_total
            cf_by_month[me_iso]["ir_estimado"] += ir_total
            cf_by_month[me_iso]["ganho_bruto_realizado"] += ganho_bruto_total
            cf_by_month[me_iso]["ganho_liquido_realizado"] += ganho_liq_total

    # ---------- Monthly snapshots ----------
    print(f"\nReplay: {len(all_events)} events from {start} to {end}")

    # Build month-end snapshots: for each (titulo, codigo) with qty>0,
    # value at month_end_iso.
    today = date.today()
    snap_rows: list[dict] = []
    final_rows: list[dict] = []

    # We need a per-month, per-position tracker that walks through history.
    # Reset and re-replay, this time emitting snapshots.
    lots_by_key = defaultdict(list)
    qty_by_key = defaultdict(float)
    # Track "alive" (not matured & not fully sold)
    sold_history: list[dict] = []  # for diagnostics

    last_month_emitted: str = ""

    months = list(iter_months(start, today.isoformat()))
    for (y, m) in months:
        me = month_end(y, m)
        me_iso = me.isoformat()

        # Apply all events with date <= me_iso that haven't been applied yet
        # We'll just iterate events on-the-fly: simpler to re-collect by month.
        pass

    # Simpler approach: collect events by date, then walk day-by-day.
    events_iter = sorted(all_events, key=lambda t: t["data"])
    ev_idx = 0
    n_events = len(events_iter)

    for (y, m) in months:
        me = month_end(y, m)
        me_iso = me.isoformat()

        # Apply all events on or before me_iso
        while ev_idx < n_events and events_iter[ev_idx]["data"] <= me_iso:
            ev = events_iter[ev_idx]
            ev_idx += 1
            key = (ev["titulo"], ev["codigo"])
            if ev["operacao"] == "BUY":
                lots_by_key[key].append(Lot(ev))
                qty_by_key[key] += ev["qty"]
            else:
                qty_remaining = ev["qty"]
                lots = lots_by_key[key]
                while qty_remaining > 1e-9 and lots:
                    lot = lots[0]
                    take = min(qty_remaining, lot.qty_remaining)
                    lot.qty_remaining -= take
                    qty_remaining -= take
                    qty_by_key[key] -= take
                    if lot.qty_remaining < 1e-9:
                        lots.pop(0)
                if qty_remaining > 1e-3:
                    qty_by_key[key] -= qty_remaining

        # Now emit snapshot
        for key, lots in lots_by_key.items():
            qty_total = sum(l.qty_remaining for l in lots)
            if qty_total < 1e-9:
                continue
            sample_lot = lots[0]
            tipo = sample_lot.tipo
            venc = sample_lot.vencimento
            titulo = sample_lot.titulo
            codigo = sample_lot.codigo

            # Tesouro: use public PU
            valor_mtm = 0.0
            valor_bruto = 0.0
            pu_used = None
            method = ""
            if tipo.startswith("TESOURO"):
                # If matured by me_iso -> position should have been sold.
                if venc and parse_iso(venc) < me:
                    # Title matured but no SELL recorded — the corretora
                    # auto-resgatou; we don't have the proceeds in cashflow,
                    # so we keep the qty visible but warn once.
                    pu = get_pu_tesouro(tesouro_prices, tipo, venc, me_iso)
                    if pu is None:
                        # try last price before maturity
                        pu = get_pu_tesouro(tesouro_prices, tipo, venc, venc)
                    valor_mtm = qty_total * (pu or sample_lot.pu_buy)
                    valor_bruto = valor_mtm  # already gross at maturity
                    method = "MATURED_NO_SELL"
                else:
                    pu = get_pu_tesouro(tesouro_prices, tipo, venc, me_iso)
                    if pu is None:
                        # fallback: use buy PU
                        pu = sample_lot.pu_buy
                        method = "FALLBACK_BUY_PU"
                    else:
                        method = "TESOURO_PUBLIC_PU"
                    pu_used = pu
                    valor_mtm = qty_total * pu
                    valor_bruto = valor_mtm  # for Tesouro, MTM is already gross
            elif tipo == "CDB":
                # CDB: per-lot compound at args.cdb_pct * CDI from buy_date to me_iso
                vmtm = 0.0
                for l in lots:
                    if l.qty_remaining < 1e-9:
                        continue
                    f = cdi_compound_factor(cdi, l.date_buy, me_iso,
                                            pct=args.cdb_pct)
                    vmtm += l.qty_remaining * l.pu_buy * f
                valor_mtm = vmtm
                valor_bruto = vmtm  # CDB MTM is already gross (no IR until sell)
                method = f"CDB_CDI_{int(args.cdb_pct*100)}PCT"
            else:
                valor_mtm = qty_total * sample_lot.pu_buy
                valor_bruto = valor_mtm
                method = "FLAT"

            snap_rows.append({
                "month_end": me_iso,
                "titulo": titulo,
                "codigo": codigo,
                "tipo": tipo,
                "vencimento": venc or "",
                "qty": round(qty_total, 6),
                "valor_mtm_liquido": round(valor_mtm, 2),
                "valor_mtm_bruto": round(valor_bruto, 2),
                "method": method,
            })

    # Final snapshot = last month_end
    last_me = months[-1]
    last_me_iso = month_end(last_me[0], last_me[1]).isoformat()
    final_rows = [r for r in snap_rows if r["month_end"] == last_me_iso]

    # ---------- Write outputs ----------
    fields = ["month_end", "titulo", "codigo", "tipo", "vencimento",
              "qty", "valor_mtm_liquido", "valor_mtm_bruto", "method"]
    with open(args.monthly_out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in snap_rows:
            w.writerow(r)

    cf_fields = ["month_end", "buy", "sell_liquido", "sell_bruto",
                 "ir_estimado", "ganho_bruto_realizado",
                 "ganho_liquido_realizado"]
    with open(args.cashflow_out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cf_fields)
        w.writeheader()
        for me in sorted(cf_by_month):
            row = {"month_end": me, **{k: round(v, 2)
                                       for k, v in cf_by_month[me].items()}}
            w.writerow(row)

    with open(args.final_out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in final_rows:
            w.writerow(r)

    # ---------- Print summary ----------
    print(f"\nWrote {args.monthly_out} ({len(snap_rows)} snapshot rows)")
    print(f"Wrote {args.cashflow_out}")
    print(f"Wrote {args.final_out}")

    print(f"\n=== Posição final ({last_me_iso}) ===")
    total_liq = total_brt = 0.0
    for r in sorted(final_rows, key=lambda r: -r["valor_mtm_liquido"]):
        print(f"  {r['titulo']:<55s} qty={r['qty']:>8.2f}  "
              f"V_mtm_liq=R${r['valor_mtm_liquido']:>11,.2f}  "
              f"V_mtm_brt=R${r['valor_mtm_bruto']:>11,.2f}  "
              f"[{r['method']}]")
        total_liq += r["valor_mtm_liquido"]
        total_brt += r["valor_mtm_bruto"]
    print(f"\n  TOTAL V_RF (líquido):  R${total_liq:>14,.2f}")
    print(f"  TOTAL V_RF (bruto):    R${total_brt:>14,.2f}")

    print(f"\n=== Realizações (vendas/vencimentos) ===")
    tot_sell_liq = sum(c["sell_liquido"] for c in cf_by_month.values())
    tot_sell_brt = sum(c["sell_bruto"] for c in cf_by_month.values())
    tot_ir = sum(c["ir_estimado"] for c in cf_by_month.values())
    print(f"  Vendido líquido (entrou na conta): R${tot_sell_liq:>14,.2f}")
    print(f"  Vendido bruto  (gross-up estimado): R${tot_sell_brt:>14,.2f}")
    print(f"  IR estimado retido:                 R${tot_ir:>14,.2f}")

    print(f"\n=== Aplicado total ===")
    tot_buy = sum(c["buy"] for c in cf_by_month.values())
    print(f"  Buy total: R${tot_buy:>14,.2f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
