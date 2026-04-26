"""
Script de importação - Carteira Anderson (XP)
Cria um grupo de ativos e importa todas as posições via API do Securo.

Uso:
    python import_anderson_xp.py --url http://46.225.24.167 --email SEU@EMAIL --password SUASENHA
"""

import argparse
import sys
import time
import requests

# ─── Dados extraídos do relatório XP ──────────────────────────────────────────

ACOES_BR = [
    # (ticker_yahoo, nome_display, qtd, preco_medio)
    ("IVVB11.SA", "iShares S&P 500 (IVVB11)",           491,   301.88),
    ("JHSF3.SA",  "JHSF Participações (JHSF3)",        1483,     4.04),
    ("RECV3.SA",  "PetroRecôncavo (RECV3)",             1510,    11.98),
    ("ASAI3.SA",  "Assaí Atacadista (ASAI3)",           2080,     8.57),
    ("GRND3.SA",  "Grendene (GRND3)",                   3600,     4.85),
    ("MYPK3.SA",  "Iochpe-Maxion (MYPK3)",              1490,    11.33),
    ("VALE3.SA",  "Vale (VALE3)",                        145,    63.66),
    ("PETR4.SA",  "Petrobras PN (PETR4)",                260,    36.52),
    ("ARML3.SA",  "Armac Locação (ARML3)",              2000,     3.67),
    ("SBSP3.SA",  "Sabesp (SBSP3)",                      64,   116.64),
    ("ISAE4.SA",  "Equatorial Energia PN (ISAE4)",       334,    25.90),
    ("LEVE3.SA",  "Metal Leve (LEVE3)",                  277,    33.85),
    ("TTEN3.SA",  "3tentos (TTEN3)",                     600,    13.30),
    ("PLPL3.SA",  "Plano & Plano (PLPL3)",               706,    11.66),
    ("POSI3.SA",  "Positivo Tecnologia (POSI3)",        1843,     5.69),
    ("BLAU3.SA",  "Blau Farmacêutica (BLAU3)",           780,     9.31),
    ("VAMO3.SA",  "Vamos Locação (VAMO3)",              1800,     3.84),
    ("KEPL3.SA",  "Kepler Weber (KEPL3)",               1000,     9.56),
    ("CMIN3.SA",  "CSN Mineração (CMIN3)",              1590,     5.44),
    ("VLID3.SA",  "Valid Soluções (VLID3)",              375,    17.90),
    ("TGMA3.SA",  "Tegma Gestão Logística (TGMA3)",      200,    35.60),
]

FIIS = [
    # (ticker_yahoo, nome_display, qtd, preco_medio)
    ("HGRU11.SA", "CSHG Renda Urbana (HGRU11)",          14,   129.94),
    ("KNRI11.SA", "Kinea Renda Imobiliária (KNRI11)",     11,   137.58),
    ("HGBS11.SA", "CSHG Brasil Shopping (HGBS11)",        88,    18.70),
    ("PVBI11.SA", "VBI Prime Properties (PVBI11)",        23,    72.27),
    ("KNIP11.SA", "Kinea Índices de Preços (KNIP11)",     19,    86.25),
    ("XPML11.SA", "XP Malls (XPML11)",                   16,   111.39),
    ("VISC11.SA", "Vinci Shopping Centers (VISC11)",      16,   117.88),
    ("HGLG11.SA", "CSHG Logística (HGLG11)",              11,   167.07),
    ("XPLG11.SA", "XP Log (XPLG11)",                      17,    98.00),
    ("KNCR11.SA", "Kinea Recebíveis Imobiliários (KNCR11)", 16, 102.56),
    ("MXRF11.SA", "Maxi Renda (MXRF11)",                 172,     9.43),
    ("BTLG11.SA", "BTG Pactual Logística (BTLG11)",       16,    98.33),
    ("RECR11.SA", "REC Recebíveis Imobiliários (RECR11)", 20,    80.86),
    ("KNHY11.SA", "Kinea High Yield CRI (KNHY11)",        16,   101.54),
    ("IRIM11.SA", "Iridium Recebíveis (IRIM11)",          23,    69.10),
]

# Fundos fechados (sem ticker no Yahoo — valuation manual)
FUNDOS = [
    # (nome, valor_aplicado, valor_atual)
    ("Vinci Capital Partners IV FIP - Trend PE XIX",   16632.17, 23063.49),
    ("Vinci Capital Partners IV FIP - Advisory",       14712.14, 15572.84),
    ("Trend Investback V - Inifinite",                   179.37,   183.12),
]

# ─── Funções ──────────────────────────────────────────────────────────────────

def login(base_url: str, email: str, password: str) -> str:
    r = requests.post(
        f"{base_url}/api/auth/login",
        data={"username": email, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        print("ERRO: login não retornou token.")
        sys.exit(1)
    print(f"✓ Login OK")
    return token


def create_group(base_url: str, headers: dict, name: str, color: str) -> str:
    r = requests.post(
        f"{base_url}/api/asset-groups",
        json={"name": name, "color": color, "icon": "wallet"},
        headers=headers,
        timeout=15,
    )
    r.raise_for_status()
    group_id = r.json()["id"]
    print(f"✓ Grupo criado: {name} ({group_id})")
    return group_id


def create_asset(base_url: str, headers: dict, payload: dict, label: str) -> bool:
    r = requests.post(
        f"{base_url}/api/assets",
        json=payload,
        headers=headers,
        timeout=20,
    )
    if r.status_code in (200, 201):
        print(f"  ✓ {label}")
        return True
    else:
        print(f"  ✗ {label} — {r.status_code}: {r.text[:120]}")
        return False


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Importa carteira Anderson XP para o Securo")
    parser.add_argument("--url",      default="http://46.225.24.167", help="URL do Securo")
    parser.add_argument("--email",    required=True)
    parser.add_argument("--password", required=True)
    args = parser.parse_args()

    base = args.url.rstrip("/")

    token = login(base, args.email, args.password)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Criar grupo "XP - Anderson"
    group_id = create_group(base, headers, "XP - Anderson", "#6366F1")

    ok = fail = 0

    # ── Ações BR + ETF BR ──────────────────────────────────────────────────────
    print(f"\n→ Importando {len(ACOES_BR)} ações/ETFs BR...")
    for ticker, nome, qtd, preco_medio in ACOES_BR:
        payload = {
            "name": nome,
            "type": "investment",
            "currency": "BRL",
            "valuation_method": "market_price",
            "ticker": ticker,
            "units": qtd,
            "purchase_price": preco_medio,
            "group_id": group_id,
        }
        if create_asset(base, headers, payload, f"{ticker} × {qtd} @ R${preco_medio}"):
            ok += 1
        else:
            fail += 1
        time.sleep(0.3)  # evita rate limit

    # ── FIIs ──────────────────────────────────────────────────────────────────
    print(f"\n→ Importando {len(FIIS)} FIIs...")
    for ticker, nome, qtd, preco_medio in FIIS:
        payload = {
            "name": nome,
            "type": "investment",
            "currency": "BRL",
            "valuation_method": "market_price",
            "ticker": ticker,
            "units": qtd,
            "purchase_price": preco_medio,
            "group_id": group_id,
        }
        if create_asset(base, headers, payload, f"{ticker} × {qtd} @ R${preco_medio}"):
            ok += 1
        else:
            fail += 1
        time.sleep(0.3)

    # ── Fundos fechados (manual) ───────────────────────────────────────────────
    print(f"\n→ Importando {len(FUNDOS)} fundos fechados (manual)...")
    for nome, valor_aplicado, valor_atual in FUNDOS:
        payload = {
            "name": nome,
            "type": "investment",
            "currency": "BRL",
            "valuation_method": "manual",
            "units": 1,
            "purchase_price": valor_aplicado,
            "current_value": valor_atual,
            "group_id": group_id,
        }
        if create_asset(base, headers, payload, f"{nome[:50]} — R${valor_atual}"):
            ok += 1
        else:
            fail += 1
        time.sleep(0.3)

    print(f"\n{'─'*50}")
    print(f"Concluído: {ok} criados, {fail} com erro")


if __name__ == "__main__":
    main()
