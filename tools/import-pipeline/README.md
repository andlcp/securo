# Import Pipeline — Histórico de Investimentos

Scripts standalone que reconstroem o histórico completo de uma carteira a
partir dos extratos públicos das corretoras e bolsas, calculam TWR
(Time-Weighted Return) e geram um arquivo `twr_full.csv` que pode ser
importado no Securo via UI (botão "Importar TWR (CSV)" na página
Investimentos).

São scripts **pessoais** — escritos para o caso de uso de **um único
investidor brasileiro** com histórico em B3 (Brasil) + IBKR (EUA). Não são
parte do app Securo: são tools que vivem ao lado.

---

## Visão geral do pipeline

```
                          ┌──── B3 Negociação .xlsx ────┐
                          ├──── B3 Movimentação .xlsx ──┤
                          ├──── B3 Renda Fixa .xlsx ────┤
   Extratos públicos ─────┤                              ├─── parse_*.py
                          └──── IBKR Activity .csv ─────┘            │
                                                                     ▼
                                                          (trades / proventos /
                                                           rf_trades / us_trades).csv
                                                                     │
   Yahoo Finance / BCB / Tesouro Transparente ─── fetch_*.py ────────┤
                                                                     ▼
                                                      (prices_cache / splits /
                                                       benchmarks_monthly /
                                                       tesouro_prices_cache /
                                                       us_prices_cache /
                                                       ptax_daily).csv
                                                                     │
                                                          replay_*.py
                                                                     │
                                                                     ▼
                                                      (holdings_monthly /
                                                       rf_holdings_monthly /
                                                       us_holdings_monthly).csv
                                                                     │
                                                          compute_twr_v2.py
                                                                     │
                                                                     ▼
                                                            twr_monthly.csv
                                                                     │
                                                          merge_twr_benchmarks.py
                                                                     │
                                                                     ▼
                                                              twr_full.csv  ← importado pelo Securo
                                                                     │
                                                          export_excel.py (opcional)
                                                                     │
                                                                     ▼
                                                  investimentos_consolidado.xlsx
```

---

## Como rodar (sequência típica)

Os scripts esperam ser executados a partir da **raiz do repositório**
(eles assumem CSVs no diretório atual). Os arquivos gerados ficam todos
na raiz e estão no `.gitignore` — nada vai pro repo público.

```bash
# 1. Renda Variável Brasil (B3 Negociação)
python tools/import-pipeline/parse_b3_negociacao.py ~/Downloads/negociacao-XXXX.xlsx
python tools/import-pipeline/fetch_splits.py
python tools/import-pipeline/replay_holdings.py
python tools/import-pipeline/fetch_prices.py

# 2. Proventos (B3 Movimentação RV)
python tools/import-pipeline/parse_b3_proventos.py ~/Downloads/movimentacao-XXXX.xlsx

# 3. Renda Fixa (B3 Movimentação RF)
python tools/import-pipeline/parse_b3_renda_fixa.py ~/Downloads/movimentacao-rf-XXXX.xlsx
python tools/import-pipeline/fetch_tesouro_prices.py
python tools/import-pipeline/replay_renda_fixa.py

# 4. Ações americanas (IBKR Activity Statement)
python tools/import-pipeline/parse_ibkr_activity.py ~/Downloads/UXXXXXXXX_*.csv
python tools/import-pipeline/fetch_ptax.py --start 2025-01-01
python tools/import-pipeline/fetch_us_prices.py
python tools/import-pipeline/replay_us_holdings.py

# 5. Benchmarks + TWR consolidado
python tools/import-pipeline/fetch_benchmarks.py --start 2019-06
python tools/import-pipeline/compute_twr_v2.py
python tools/import-pipeline/merge_twr_benchmarks.py     # gera twr_full.csv

# 6. (opcional) Planilha consolidada
python tools/import-pipeline/export_excel.py
```

Ao final, importa `twr_full.csv` no Securo: **Investimentos → Importar TWR
(CSV)**.

---

## Scripts

### Parsers (transformam extratos em CSV limpo)

| Script | Entrada | Saída |
|---|---|---|
| `parse_b3_negociacao.py` | xlsx Negociação B3 | `trades.csv` |
| `parse_b3_proventos.py` | xlsx Movimentação B3 (proventos RV) | `proventos.csv` |
| `parse_b3_renda_fixa.py` | xlsx Movimentação B3 (RF) | `rf_trades.csv` |
| `parse_ibkr_activity.py` | CSV Activity Statement IBKR | `us_trades.csv`, `us_dividends.csv`, `us_withholding.csv`, `us_deposits.csv`, `us_positions_final.csv` |

### Fetchers (baixam dados públicos)

| Script | Fonte | Saída |
|---|---|---|
| `fetch_prices.py` | Yahoo Finance (.SA) | `prices_cache.csv` |
| `fetch_splits.py` | Yahoo (events=split) | `splits.csv` |
| `fetch_benchmarks.py` | Yahoo + BCB (CDI) | `benchmarks_monthly.csv` |
| `fetch_tesouro_prices.py` | Tesouro Transparente | `tesouro_prices_cache.csv` |
| `fetch_us_prices.py` | Yahoo Finance (US) | `us_prices_cache.csv` |
| `fetch_ptax.py` | BCB SGS séries 1 e 10813 | `ptax_daily.csv` |

### Replayers (reconstroem posições mês a mês)

| Script | Saída |
|---|---|
| `replay_holdings.py` | `holdings_monthly.csv`, `holdings_final.csv` |
| `replay_renda_fixa.py` | `rf_holdings_monthly.csv`, `rf_cashflow_monthly.csv`, `rf_final.csv` |
| `replay_us_holdings.py` | `us_holdings_monthly.csv`, `us_summary_monthly.csv`, `us_final.csv` |

### TWR + export

| Script | Função |
|---|---|
| `compute_twr_v2.py` | Modified Dietz mensal, TWR bruto + líquido |
| `merge_twr_benchmarks.py` | Junta TWR + benchmarks → `twr_full.csv` |
| `export_excel.py` | Gera `investimentos_consolidado.xlsx` (17 abas) |

### Utilidades

| Script | Função |
|---|---|
| `import_positions.py` | Empurra posições atuais pra Securo via API (CRUD de Asset) |
| `reset_investments.py` | Limpa todos os assets do tipo investimento (helper) |

### Legacy (`legacy/`)

Versões antigas / one-offs. Mantidos para referência, não são parte do
fluxo atual:
- `parse_b3_excel.py`, `parse_b3_history.py` — parsers PDF antigos
- `parse_xp_history.py`, `parse_xp_positions.py` — parsers da XP
- `compute_twr.py` — versão v1 (apenas RV, sem proventos)
- `net_positions.py` — cálculo antigo de posições
- `import_anderson_xp.py` — script one-off de importação inicial

---

## Dados sensíveis

Os CSVs e XLSX gerados (`trades.csv`, `holdings_*.csv`, `proventos.csv`,
`investimentos_consolidado.xlsx`, etc.) contêm **valores investidos,
posições, P&L** — dados financeiros pessoais.

**Estão todos no `.gitignore`.** Mantém assim — esse repo é público.

---

## Arquivos de configuração manual (que você cria à mão)

Alguns CSVs são preenchidos por você, não gerados automaticamente:

- `target_position.csv` — posição atual oficial (vinda do app da corretora),
  usada para validar o resultado do replay
- `ticker_aliases.csv` — renomeações de ações (TRPL4→ISAE4, MCHF11+MCHY11→MCRE11, etc.)
- `tesouro_initial_positions.csv` — seed de posições anteriores ao
  período do extrato da B3 (caso você já tinha Tesouro antes de dez/2019)

Esses também estão no `.gitignore`.
