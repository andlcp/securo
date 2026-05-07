/**
 * Resultado financeiro e rentabilidade
 *
 * Performance table modeled on Gorila Invest's "Resultado" widget — also the
 * standard format Brazilian funds use to publish monthly/annual returns
 * alongside CDI. Two views:
 *
 *   Rentabilidade (%): per-month TWR, with a CDI row underneath each year.
 *   Retorno (R$):       per-month money gain (V_end − V_start − cashflow).
 *
 * Per-period aggregation across the top:
 *   Período / Mês / Ano / 12 meses / 24 meses / Desde o início
 *
 * Source data is daily timeseries (since start) so we don't depend on the
 * monthly endpoint's implicit-cashflow heuristic — we derive monthly
 * boundaries from daily v_end / twr_cum / cashflow ourselves.
 */
import { useMemo, useState } from 'react'
import { ChevronDown } from 'lucide-react'
import type { PortfolioPoint } from '@/lib/api'

interface BenchmarkPoint {
  date: string
  value: number  // cumulative % from start of dataset
}

interface ResultadoTableProps {
  /** Daily portfolio series since the user's first transaction. */
  lifetimeDaily: PortfolioPoint[] | undefined
  /** CDI cumulative-% series (daily granularity, since start). */
  cdiSeries: BenchmarkPoint[] | undefined
  /** Locale for number formatting. */
  locale: string
  /** Privacy mask (returns "***" or the value). */
  mask: (s: string) => string
  /**
   * Optional label/range describing the user's currently-selected window.
   * If set, the "Período" column aggregates over this range; otherwise it
   * mirrors "Mês".
   */
  periodLabel?: string
  periodFrom?: string  // YYYY-MM-DD
  periodTo?: string    // YYYY-MM-DD
}

interface MonthlyStat {
  yyyy: number
  mm: number  // 1..12
  monthKey: string         // "2026-04"
  monthLastDate: string    // ISO of last day of month with data
  twr_pct: number          // monthly TWR %
  gain: number             // money gain in month (BRL)
  v_end: number            // portfolio value at month end
  cdi_pct: number | null   // CDI monthly return %
}

const MONTH_NAMES = [
  'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun',
  'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez',
]

function fmtPct(v: number | null | undefined, opts?: { signed?: boolean }) {
  if (v == null || isNaN(v)) return '—'
  const s = `${v.toFixed(2)}%`
  return opts?.signed && v >= 0 ? `+${s}` : v < 0 ? s : s
}

function fmtCurrency(v: number, locale: string, currency = 'BRL') {
  try {
    return new Intl.NumberFormat(locale, {
      style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 2,
    }).format(v)
  } catch {
    return v.toFixed(2)
  }
}

/**
 * Aggregate daily portfolio + CDI series into per-month statistics.
 *
 * For each calendar month present in the daily data, we keep the LAST day
 * with data and treat it as the month-end snapshot. Monthly TWR is the
 * chain link between consecutive month-ends; monthly gain is the change
 * in portfolio value net of cashflow. The first month is anchored against
 * the very first daily point (where twr_cum is typically 0).
 */
function computeMonthlyStats(
  daily: PortfolioPoint[],
  cdi: BenchmarkPoint[],
): MonthlyStat[] {
  if (!daily || daily.length === 0) return []

  // Bucket portfolio days by YYYY-MM and keep the last entry of each month.
  const byMonth = new Map<string, PortfolioPoint>()
  const cashflowByMonth = new Map<string, number>()
  for (const p of daily) {
    const key = p.month_end.slice(0, 7)  // "YYYY-MM"
    // Last-write wins (input is chronological): we want the LAST day of the
    // month with data, which gives the closing v_end and twr_cum.
    byMonth.set(key, p)
    cashflowByMonth.set(key, (cashflowByMonth.get(key) ?? 0) + (p.cashflow ?? 0))
  }
  const months = Array.from(byMonth.keys()).sort()

  // Anchor for monthly TWR chaining. Before the first month we use twr_cum=0
  // and v_end=0 — same convention the daily walk uses on day 0.
  let prevTwrCum = 0
  let prevVEnd = 0

  // Index CDI by date for as-of lookups (cumulative %, monthly increments
  // come from chaining the cumulative values at month boundaries).
  const cdiByDate = new Map<string, number>()
  for (const p of cdi) cdiByDate.set(p.date, p.value)
  const cdiDatesSorted = cdi.map(p => p.date).sort()
  const cdiAsOf = (target: string): number | null => {
    // Last entry with date <= target. CDI series is daily/business days,
    // and the target is always our portfolio's last-day-of-month — a
    // weekend or holiday in 30-40% of cases. Walking back picks up the
    // nearest preceding business day.
    let last: number | null = null
    for (const d of cdiDatesSorted) {
      if (d > target) break
      last = cdiByDate.get(d) ?? last
    }
    return last
  }

  // CDI baseline at the start of the dataset (the first month's "previous"
  // cdi_cum). We use the first daily point's date to seed it.
  let prevCdiCum: number | null = cdiAsOf(daily[0].month_end) ?? 0

  const out: MonthlyStat[] = []
  for (const monthKey of months) {
    const p = byMonth.get(monthKey)!
    const cf = cashflowByMonth.get(monthKey) ?? 0
    const r_m = (1 + p.twr_cum) / (1 + prevTwrCum) - 1
    const gain = p.v_end - prevVEnd - cf

    const cdiCumNow = cdiAsOf(p.month_end)
    let cdi_pct: number | null = null
    if (cdiCumNow != null && prevCdiCum != null) {
      cdi_pct = ((1 + cdiCumNow / 100) / (1 + prevCdiCum / 100) - 1) * 100
    }

    const [yStr, mStr] = monthKey.split('-')
    out.push({
      yyyy: parseInt(yStr, 10),
      mm: parseInt(mStr, 10),
      monthKey,
      monthLastDate: p.month_end,
      twr_pct: r_m * 100,
      gain,
      v_end: p.v_end,
      cdi_pct,
    })
    prevTwrCum = p.twr_cum
    prevVEnd = p.v_end
    if (cdiCumNow != null) prevCdiCum = cdiCumNow
  }
  return out
}

/** Chain monthly TWRs: (1+r1)*(1+r2)*...*(1+rn) - 1. Returns percent. */
function chainTwr(stats: MonthlyStat[], pick: (s: MonthlyStat) => number | null): number | null {
  if (stats.length === 0) return null
  let f = 1
  let any = false
  for (const s of stats) {
    const r = pick(s)
    if (r == null) continue
    f *= 1 + r / 100
    any = true
  }
  return any ? (f - 1) * 100 : null
}

function sumGain(stats: MonthlyStat[]): number {
  return stats.reduce((acc, s) => acc + s.gain, 0)
}

export function ResultadoTable({
  lifetimeDaily,
  cdiSeries,
  locale,
  mask,
  periodLabel,
  periodFrom,
  periodTo,
}: ResultadoTableProps) {
  const [view, setView] = useState<'pct' | 'brl'>('pct')
  const [showAllYears, setShowAllYears] = useState(false)

  const stats = useMemo(
    () => computeMonthlyStats(lifetimeDaily ?? [], cdiSeries ?? []),
    [lifetimeDaily, cdiSeries],
  )

  // Aggregate buckets — both for the % view (chained TWR) and the R$ view
  // (sum of monthly gains).
  const aggregates = useMemo(() => {
    if (stats.length === 0) return null
    const last = stats[stats.length - 1]
    const lastYear = last.yyyy

    const inYTD = stats.filter(s => s.yyyy === lastYear)
    const last12 = stats.slice(-12)
    const last24 = stats.slice(-24)

    const monthOnly = stats.slice(-1)  // single month
    const periodScope = (() => {
      if (!periodFrom || !periodTo) return monthOnly
      // Bracket-inclusive: include any month whose monthLastDate falls
      // within [periodFrom, periodTo]. The from cutoff uses month-key
      // comparison so picking the 15th of a month still includes that
      // month's stats (we don't have intra-month TWR here).
      const fromKey = periodFrom.slice(0, 7)
      const toKey = periodTo.slice(0, 7)
      return stats.filter(s => s.monthKey >= fromKey && s.monthKey <= toKey)
    })()

    return {
      periodo: {
        twr: chainTwr(periodScope, s => s.twr_pct),
        cdi: chainTwr(periodScope, s => s.cdi_pct),
        gain: sumGain(periodScope),
      },
      mes: {
        twr: chainTwr(monthOnly, s => s.twr_pct),
        cdi: chainTwr(monthOnly, s => s.cdi_pct),
        gain: sumGain(monthOnly),
      },
      ano: {
        twr: chainTwr(inYTD, s => s.twr_pct),
        cdi: chainTwr(inYTD, s => s.cdi_pct),
        gain: sumGain(inYTD),
      },
      m12: {
        twr: chainTwr(last12, s => s.twr_pct),
        cdi: chainTwr(last12, s => s.cdi_pct),
        gain: sumGain(last12),
      },
      m24: {
        twr: chainTwr(last24, s => s.twr_pct),
        cdi: chainTwr(last24, s => s.cdi_pct),
        gain: sumGain(last24),
      },
      desde: {
        twr: chainTwr(stats, s => s.twr_pct),
        cdi: chainTwr(stats, s => s.cdi_pct),
        gain: sumGain(stats),
      },
    }
  }, [stats, periodFrom, periodTo])

  // Group stats by year for the monthly grid.
  const byYear = useMemo(() => {
    const map = new Map<number, MonthlyStat[]>()
    for (const s of stats) {
      if (!map.has(s.yyyy)) map.set(s.yyyy, [])
      map.get(s.yyyy)!.push(s)
    }
    // Sort years descending so most recent is on top.
    return Array.from(map.entries()).sort((a, b) => b[0] - a[0])
  }, [stats])

  const yearsToShow = showAllYears ? byYear : byYear.slice(0, 3)
  const hasMore = byYear.length > 3

  if (stats.length === 0 || !aggregates) {
    return (
      <div className="bg-card rounded-xl border border-border shadow-sm p-5 text-sm text-muted-foreground">
        Sem dados suficientes para a tabela de resultados.
      </div>
    )
  }

  // Color helper for cells: green for positive, rose for negative, neutral for zero.
  const cellColor = (v: number | null | undefined) =>
    v == null || isNaN(v) ? 'text-muted-foreground'
      : v > 0 ? 'text-emerald-600'
        : v < 0 ? 'text-rose-500'
          : 'text-foreground'

  const periodColLabel = periodLabel ?? 'Período'

  // Cell renderer used by both views to keep formatting consistent.
  const renderCell = (v: number | null | undefined, mode: 'pct' | 'brl', kind: 'cdi' | 'pf') => {
    if (v == null || isNaN(v)) return <span className="text-muted-foreground/50">—</span>
    if (mode === 'pct') {
      return (
        <span className={cellColor(v)}>{fmtPct(v)}</span>
      )
    }
    // BRL view — CDI row never shows here (filtered by caller); only portfolio.
    return (
      <span className={cellColor(v)}>
        {kind === 'pf' ? mask(fmtCurrency(v, locale)) : '—'}
      </span>
    )
  }

  return (
    <div className="bg-card rounded-xl border border-border shadow-sm">
      {/* Header — title + view toggle */}
      <div className="flex items-center justify-between px-5 py-4 border-b border-border">
        <p className="text-sm font-semibold text-foreground">
          Resultado financeiro e rentabilidade
        </p>
        <div className="flex items-center rounded-lg border border-border bg-muted/30 overflow-hidden text-xs font-semibold">
          <button
            type="button"
            onClick={() => setView('pct')}
            className={`px-3 py-1.5 transition-colors ${
              view === 'pct'
                ? 'bg-foreground text-background'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            Rentabilidade
          </button>
          <button
            type="button"
            onClick={() => setView('brl')}
            className={`px-3 py-1.5 transition-colors ${
              view === 'brl'
                ? 'bg-foreground text-background'
                : 'text-muted-foreground hover:text-foreground'
            }`}
          >
            Retorno (R$)
          </button>
        </div>
      </div>

      {/* Top summary — Período / Mês / Ano / 12m / 24m / Desde início */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-[11px] text-muted-foreground bg-muted/20">
              <th className="text-left font-medium px-5 py-2.5 w-28">&nbsp;</th>
              <th className="text-right font-medium px-3 py-2.5">{periodColLabel}</th>
              <th className="text-right font-medium px-3 py-2.5">Mês</th>
              <th className="text-right font-medium px-3 py-2.5">Ano</th>
              <th className="text-right font-medium px-3 py-2.5">12 meses</th>
              <th className="text-right font-medium px-3 py-2.5">24 meses</th>
              <th className="text-right font-medium px-5 py-2.5">Desde o início</th>
            </tr>
          </thead>
          <tbody>
            <tr className="border-t border-border">
              <td className="text-left font-bold px-5 py-2.5 text-foreground">Portfólio</td>
              <td className="text-right tabular-nums px-3 py-2.5">
                {view === 'pct'
                  ? renderCell(aggregates.periodo.twr, 'pct', 'pf')
                  : renderCell(aggregates.periodo.gain, 'brl', 'pf')}
              </td>
              <td className="text-right tabular-nums px-3 py-2.5">
                {view === 'pct'
                  ? renderCell(aggregates.mes.twr, 'pct', 'pf')
                  : renderCell(aggregates.mes.gain, 'brl', 'pf')}
              </td>
              <td className="text-right tabular-nums px-3 py-2.5">
                {view === 'pct'
                  ? renderCell(aggregates.ano.twr, 'pct', 'pf')
                  : renderCell(aggregates.ano.gain, 'brl', 'pf')}
              </td>
              <td className="text-right tabular-nums px-3 py-2.5">
                {view === 'pct'
                  ? renderCell(aggregates.m12.twr, 'pct', 'pf')
                  : renderCell(aggregates.m12.gain, 'brl', 'pf')}
              </td>
              <td className="text-right tabular-nums px-3 py-2.5">
                {view === 'pct'
                  ? renderCell(aggregates.m24.twr, 'pct', 'pf')
                  : renderCell(aggregates.m24.gain, 'brl', 'pf')}
              </td>
              <td className="text-right tabular-nums px-5 py-2.5">
                {view === 'pct'
                  ? renderCell(aggregates.desde.twr, 'pct', 'pf')
                  : renderCell(aggregates.desde.gain, 'brl', 'pf')}
              </td>
            </tr>
            {/* CDI row only makes sense in % view — CDI is a rate, not a R$ amount. */}
            {view === 'pct' && (
              <tr className="border-t border-border bg-muted/10">
                <td className="text-left font-medium px-5 py-2.5 text-muted-foreground">CDI</td>
                <td className="text-right tabular-nums px-3 py-2.5">
                  {renderCell(aggregates.periodo.cdi, 'pct', 'cdi')}
                </td>
                <td className="text-right tabular-nums px-3 py-2.5">
                  {renderCell(aggregates.mes.cdi, 'pct', 'cdi')}
                </td>
                <td className="text-right tabular-nums px-3 py-2.5">
                  {renderCell(aggregates.ano.cdi, 'pct', 'cdi')}
                </td>
                <td className="text-right tabular-nums px-3 py-2.5">
                  {renderCell(aggregates.m12.cdi, 'pct', 'cdi')}
                </td>
                <td className="text-right tabular-nums px-3 py-2.5">
                  {renderCell(aggregates.m24.cdi, 'pct', 'cdi')}
                </td>
                <td className="text-right tabular-nums px-5 py-2.5">
                  {renderCell(aggregates.desde.cdi, 'pct', 'cdi')}
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {/* Monthly grid */}
      <div className="overflow-x-auto border-t border-border">
        <table className="w-full text-xs">
          <thead>
            <tr className="text-[11px] text-muted-foreground bg-muted/20">
              <th className="text-left font-medium px-5 py-2.5 w-16">&nbsp;</th>
              {MONTH_NAMES.map(m => (
                <th key={m} className="text-right font-medium px-2 py-2.5">{m}</th>
              ))}
              <th className="text-right font-medium px-3 py-2.5">Total</th>
            </tr>
          </thead>
          <tbody>
            {yearsToShow.map(([year, list]) => {
              // Build a 12-slot array indexed by month (1..12).
              const monthMap = new Map<number, MonthlyStat>(list.map(s => [s.mm, s]))
              const yearTotalTwr = chainTwr(list, s => s.twr_pct)
              const yearTotalCdi = chainTwr(list, s => s.cdi_pct)
              const yearTotalGain = sumGain(list)

              return (
                <>
                  {/* Year row — portfolio TWR or gain */}
                  <tr key={`y-${year}`} className="border-t border-border">
                    <td className="text-left font-bold px-5 py-2.5 text-foreground">{year}</td>
                    {Array.from({ length: 12 }, (_, i) => i + 1).map(mm => {
                      const s = monthMap.get(mm)
                      return (
                        <td key={mm} className="text-right tabular-nums px-2 py-2.5">
                          {view === 'pct'
                            ? renderCell(s?.twr_pct, 'pct', 'pf')
                            : renderCell(s?.gain, 'brl', 'pf')}
                        </td>
                      )
                    })}
                    <td className="text-right tabular-nums font-bold px-3 py-2.5">
                      {view === 'pct'
                        ? renderCell(yearTotalTwr, 'pct', 'pf')
                        : renderCell(yearTotalGain, 'brl', 'pf')}
                    </td>
                  </tr>
                  {/* CDI row — only in % view */}
                  {view === 'pct' && (
                    <tr key={`c-${year}`} className="bg-muted/5">
                      <td className="text-left font-medium px-5 py-2.5 text-muted-foreground">CDI</td>
                      {Array.from({ length: 12 }, (_, i) => i + 1).map(mm => {
                        const s = monthMap.get(mm)
                        return (
                          <td key={mm} className="text-right tabular-nums px-2 py-2.5">
                            {renderCell(s?.cdi_pct, 'pct', 'cdi')}
                          </td>
                        )
                      })}
                      <td className="text-right tabular-nums font-bold px-3 py-2.5">
                        {renderCell(yearTotalCdi, 'pct', 'cdi')}
                      </td>
                    </tr>
                  )}
                </>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Show-more toggle */}
      {hasMore && (
        <div className="border-t border-border px-5 py-3 text-center">
          <button
            type="button"
            onClick={() => setShowAllYears(v => !v)}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-primary hover:underline"
          >
            {showAllYears ? 'Mostrar menos' : 'Ver tudo'}
            <ChevronDown
              size={12}
              className={`transition-transform ${showAllYears ? 'rotate-180' : ''}`}
            />
          </button>
        </div>
      )}
    </div>
  )
}
