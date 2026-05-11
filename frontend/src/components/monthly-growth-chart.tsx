/**
 * Crescimento mensal do patrimônio
 *
 * Stacked vertical bar chart showing per-month net change in portfolio value,
 * split into two stacks:
 *   - Aportes: signed cashflow of the month (money you added or removed).
 *   - Ganho de capital: gain that came from price movement / interest, i.e.
 *     (V_end − V_start − cashflow).
 *
 * The two together equal the period-over-period change in V_end. Negative
 * months render below the zero line. We default to the last 24 months so
 * recent activity is dense enough to be readable but the chart still shows
 * historical context.
 */
import { useMemo } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { PortfolioPoint } from '@/lib/api'

interface MonthlyGrowthChartProps {
  /** Monthly portfolio series (since user's first transaction). */
  monthlyData: PortfolioPoint[] | undefined
  /** Currency code for tooltip formatting. */
  currency: string
  /** Locale for number formatting. */
  locale: string
  /** Privacy mask: returns "***" or the formatted value. */
  mask: (s: string) => string
  /** Max number of trailing months to render (default 24). */
  windowMonths?: number
}

interface MonthlyGrowth {
  monthKey: string         // YYYY-MM
  monthLabel: string       // "Jan/26" or similar
  aportes: number          // signed cashflow
  ganho: number            // V_end − V_prev_end − cashflow
  total: number            // aportes + ganho
}

function formatMoney(v: number, currency: string, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: 'currency',
    currency,
    maximumFractionDigits: 0,
  }).format(v)
}

function formatMonthLabel(monthEnd: string, locale: string): string {
  // monthEnd is ISO YYYY-MM-DD (last day of month)
  const d = new Date(monthEnd + 'T00:00:00')
  const fmt = new Intl.DateTimeFormat(locale, { month: 'short', year: '2-digit' })
  return fmt.format(d).replace('.', '')
}

export function MonthlyGrowthChart({
  monthlyData,
  currency,
  locale,
  mask,
  windowMonths = 24,
}: MonthlyGrowthChartProps) {
  const rows = useMemo<MonthlyGrowth[]>(() => {
    if (!monthlyData || monthlyData.length === 0) return []
    // monthlyData is already sorted by month_end ascending. Compute period-
    // over-period changes; the first point has no predecessor so its "ganho"
    // is treated as 0 (we can't separate seed capital from organic gain).
    const out: MonthlyGrowth[] = []
    let prevVEnd: number | null = null
    for (const p of monthlyData) {
      const aportes = p.cashflow ?? 0
      const vEnd = p.v_end ?? 0
      const ganho = prevVEnd == null ? 0 : (vEnd - prevVEnd - aportes)
      out.push({
        monthKey: p.month,
        monthLabel: formatMonthLabel(p.month_end, locale),
        aportes,
        ganho,
        total: aportes + ganho,
      })
      prevVEnd = vEnd
    }
    // Tail-trim to the requested window.
    return out.slice(-windowMonths)
  }, [monthlyData, locale, windowMonths])

  if (!monthlyData) {
    return <div className="h-72 animate-pulse bg-muted/30 rounded-lg" />
  }
  if (rows.length === 0) {
    return (
      <p className="text-muted-foreground text-sm text-center py-12">
        Sem dados suficientes para o gráfico.
      </p>
    )
  }

  return (
    <div style={{ height: 320 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart
          data={rows}
          margin={{ top: 16, right: 20, left: 8, bottom: 8 }}
          stackOffset="sign"
        >
          <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" vertical={false} />
          <XAxis
            dataKey="monthLabel"
            tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
            axisLine={false}
            tickLine={false}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
            axisLine={false}
            tickLine={false}
            tickFormatter={(v) => {
              const n = v as number
              const abs = Math.abs(n)
              if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
              if (abs >= 1_000) return `${(n / 1_000).toFixed(0)}k`
              return `${n}`
            }}
            width={50}
          />
          <ReferenceLine y={0} stroke="var(--border)" strokeWidth={1} />
          <Tooltip
            cursor={{ fill: 'transparent' }}
            contentStyle={{
              background: 'var(--card)',
              border: '1px solid var(--border)',
              borderRadius: 8,
              fontSize: 12,
            }}
            formatter={(value: unknown, key: unknown) => {
              const v = value as number
              const labelMap: Record<string, string> = {
                aportes: 'Aportes',
                ganho: 'Ganho de capital',
              }
              return [mask(formatMoney(v, currency, locale)), labelMap[key as string] ?? key]
            }}
            labelFormatter={(label, payload) => {
              // Show total at the top of the tooltip when both stacks present.
              const arr = (payload ?? []) as ReadonlyArray<{ payload?: MonthlyGrowth }>
              const total = arr[0]?.payload?.total ?? 0
              return `${label} · Total ${mask(formatMoney(total, currency, locale))}`
            }}
          />
          <Bar dataKey="aportes" stackId="growth" name="aportes" radius={[0, 0, 0, 0]} maxBarSize={28}>
            {rows.map((r) => (
              <Cell
                key={`aporte-${r.monthKey}`}
                fill={r.aportes >= 0 ? '#22c55e' : '#fda4af'}
              />
            ))}
          </Bar>
          <Bar dataKey="ganho" stackId="growth" name="ganho" radius={[3, 3, 0, 0]} maxBarSize={28}>
            {rows.map((r) => (
              <Cell
                key={`ganho-${r.monthKey}`}
                fill={r.ganho >= 0 ? '#0ea5e9' : '#fb7185'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      {/* Legend (manual — Recharts default legend renders by stackId not Cell colors) */}
      <div className="flex items-center justify-center gap-5 mt-2 text-xs text-muted-foreground">
        <div className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm" style={{ background: '#22c55e' }} />
          Aportes
        </div>
        <div className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm" style={{ background: '#0ea5e9' }} />
          Ganho de capital
        </div>
      </div>
    </div>
  )
}
