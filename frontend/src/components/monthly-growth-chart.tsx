/**
 * Crescimento mensal do patrimônio
 *
 * Vertical bar chart showing per-month net change in portfolio value.
 * Each bar is V_end − V_prev_end (the bottom-line delta), colored green
 * when positive and rose when negative. The tooltip decomposes the delta
 * into Aportes (cashflow) and Ganho de capital (V_end − V_prev − cashflow).
 *
 * The "ganho de capital" line here is money-on-money: V_end minus capital
 * invested. It differs from the % rentabilidade in the ResultadoTable
 * because that one uses TWR chained from daily returns (which weights by
 * time inside the month), while this one is the raw monetary delta. They
 * agree in sign over long horizons but can disagree in a single month
 * when cashflows arrive mid-period and the market moves around them.
 *
 * Defaults to the last 24 months — dense enough to read trends, short
 * enough to keep each bar legible.
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
  monthlyData: PortfolioPoint[] | undefined
  currency: string
  locale: string
  mask: (s: string) => string
  windowMonths?: number
}

interface MonthlyGrowth {
  monthKey: string
  monthLabel: string
  aportes: number
  ganho: number
  net: number
}

function formatMoney(v: number, currency: string, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: 'currency',
    currency,
    maximumFractionDigits: 0,
  }).format(v)
}

function formatMonthLabel(monthEnd: string, locale: string): string {
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
        net: aportes + ganho,
      })
      prevVEnd = vEnd
    }
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
            cursor={{ fill: 'var(--muted)', opacity: 0.3 }}
            contentStyle={{
              background: 'var(--card)',
              border: '1px solid var(--border)',
              borderRadius: 8,
              fontSize: 12,
              padding: 10,
            }}
            content={(props) => {
              const payload = (props.payload ?? []) as Array<{ payload?: MonthlyGrowth }>
              const r = payload[0]?.payload
              if (!r) return null
              return (
                <div style={{
                  background: 'var(--card)',
                  border: '1px solid var(--border)',
                  borderRadius: 8,
                  padding: 10,
                  fontSize: 12,
                  minWidth: 200,
                }}>
                  <div style={{ fontWeight: 600, marginBottom: 6 }}>
                    {r.monthLabel}
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                    <span style={{ color: 'var(--muted-foreground)' }}>Total do mês</span>
                    <span style={{ fontWeight: 600, color: r.net >= 0 ? '#16a34a' : '#dc2626' }}>
                      {(r.net >= 0 ? '+' : '') + mask(formatMoney(r.net, currency, locale))}
                    </span>
                  </div>
                  <div style={{ height: 1, background: 'var(--border)', margin: '6px 0' }} />
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, marginTop: 2 }}>
                    <span style={{ color: 'var(--muted-foreground)' }}>Aportes</span>
                    <span style={{ color: r.aportes >= 0 ? '#16a34a' : '#dc2626' }}>
                      {(r.aportes >= 0 ? '+' : '') + mask(formatMoney(r.aportes, currency, locale))}
                    </span>
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12 }}>
                    <span style={{ color: 'var(--muted-foreground)' }}>Ganho de capital</span>
                    <span style={{ color: r.ganho >= 0 ? '#16a34a' : '#dc2626' }}>
                      {(r.ganho >= 0 ? '+' : '') + mask(formatMoney(r.ganho, currency, locale))}
                    </span>
                  </div>
                </div>
              )
            }}
          />
          <Bar dataKey="net" name="net" radius={[3, 3, 0, 0]} maxBarSize={28}>
            {rows.map((r) => (
              <Cell
                key={r.monthKey}
                fill={r.net >= 0 ? '#16a34a' : '#dc2626'}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
      <div className="flex items-center justify-center gap-5 mt-2 text-xs text-muted-foreground">
        <div className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm" style={{ background: '#16a34a' }} />
          Mês positivo
        </div>
        <div className="flex items-center gap-1.5">
          <span className="w-3 h-3 rounded-sm" style={{ background: '#dc2626' }} />
          Mês negativo
        </div>
      </div>
    </div>
  )
}
