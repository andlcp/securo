import { useState, useMemo, useRef } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  Tooltip,
  ReferenceLine,
  CartesianGrid,
  ResponsiveContainer,
} from 'recharts'
import { investmentBenchmarks, assetGroups, portfolioSnapshots, portfolioTimeseries } from '@/lib/api'
import type { PortfolioSnapshot, PortfolioPoint } from '@/lib/api'
import { ASSET_CLASS_OPTIONS, type AssetClass, ASSET_CLASS_LABELS } from '@/types'
import { Skeleton } from '@/components/ui/skeleton'
import { PageHeader } from '@/components/page-header'
import { usePrivacyMode } from '@/hooks/use-privacy-mode'
import { useAuth } from '@/contexts/auth-context'
import type { AssetGroup } from '@/types'

interface BenchmarkPoint {
  date: string
  value: number
}

interface BenchmarkData {
  cdi: BenchmarkPoint[]
  ibov: BenchmarkPoint[]
  sp500: BenchmarkPoint[]
}

interface GroupReturn {
  id: string
  name: string
  invested: number
  current: number
  return_pct: number | null
}

interface ClassReturn {
  name: string
  invested: number
  current: number
  return_pct: number | null
}

interface PortfolioReturns {
  consolidated: { invested: number; current: number; return_pct: number | null }
  by_group: GroupReturn[]
  by_class: ClassReturn[]
}

const CDI_COLOR = '#F59E0B'
const IBOV_COLOR = '#6366F1'
const SP500_COLOR = '#10B981'
const TWR_COLOR = '#EC4899'  // pink — destaque pra carteira no gráfico

const CLASS_COLORS: Record<string, string> = {
  'Ação': '#6366F1',
  'ETF': '#10B981',
  'FII': '#F59E0B',
  'Cripto': '#F43F5E',
  'Fundo/RF': '#8B5CF6',
}

const FALLBACK_COLORS = ['#6366F1', '#10B981', '#F59E0B', '#F43F5E', '#8B5CF6', '#06B6D4']

function classColor(name: string, idx: number) {
  return CLASS_COLORS[name] ?? FALLBACK_COLORS[idx % FALLBACK_COLORS.length]
}

function parseDateKey(d: string) {
  const [dd, mm, yy] = d.split('/')
  return `${yy}${mm}${dd}`
}

type MergedRow = {
  date: string; _s: string;
  cdi?: number; ibov?: number; sp500?: number;
  twr?: number; twr_bruto?: number; ivvb?: number;
}

function mergeSeries(
  cdi: BenchmarkPoint[],
  ibov: BenchmarkPoint[],
  sp500: BenchmarkPoint[],
): MergedRow[] {
  const map = new Map<string, MergedRow>()
  for (const p of cdi) map.set(p.date, { date: p.date, _s: parseDateKey(p.date), cdi: p.value })
  for (const p of ibov) {
    const row = map.get(p.date) ?? { date: p.date, _s: parseDateKey(p.date) }
    row.ibov = p.value
    map.set(p.date, row)
  }
  for (const p of sp500) {
    const row = map.get(p.date) ?? { date: p.date, _s: parseDateKey(p.date) }
    row.sp500 = p.value
    map.set(p.date, row)
  }
  return Array.from(map.values()).sort((a, b) => a._s.localeCompare(b._s))
}

/** Build merged chart data from imported portfolio snapshots.
 *  Each line is rebased to 0 % at the start of the selected window so
 *  short timeframes show the return INSIDE the window — not the
 *  cumulative-since-inception clipped to a tail. */
function snapshotsToChartData(
  snaps: PortfolioSnapshot[],
  months: number,
  sinceStart: boolean,
): MergedRow[] {
  if (!snaps || snaps.length === 0) return []
  const sliced = sinceStart ? snaps : snaps.slice(-months - 1)
  if (sliced.length === 0) return []
  const base = sliced[0]
  const baseFactor = (cum: number | null | undefined): number =>
    1 + (cum ?? 0)
  const factors = {
    twr: baseFactor(base.twr_cum),
    twr_bruto: baseFactor(base.twr_cum_bruto),
    cdi: baseFactor(base.cdi_cum),
    ibov: baseFactor(base.ibov_cum),
    sp500: baseFactor(base.sp500_cum),
    ivvb: baseFactor(base.ivvb11_cum),
  }
  const rebase = (cum: number | null | undefined,
                  base_f: number): number | undefined =>
    cum == null ? undefined : ((1 + cum) / base_f - 1) * 100

  return sliced.map(s => {
    const [yyyy, mm, dd] = s.month_end.split('-')
    const date = `${dd}/${mm}/${yyyy}`
    return {
      date,
      _s: `${yyyy}${mm}${dd}`,
      cdi: rebase(s.cdi_cum, factors.cdi),
      ibov: rebase(s.ibov_cum, factors.ibov),
      sp500: rebase(s.sp500_cum, factors.sp500),
      ivvb: rebase(s.ivvb11_cum, factors.ivvb),
      twr: rebase(s.twr_cum, factors.twr),
      twr_bruto: rebase(s.twr_cum_bruto, factors.twr_bruto),
    }
  })
}

/** Build chart data from the live timeseries endpoint. The TWR line comes
 *  from `points`; benchmarks come from `bench`. Both sources are sliced
 *  to the same window and rebased to 0 % at the start of the window so
 *  every line shows the return INSIDE the window — not the cumulative
 *  return since inception clipped to a tail. */
function timeseriesToChartData(
  points: PortfolioPoint[] | undefined,
  bench: BenchmarkData | undefined,
  windowMonths: number,
  sinceStart: boolean,
): MergedRow[] {
  if (!points || points.length === 0) return []

  // 1. Slice the portfolio series to the requested window.
  const sliced = sinceStart ? points : points.slice(-windowMonths - 1)
  if (sliced.length === 0) return []

  // 2. Build a row per month_end with TWR (rebased to 0 % at first row).
  const baseTwr = sliced[0].twr_cum
  const baseFactor = 1 + baseTwr  // factor at start of window
  const rows: MergedRow[] = sliced.map(p => {
    const [yyyy, mm, dd] = p.month_end.split('-')
    const date = `${dd}/${mm}/${yyyy}`
    const factor = 1 + p.twr_cum
    const rebased = factor / baseFactor - 1
    return {
      date,
      _s: `${yyyy}${mm}${dd}`,
      twr: rebased * 100,  // pct
    }
  })

  // 3. Merge benchmark series by date label. Each benchmark is rebased to
  //    its own start-of-window value, so all lines start at 0%.
  if (bench) {
    const rebaseSeries = (series: BenchmarkPoint[]) => {
      // Keep only points whose date is between rows[0] and rows[last].
      const startKey = rows[0]?._s
      const endKey = rows[rows.length - 1]?._s
      if (!startKey || !endKey) return new Map<string, number>()
      const filtered = series.filter(p => {
        const k = parseDateKey(p.date)
        return k >= startKey && k <= endKey
      })
      if (filtered.length === 0) return new Map<string, number>()
      // Each benchmark series stores cumulative % since its own start.
      // To rebase to the chart window, divide each (1 + v/100) by the
      // first value's (1 + v/100). Output back as %.
      const baseFactor = 1 + filtered[0].value / 100
      return new Map(
        filtered.map(p => {
          const f = 1 + p.value / 100
          return [p.date, (f / baseFactor - 1) * 100]
        })
      )
    }
    const cdiMap = rebaseSeries(bench.cdi)
    const ibovMap = rebaseSeries(bench.ibov)
    const sp500Map = rebaseSeries(bench.sp500)
    for (const r of rows) {
      r.cdi = cdiMap.get(r.date)
      r.ibov = ibovMap.get(r.date)
      r.sp500 = sp500Map.get(r.date)
    }
  }
  return rows
}

function fmtPct(v: number | null) {
  if (v == null) return '—'
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`
}

function fmtCurrency(v: number, currency = 'BRL', locale = 'pt-BR') {
  try {
    return new Intl.NumberFormat(locale, { style: 'currency', currency }).format(v)
  } catch {
    return v.toFixed(2)
  }
}

const tooltipStyle = {
  background: 'var(--card)',
  color: 'var(--foreground)',
  border: '1px solid var(--border)',
  borderRadius: '0.75rem',
  boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
  fontSize: '12px',
}

export default function InvestmentsPage() {
  const { t, i18n } = useTranslation()
  const locale = i18n.language === 'en' ? 'en-US' : 'pt-BR'
  const { mask, privacyMode, MASK } = usePrivacyMode()
  const { user } = useAuth()
  const userCurrency = user?.preferences?.currency_display ?? 'USD'

  const [months, setMonths] = useState(12)
  const [sinceStart, setSinceStart] = useState(false)
  const [selectedGroups, setSelectedGroups] = useState<Set<string>>(new Set())
  const [selectedClasses, setSelectedClasses] = useState<Set<AssetClass>>(new Set())
  const [importing, setImporting] = useState(false)
  const [importMessage, setImportMessage] = useState<string | null>(null)
  const fileInputRef = useRef<HTMLInputElement>(null)
  const queryClient = useQueryClient()

  const { data: groupsList } = useQuery<AssetGroup[]>({
    queryKey: ['asset-groups'],
    queryFn: () => assetGroups.list(),
  })

  // Imported snapshots (offline TWR pipeline). When present, override the
  // live benchmark series — the snapshot CSV already contains both the
  // portfolio TWR and the benchmark cumulatives at the same month-ends.
  const { data: snapshots, isLoading: snapshotsLoading } = useQuery<PortfolioSnapshot[]>({
    queryKey: ['portfolio-snapshots'],
    queryFn: () => portfolioSnapshots.list(),
    staleTime: 1000 * 60 * 5,
  })

  const hasSnapshots = (snapshots?.length ?? 0) > 0

  const { data: benchmarkData, isLoading: benchmarkLoading } = useQuery<BenchmarkData>({
    queryKey: ['inv-benchmarks-series', sinceStart ? 'start' : months],
    queryFn: () => investmentBenchmarks.series(months, sinceStart),
    staleTime: 1000 * 60 * 30,
  })

  // Live portfolio time series — Modified Dietz computed from
  // Asset/AssetValue/AssetTransaction. This is the source of truth for the
  // chart. Adding/editing assets in the UI immediately changes the chart.
  const classesParam = useMemo(
    () => Array.from(selectedClasses),
    [selectedClasses]
  )
  const groupsParam = useMemo(
    () => Array.from(selectedGroups),
    [selectedGroups]
  )
  const { data: tsData, isLoading: tsLoading } = useQuery<PortfolioPoint[]>({
    queryKey: ['portfolio-timeseries', months, sinceStart, classesParam, groupsParam],
    queryFn: () => portfolioTimeseries.series({
      months,
      sinceStart,
      assetClasses: classesParam,
      groupIds: groupsParam,
    }),
    staleTime: 1000 * 60,
  })

  async function handleImportFile(file: File) {
    setImporting(true)
    setImportMessage(null)
    try {
      const result = await portfolioSnapshots.importCsv(file)
      const errs = result.errors?.length ?? 0
      setImportMessage(
        `Importados ${result.inserted_or_updated} snapshots${errs > 0 ? ` (${errs} avisos)` : ''}.`,
      )
      await queryClient.invalidateQueries({ queryKey: ['portfolio-snapshots'] })
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : 'Falha ao importar.'
      setImportMessage(`Erro: ${msg}`)
    } finally {
      setImporting(false)
    }
  }

  function onUploadClick() {
    fileInputRef.current?.click()
  }

  async function onFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (!file) return
    await handleImportFile(file)
    e.target.value = ''
  }

  const groupIdsParam = selectedGroups.size > 0 ? [...selectedGroups].join(',') : undefined

  const { data: returnsData, isLoading: returnsLoading } = useQuery<PortfolioReturns>({
    queryKey: ['inv-benchmarks-returns', groupIdsParam],
    queryFn: () => investmentBenchmarks.returns(groupIdsParam),
  })

  const chartData = useMemo<MergedRow[]>(() => {
    // Prefer the live timeseries (single source of truth: Asset+AssetValue+AssetTransaction).
    // Fall back to the imported portfolio_snapshots only if the user has no
    // assets yet (shouldn't happen after migration, but defensive).
    if (tsData && tsData.length > 0) {
      return timeseriesToChartData(tsData, benchmarkData, months, sinceStart)
    }
    if (hasSnapshots && snapshots) {
      return snapshotsToChartData(snapshots, months, sinceStart)
    }
    if (!benchmarkData) return []
    return mergeSeries(benchmarkData.cdi, benchmarkData.ibov, benchmarkData.sp500)
  }, [tsData, benchmarkData, hasSnapshots, snapshots, months, sinceStart])

  // Latest snapshot — used to show TWR badges from imported data.
  const latestSnap = hasSnapshots && snapshots ? snapshots[snapshots.length - 1] : null
  const latestTs = tsData && tsData.length > 0 ? tsData[tsData.length - 1] : null

  const groups = groupsList ?? []
  const consolidated = returnsData?.consolidated
  const byGroup = returnsData?.by_group ?? []
  const byClass = returnsData?.by_class ?? []

  const lastCdi = benchmarkData?.cdi?.at(-1)?.value ?? null
  const lastIbov = benchmarkData?.ibov?.at(-1)?.value ?? null

  function toggleGroup(id: string) {
    setSelectedGroups(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  const portfolioRefLines = useMemo(() => {
    const lines: { label: string; value: number; color: string }[] = []
    if (selectedGroups.size === 0) {
      if (consolidated?.return_pct != null) {
        lines.push({ label: t('investments.consolidated'), value: consolidated.return_pct, color: '#EC4899' })
      }
    } else {
      for (const g of byGroup) {
        if (selectedGroups.has(g.id) && g.return_pct != null) {
          const wallet = groups.find((w: AssetGroup) => w.id === g.id)
          lines.push({ label: g.name, value: g.return_pct, color: wallet?.color ?? '#EC4899' })
        }
      }
    }
    return lines
  }, [selectedGroups, consolidated, byGroup, groups, t])

  const tickInterval = chartData.length > 0
    ? Math.max(1, Math.floor(chartData.length / 8))
    : 1

  const barData = byClass.map((c: ClassReturn, i: number) => ({
    name: c.name,
    value: c.return_pct ?? 0,
    fill: classColor(c.name, i),
  }))

  return (
    <div>
      <PageHeader
        section={t('nav.groupAnalysis')}
        title={t('investments.title')}
        action={
          <div className="flex items-center gap-2">
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv"
              className="hidden"
              onChange={onFileChange}
            />
            <button
              onClick={onUploadClick}
              disabled={importing}
              className="px-3 py-1.5 text-xs font-semibold rounded-lg border border-border text-muted-foreground hover:text-foreground hover:bg-muted/50 disabled:opacity-50"
              title="Importar twr_full.csv (saída do compute_twr_v2.py + merge_twr_benchmarks.py)"
            >
              {importing ? 'Importando…' : 'Importar TWR (CSV)'}
            </button>
            <div className="flex items-center rounded-lg border border-border bg-card overflow-hidden">
              {([3, 6, 12, 24] as const).map(m => (
                <button
                  key={m}
                  onClick={() => { setMonths(m); setSinceStart(false) }}
                  className={`px-3 py-1.5 text-xs font-semibold transition-colors ${
                    !sinceStart && months === m
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
                  }`}
                >
                  {m < 12 ? `${m}M` : `${m / 12}A`}
                </button>
              ))}
              <button
                onClick={() => setSinceStart(true)}
                className={`px-3 py-1.5 text-xs font-semibold transition-colors ${
                  sinceStart
                    ? 'bg-primary text-primary-foreground'
                    : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
                }`}
              >
                {t('investments.sinceStart')}
              </button>
            </div>
          </div>
        }
      />
      {importMessage && (
        <div className="mb-4 px-4 py-2 rounded-lg bg-muted/50 border border-border text-xs text-foreground">
          {importMessage}
        </div>
      )}

      {(latestTs || latestSnap) && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-5">
          <div className="rounded-xl border border-border bg-card p-4">
            <p className="text-[11px] text-muted-foreground uppercase tracking-wider">V_end TOTAL</p>
            <p className="text-base font-bold text-foreground tabular-nums">
              {mask(fmtCurrency(
                latestTs?.v_end ?? latestSnap?.v_end_total ?? 0,
                userCurrency, locale))}
            </p>
            <p className="text-[10px] text-muted-foreground mt-0.5">
              {latestTs?.month_end ?? latestSnap?.month_end}
            </p>
          </div>
          <div className="rounded-xl border border-border bg-card p-4">
            <p className="text-[11px] text-muted-foreground uppercase tracking-wider">Por classe</p>
            {latestTs ? (
              Object.entries(latestTs.by_class)
                .sort((a, b) => b[1] - a[1])
                .slice(0, 4)
                .map(([cls, v]) => (
                  <p key={cls} className="text-xs font-semibold text-foreground tabular-nums leading-snug flex justify-between gap-2">
                    <span className="text-muted-foreground truncate">
                      {ASSET_CLASS_LABELS[cls as AssetClass] ?? cls}
                    </span>
                    <span>{mask(fmtCurrency(v, userCurrency, locale))}</span>
                  </p>
                ))
            ) : (
              <>
                <p className="text-xs font-semibold text-foreground tabular-nums leading-snug">
                  {mask(fmtCurrency(latestSnap?.v_end_rv ?? 0, 'BRL', locale))}
                </p>
                <p className="text-xs font-semibold text-foreground tabular-nums leading-snug">
                  {mask(fmtCurrency(latestSnap?.v_end_rf ?? 0, 'BRL', locale))}
                </p>
                <p className="text-xs font-semibold text-foreground tabular-nums leading-snug">
                  {mask(fmtCurrency(latestSnap?.v_end_us ?? 0, 'BRL', locale))}
                </p>
              </>
            )}
          </div>
          <div className="rounded-xl border border-border bg-card p-4">
            <p className="text-[11px] text-muted-foreground uppercase tracking-wider">
              TWR (cumulativo)
            </p>
            <p className={`text-base font-bold tabular-nums ${
              (latestTs?.twr_cum ?? latestSnap?.twr_cum ?? 0) >= 0
                ? 'text-emerald-600' : 'text-rose-500'
            }`}>
              {privacyMode ? MASK : fmtPct((latestTs?.twr_cum ?? latestSnap?.twr_cum ?? 0) * 100)}
            </p>
            <p className="text-[10px] text-muted-foreground mt-0.5">
              {sinceStart ? 'desde o início' : `últimos ${months}m`}
            </p>
          </div>
          <div className="rounded-xl border border-border bg-card p-4">
            <p className="text-[11px] text-muted-foreground uppercase tracking-wider">No período</p>
            {chartData.length > 0 ? (
              <p className={`text-base font-bold tabular-nums ${
                ((chartData[chartData.length - 1] as MergedRow).twr ?? 0) >= 0
                  ? 'text-emerald-600' : 'text-rose-500'
              }`}>
                {privacyMode ? MASK : fmtPct((chartData[chartData.length - 1] as MergedRow).twr ?? 0)}
              </p>
            ) : (
              <p className="text-base font-bold text-muted-foreground">—</p>
            )}
            <p className="text-[10px] text-muted-foreground mt-0.5">retorno na janela</p>
          </div>
        </div>
      )}

      {/* Filter chips by asset class */}
      <div className="flex flex-wrap items-center gap-2 mb-5">
        <span className="text-[11px] uppercase tracking-wider text-muted-foreground">Classe:</span>
        <button
          onClick={() => setSelectedClasses(new Set())}
          className={`px-3 py-1.5 text-xs font-semibold rounded-lg border transition-colors ${
            selectedClasses.size === 0
              ? 'bg-primary text-primary-foreground border-primary'
              : 'border-border text-muted-foreground hover:text-foreground hover:bg-muted/50'
          }`}
        >
          Todas
        </button>
        {ASSET_CLASS_OPTIONS.map(opt => (
          <button
            key={opt.value}
            onClick={() => setSelectedClasses(prev => {
              const next = new Set(prev)
              if (next.has(opt.value)) next.delete(opt.value)
              else next.add(opt.value)
              return next
            })}
            className={`px-3 py-1.5 text-xs font-semibold rounded-lg border transition-colors ${
              selectedClasses.has(opt.value)
                ? 'bg-primary text-primary-foreground border-primary'
                : 'border-border text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>

      {/* Wallet filter chips */}
      {groups.length > 0 && (
        <div className="flex flex-wrap gap-2 mb-5">
          <button
            onClick={() => setSelectedGroups(new Set())}
            className={`px-3 py-1.5 text-xs font-semibold rounded-lg border transition-colors ${
              selectedGroups.size === 0
                ? 'bg-primary text-primary-foreground border-primary'
                : 'border-border text-muted-foreground hover:text-foreground hover:bg-muted/50'
            }`}
          >
            {t('investments.consolidated')}
          </button>
          {groups.map((g: AssetGroup) => (
            <button
              key={g.id}
              onClick={() => toggleGroup(g.id)}
              className={`px-3 py-1.5 text-xs font-semibold rounded-lg border transition-colors ${
                selectedGroups.has(g.id)
                  ? 'border-transparent text-white'
                  : 'border-border text-muted-foreground hover:text-foreground hover:bg-muted/50'
              }`}
              style={selectedGroups.has(g.id) ? { backgroundColor: g.color } : {}}
            >
              {g.name}
            </button>
          ))}
        </div>
      )}

      {/* Benchmark + Portfolio line chart */}
      <div className="bg-card rounded-xl border border-border shadow-sm mb-5">
        <div className="px-5 pt-5 pb-2 flex flex-wrap items-center gap-x-5 gap-y-2">
          <p className="text-sm font-semibold text-foreground">{t('investments.benchmarks')}</p>
          <div className="flex flex-wrap items-center gap-x-4 gap-y-1 ml-auto">
            {[
              ...((tsData && tsData.length > 0) || hasSnapshots ? [
                { label: 'TWR Carteira', color: TWR_COLOR, dashed: false },
              ] : []),
              { label: 'CDI', color: CDI_COLOR, dashed: false },
              { label: 'IBOV', color: IBOV_COLOR, dashed: false },
              { label: 'S&P 500', color: SP500_COLOR, dashed: false },
              ...((tsData && tsData.length > 0) || hasSnapshots ? []
                : portfolioRefLines.map(rl => ({ label: rl.label, color: rl.color, dashed: true }))),
            ].map(item => (
              <div key={item.label} className="flex items-center gap-1.5">
                <div
                  className="w-5 border-t-2"
                  style={{ borderColor: item.color, borderStyle: item.dashed ? 'dashed' : 'solid' }}
                />
                <span className="text-[11px] text-muted-foreground">{item.label}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="px-1 pb-4" style={{ height: 360 }}>
          {(benchmarkLoading || snapshotsLoading || tsLoading) ? (
            <div className="px-4 h-full"><Skeleton className="h-full w-full" /></div>
          ) : chartData.length > 0 ? (
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={chartData} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" strokeOpacity={0.4} />
                <XAxis
                  dataKey="date"
                  tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                  axisLine={false}
                  tickLine={false}
                  interval={tickInterval}
                />
                <YAxis
                  tickFormatter={v => `${(v as number) >= 0 ? '+' : ''}${(v as number).toFixed(0)}%`}
                  tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                  axisLine={false}
                  tickLine={false}
                  width={52}
                />
                <Tooltip
                  formatter={(value: unknown, name: string | undefined) => [
                    `${(value as number).toFixed(2)}%`,
                    name,
                  ]}
                  labelFormatter={label => label as string}
                  contentStyle={tooltipStyle}
                />
                <ReferenceLine y={0} stroke="var(--border)" />
                <Line type="monotone" dataKey="cdi" stroke={CDI_COLOR} strokeWidth={1.5} dot={false} name="CDI" connectNulls />
                <Line type="monotone" dataKey="ibov" stroke={IBOV_COLOR} strokeWidth={1.5} dot={false} name="IBOV" connectNulls />
                <Line type="monotone" dataKey="sp500" stroke={SP500_COLOR} strokeWidth={1.5} dot={false} name="S&P 500" connectNulls />
                {((tsData && tsData.length > 0) || hasSnapshots) && (
                  <Line type="monotone" dataKey="twr" stroke={TWR_COLOR} strokeWidth={2.5} dot={false} name="TWR Carteira" connectNulls />
                )}
                {!(tsData && tsData.length > 0) && !hasSnapshots && portfolioRefLines.map(rl => (
                  <ReferenceLine
                    key={rl.label}
                    y={rl.value}
                    stroke={rl.color}
                    strokeDasharray="6 3"
                    strokeWidth={2}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          ) : (
            <p className="text-muted-foreground text-sm text-center py-16">{t('investments.noData')}</p>
          )}
        </div>
      </div>

      {/* Portfolio returns + asset class breakdown */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-5">
        {/* Portfolio returns */}
        <div className="bg-card rounded-xl border border-border shadow-sm">
          <div className="px-5 pt-5 pb-4">
            <p className="text-sm font-semibold text-foreground mb-4">{t('investments.portfolioReturn')}</p>
            {returnsLoading ? (
              <div className="space-y-3">
                {[0, 1, 2].map(i => <Skeleton key={i} className="h-12 w-full" />)}
              </div>
            ) : consolidated ? (
              <div className="space-y-1.5">
                <div className="flex items-center justify-between p-3 rounded-lg bg-muted/40">
                  <div>
                    <p className="text-xs font-semibold text-foreground">{t('investments.consolidated')}</p>
                    <p className="text-[11px] text-muted-foreground">
                      {mask(fmtCurrency(consolidated.invested, userCurrency, locale))}
                      {' → '}
                      {mask(fmtCurrency(consolidated.current, userCurrency, locale))}
                    </p>
                  </div>
                  <span className={`text-sm font-bold tabular-nums ${(consolidated.return_pct ?? 0) >= 0 ? 'text-emerald-600' : 'text-rose-500'}`}>
                    {privacyMode ? MASK : fmtPct(consolidated.return_pct)}
                  </span>
                </div>
                {byGroup.map((g: GroupReturn) => {
                  const wallet = groups.find((w: AssetGroup) => w.id === g.id)
                  return (
                    <div key={g.id} className="flex items-center justify-between px-3 py-2.5 rounded-lg hover:bg-muted/20 transition-colors">
                      <div className="flex items-center gap-2 min-w-0">
                        {wallet && (
                          <div className="w-2 h-2 rounded-full shrink-0" style={{ backgroundColor: wallet.color }} />
                        )}
                        <div className="min-w-0">
                          <p className="text-xs font-medium text-foreground truncate">{g.name}</p>
                          <p className="text-[11px] text-muted-foreground">
                            {mask(fmtCurrency(g.invested, userCurrency, locale))}
                            {' → '}
                            {mask(fmtCurrency(g.current, userCurrency, locale))}
                          </p>
                        </div>
                      </div>
                      <span className={`text-sm font-bold tabular-nums shrink-0 ${(g.return_pct ?? 0) >= 0 ? 'text-emerald-600' : 'text-rose-500'}`}>
                        {privacyMode ? MASK : fmtPct(g.return_pct)}
                      </span>
                    </div>
                  )
                })}
              </div>
            ) : (
              <p className="text-muted-foreground text-sm text-center py-8">{t('investments.noData')}</p>
            )}
          </div>
        </div>

        {/* Asset class bar chart */}
        <div className="bg-card rounded-xl border border-border shadow-sm">
          <div className="px-5 pt-5 pb-4">
            <p className="text-sm font-semibold text-foreground mb-4">{t('investments.byClass')}</p>
            {returnsLoading ? (
              <div className="h-48"><Skeleton className="h-full w-full" /></div>
            ) : barData.length > 0 ? (
              <div style={{ height: Math.max(barData.length * 44 + 40, 160) }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart
                    data={barData}
                    layout="vertical"
                    margin={{ top: 4, right: 20, left: 4, bottom: 4 }}
                  >
                    <XAxis
                      type="number"
                      tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                      axisLine={false}
                      tickLine={false}
                      tickFormatter={v => `${(v as number) >= 0 ? '+' : ''}${(v as number).toFixed(0)}%`}
                    />
                    <YAxis
                      type="category"
                      dataKey="name"
                      tick={{ fontSize: 11, fill: 'var(--foreground)' }}
                      axisLine={false}
                      tickLine={false}
                      width={68}
                    />
                    <Tooltip
                      formatter={(value: unknown) => [
                        `${(value as number).toFixed(2)}%`,
                        t('investments.return'),
                      ]}
                      contentStyle={tooltipStyle}
                    />
                    {lastCdi != null && (
                      <ReferenceLine
                        x={lastCdi}
                        stroke={CDI_COLOR}
                        strokeDasharray="4 2"
                        strokeWidth={1.5}
                        label={{ value: 'CDI', position: 'insideTopRight', fontSize: 9, fill: CDI_COLOR }}
                      />
                    )}
                    {lastIbov != null && (
                      <ReferenceLine
                        x={lastIbov}
                        stroke={IBOV_COLOR}
                        strokeDasharray="4 2"
                        strokeWidth={1.5}
                        label={{ value: 'IBOV', position: 'insideTopRight', fontSize: 9, fill: IBOV_COLOR }}
                      />
                    )}
                    <Bar dataKey="value" radius={[0, 4, 4, 0]} maxBarSize={28}>
                      {barData.map(entry => (
                        <Cell key={entry.name} fill={entry.fill} />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            ) : (
              <p className="text-muted-foreground text-sm text-center py-8">{t('investments.noData')}</p>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
