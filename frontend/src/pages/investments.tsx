import { useState, useMemo, useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { investments as investmentsApi } from '@/lib/api'
import { toast } from 'sonner'
import { usePrivacyMode } from '@/hooks/use-privacy-mode'
import type {
  InvestmentPortfolio,
  InvestmentPosition,
  InvestmentAssetType,
  AllocationItem,
} from '@/types'
import {
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import {
  TrendingUp,
  TrendingDown,
  Plus,
  Pencil,
  Trash2,
  RefreshCw,
  DollarSign,
  Wallet,
  BarChart2,
  Layers,
} from 'lucide-react'
import { cn } from '@/lib/utils'

// ─── Constants ───────────────────────────────────────────────────────────────

const ASSET_TYPE_LABELS: Record<InvestmentAssetType, string> = {
  stock_br: 'Ação BR',
  stock_us: 'Ação EUA',
  fii: 'FII',
  etf_br: 'ETF BR',
  etf_us: 'ETF EUA',
  tesouro: 'Tesouro Direto',
  cdb: 'CDB',
  bitcoin: 'Bitcoin',
}

const ASSET_TYPE_CURRENCIES: Record<InvestmentAssetType, string> = {
  stock_br: 'BRL',
  stock_us: 'USD',
  fii: 'BRL',
  etf_br: 'BRL',
  etf_us: 'USD',
  tesouro: 'BRL',
  cdb: 'BRL',
  bitcoin: 'BRL',
}

const ASSET_COLORS: Record<InvestmentAssetType, string> = {
  stock_br: '#6366F1',
  stock_us: '#10B981',
  fii: '#F59E0B',
  etf_br: '#8B5CF6',
  etf_us: '#06B6D4',
  tesouro: '#84CC16',
  cdb: '#F43F5E',
  bitcoin: '#F97316',
}

const PORTFOLIO_COLORS = ['#6366F1', '#10B981', '#F59E0B', '#F43F5E', '#8B5CF6']

const BENCHMARK_MONTHS = [3, 6, 12, 24, 60]

// ─── Helpers ─────────────────────────────────────────────────────────────────

function fmt(value: number, currency = 'BRL', hideValue = false): string {
  if (hideValue) return '••••••'
  return new Intl.NumberFormat('pt-BR', {
    style: 'currency',
    currency,
    minimumFractionDigits: 2,
  }).format(value)
}

function fmtPct(value: number | null | undefined, hideValue = false): string {
  if (hideValue) return '••%'
  if (value == null) return '—'
  const sign = value >= 0 ? '+' : ''
  return `${sign}${value.toFixed(2)}%`
}

// ─── Sub-components ──────────────────────────────────────────────────────────

function SummaryCard({
  label,
  value,
  sub,
  positive,
  icon: Icon,
}: {
  label: string
  value: string
  sub?: string
  positive?: boolean
  icon: React.ElementType
}) {
  return (
    <div className="rounded-xl border bg-card p-5 flex flex-col gap-2">
      <div className="flex items-center justify-between">
        <span className="text-sm text-muted-foreground">{label}</span>
        <div className="h-8 w-8 rounded-lg bg-primary/10 flex items-center justify-center">
          <Icon className="h-4 w-4 text-primary" />
        </div>
      </div>
      <p className="text-2xl font-bold tracking-tight">{value}</p>
      {sub && (
        <p
          className={cn(
            'text-sm font-medium',
            positive === true && 'text-emerald-600',
            positive === false && 'text-rose-500',
            positive === undefined && 'text-muted-foreground',
          )}
        >
          {sub}
        </p>
      )}
    </div>
  )
}

function AllocationChart({
  allocation,
  hideValues,
}: {
  allocation: AllocationItem[]
  hideValues: boolean
}) {
  const [active, setActive] = useState<string | null>(null)

  if (!allocation.length) {
    return (
      <div className="flex items-center justify-center h-40 text-muted-foreground text-sm">
        Sem posições cadastradas
      </div>
    )
  }

  return (
    <div className="flex flex-col sm:flex-row items-center gap-6">
      <ResponsiveContainer width={180} height={180}>
        <PieChart>
          <Pie
            data={allocation}
            dataKey="value"
            nameKey="type"
            cx="50%"
            cy="50%"
            innerRadius={50}
            outerRadius={80}
            onMouseEnter={(_, idx) => setActive(allocation[idx]?.type ?? null)}
            onMouseLeave={() => setActive(null)}
          >
            {allocation.map((item) => (
              <Cell
                key={item.type}
                fill={ASSET_COLORS[item.type as InvestmentAssetType] ?? '#94a3b8'}
                opacity={active && active !== item.type ? 0.5 : 1}
                stroke="transparent"
              />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>

      <div className="flex flex-col gap-2 flex-1">
        {allocation.map((item) => (
          <div key={item.type} className="flex items-center gap-2 text-sm">
            <span
              className="h-2.5 w-2.5 rounded-full flex-shrink-0"
              style={{
                backgroundColor:
                  ASSET_COLORS[item.type as InvestmentAssetType] ?? '#94a3b8',
              }}
            />
            <span className="flex-1 text-muted-foreground">
              {ASSET_TYPE_LABELS[item.type as InvestmentAssetType] ?? item.type}
            </span>
            <span className="font-medium">{item.pct.toFixed(1)}%</span>
            <span className="text-muted-foreground">
              {hideValues ? '••••' : fmt(item.value)}
            </span>
          </div>
        ))}
      </div>
    </div>
  )
}

function BenchmarkChart({ months }: { months: number }) {
  const { data, isLoading } = useQuery({
    queryKey: ['investment-benchmarks', months],
    queryFn: () => investmentsApi.benchmarks(months),
    staleTime: 1000 * 60 * 30,
  })

  if (isLoading) return <Skeleton className="h-64 w-full" />
  if (!data) return null

  // Merge all series into a single array keyed by index (CDI has more points)
  // Sample down to ~60 points for readability
  const cdi = data.cdi ?? []
  const ibov = data.ibov ?? []
  const sp500 = data.sp500 ?? []
  const maxLen = Math.max(cdi.length, ibov.length, sp500.length)
  if (maxLen === 0) return <p className="text-sm text-muted-foreground">Dados de benchmark indisponíveis</p>

  const step = Math.max(1, Math.floor(maxLen / 60))
  const merged: { date: string; cdi?: number; ibov?: number; sp500?: number }[] = []

  for (let i = 0; i < maxLen; i += step) {
    merged.push({
      date: cdi[i]?.date ?? ibov[i]?.date ?? sp500[i]?.date ?? '',
      cdi: cdi[i]?.value,
      ibov: ibov[i]?.value,
      sp500: sp500[i]?.value,
    })
  }

  return (
    <ResponsiveContainer width="100%" height={260}>
      <LineChart data={merged} margin={{ top: 4, right: 8, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
        <XAxis
          dataKey="date"
          tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
          tickLine={false}
          interval="preserveStartEnd"
        />
        <YAxis
          tickFormatter={(v: number) => `${v.toFixed(1)}%`}
          tick={{ fontSize: 11, fill: 'var(--muted-foreground)' }}
          tickLine={false}
          axisLine={false}
        />
        <Tooltip
          formatter={(value: unknown, name: string | undefined) => [
            `${(value as number).toFixed(2)}%`,
            name === 'cdi' ? 'CDI' : name === 'ibov' ? 'IBOV' : 'S&P 500',
          ]}
          contentStyle={{
            background: 'var(--card)',
            border: '1px solid var(--border)',
            borderRadius: 8,
            fontSize: 12,
          }}
        />
        <Legend
          formatter={(value) =>
            value === 'cdi' ? 'CDI' : value === 'ibov' ? 'IBOV' : 'S&P 500'
          }
          wrapperStyle={{ fontSize: 12 }}
        />
        {cdi.length > 0 && (
          <Line
            type="monotone"
            dataKey="cdi"
            stroke="#10B981"
            dot={false}
            strokeWidth={2}
          />
        )}
        {ibov.length > 0 && (
          <Line
            type="monotone"
            dataKey="ibov"
            stroke="#6366F1"
            dot={false}
            strokeWidth={2}
          />
        )}
        {sp500.length > 0 && (
          <Line
            type="monotone"
            dataKey="sp500"
            stroke="#F59E0B"
            dot={false}
            strokeWidth={2}
          />
        )}
      </LineChart>
    </ResponsiveContainer>
  )
}

function PositionRow({
  pos,
  hideValues,
  onEdit,
  onDelete,
}: {
  pos: InvestmentPosition
  hideValues: boolean
  onEdit: (pos: InvestmentPosition) => void
  onDelete: (pos: InvestmentPosition) => void
}) {
  const isPositive = (pos.gain_loss ?? 0) >= 0
  const currency = pos.currency as 'BRL' | 'USD'

  return (
    <tr className="border-b border-border last:border-0 hover:bg-muted/30 transition-colors">
      <td className="py-3 px-4">
        <div className="flex flex-col">
          <span className="font-semibold text-sm">{pos.ticker}</span>
          <span className="text-xs text-muted-foreground truncate max-w-[160px]">{pos.name}</span>
        </div>
      </td>
      <td className="py-3 px-4">
        <Badge
          variant="secondary"
          style={{
            backgroundColor: `${ASSET_COLORS[pos.asset_type]}20`,
            color: ASSET_COLORS[pos.asset_type],
            borderColor: 'transparent',
          }}
        >
          {ASSET_TYPE_LABELS[pos.asset_type] ?? pos.asset_type}
        </Badge>
      </td>
      <td className="py-3 px-4 text-right text-sm">
        {hideValues ? '••••' : pos.units.toLocaleString('pt-BR', { maximumFractionDigits: 6 })}
      </td>
      <td className="py-3 px-4 text-right text-sm">
        {pos.current_price != null
          ? fmt(pos.current_price, currency, hideValues)
          : '—'}
        {pos.change_pct != null && (
          <span
            className={cn(
              'ml-1 text-xs',
              pos.change_pct >= 0 ? 'text-emerald-600' : 'text-rose-500',
            )}
          >
            {fmtPct(pos.change_pct)}
          </span>
        )}
      </td>
      <td className="py-3 px-4 text-right text-sm font-medium">
        {pos.current_value_brl != null
          ? fmt(pos.current_value_brl, 'BRL', hideValues)
          : fmt(pos.total_invested, 'BRL', hideValues)}
      </td>
      <td className="py-3 px-4 text-right text-sm">
        {pos.gain_loss != null ? (
          <div className="flex flex-col items-end">
            <span className={cn(isPositive ? 'text-emerald-600' : 'text-rose-500', 'font-medium')}>
              {fmt(pos.gain_loss, currency, hideValues)}
            </span>
            <span className={cn(isPositive ? 'text-emerald-600' : 'text-rose-500', 'text-xs')}>
              {fmtPct(pos.gain_loss_pct, hideValues)}
            </span>
          </div>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </td>
      <td className="py-3 px-4">
        <div className="flex items-center justify-end gap-1">
          <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => onEdit(pos)}>
            <Pencil className="h-3.5 w-3.5" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="h-7 w-7 text-destructive hover:text-destructive"
            onClick={() => onDelete(pos)}
          >
            <Trash2 className="h-3.5 w-3.5" />
          </Button>
        </div>
      </td>
    </tr>
  )
}

// ─── Position Dialog ──────────────────────────────────────────────────────────

interface PositionForm {
  ticker: string
  name: string
  asset_type: InvestmentAssetType | ''
  currency: string
  units: string
  avg_price: string
  broker: string
  notes: string
}

const EMPTY_FORM: PositionForm = {
  ticker: '',
  name: '',
  asset_type: '',
  currency: 'BRL',
  units: '',
  avg_price: '',
  broker: '',
  notes: '',
}

function PositionDialog({
  open,
  onClose,
  portfolioId,
  editPosition,
}: {
  open: boolean
  onClose: () => void
  portfolioId: string
  editPosition: InvestmentPosition | null
}) {
  const qc = useQueryClient()
  const [form, setForm] = useState<PositionForm>(EMPTY_FORM)

  useEffect(() => {
    if (open) {
      if (editPosition) {
        setForm({
          ticker: editPosition.ticker,
          name: editPosition.name,
          asset_type: editPosition.asset_type,
          currency: editPosition.currency,
          units: String(editPosition.units),
          avg_price: String(editPosition.avg_price),
          broker: editPosition.broker ?? '',
          notes: editPosition.notes ?? '',
        })
      } else {
        setForm(EMPTY_FORM)
      }
    }
  }, [open, editPosition])

  const set = (field: keyof PositionForm) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setForm((f) => ({ ...f, [field]: e.target.value }))

  const addMut = useMutation({
    mutationFn: () =>
      investmentsApi.addPosition(portfolioId, {
        ticker: form.ticker.trim().toUpperCase(),
        name: form.name.trim(),
        asset_type: form.asset_type as InvestmentAssetType,
        currency: form.currency,
        units: parseFloat(form.units),
        avg_price: parseFloat(form.avg_price),
        broker: form.broker || undefined,
        notes: form.notes || undefined,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success('Posição adicionada')
      onClose()
    },
    onError: () => toast.error('Erro ao adicionar posição'),
  })

  const editMut = useMutation({
    mutationFn: () =>
      investmentsApi.updatePosition(editPosition!.id, {
        ticker: form.ticker.trim().toUpperCase(),
        name: form.name.trim(),
        asset_type: form.asset_type as InvestmentAssetType,
        currency: form.currency,
        units: parseFloat(form.units),
        avg_price: parseFloat(form.avg_price),
        broker: form.broker || undefined,
        notes: form.notes || undefined,
      }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success('Posição atualizada')
      onClose()
    },
    onError: () => toast.error('Erro ao atualizar posição'),
  })

  const isValid =
    form.ticker &&
    form.name &&
    form.asset_type &&
    !isNaN(parseFloat(form.units)) &&
    !isNaN(parseFloat(form.avg_price))

  const handleAssetTypeChange = (val: string) => {
    const type = val as InvestmentAssetType
    setForm((f) => ({
      ...f,
      asset_type: type,
      currency: ASSET_TYPE_CURRENCIES[type] ?? 'BRL',
    }))
  }

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>{editPosition ? 'Editar posição' : 'Adicionar posição'}</DialogTitle>
        </DialogHeader>

        <div className="grid gap-4 py-2">
          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label>Tipo de ativo</Label>
              <select
                value={form.asset_type}
                onChange={(e) => handleAssetTypeChange(e.target.value)}
                className="flex h-9 w-full rounded-md border border-input bg-transparent px-3 py-1 text-sm shadow-sm focus:outline-none focus:ring-1 focus:ring-ring"
              >
                <option value="" disabled>Selecione...</option>
                {(Object.entries(ASSET_TYPE_LABELS) as [InvestmentAssetType, string][]).map(
                  ([key, label]) => (
                    <option key={key} value={key}>{label}</option>
                  ),
                )}
              </select>
            </div>

            <div className="space-y-1.5">
              <Label>Ticker / Código</Label>
              <Input
                placeholder="Ex: PETR4, AAPL"
                value={form.ticker}
                onChange={set('ticker')}
                className="uppercase"
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <Label>Nome do ativo</Label>
            <Input
              placeholder="Ex: Petrobras PN, Apple Inc."
              value={form.name}
              onChange={set('name')}
            />
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div className="space-y-1.5">
              <Label>Quantidade</Label>
              <Input
                type="number"
                placeholder="0"
                value={form.units}
                onChange={set('units')}
                min={0}
                step="any"
              />
            </div>

            <div className="space-y-1.5">
              <Label>Preço médio ({form.currency})</Label>
              <Input
                type="number"
                placeholder="0,00"
                value={form.avg_price}
                onChange={set('avg_price')}
                min={0}
                step="any"
              />
            </div>
          </div>

          <div className="space-y-1.5">
            <Label>Corretora (opcional)</Label>
            <Input placeholder="Ex: XP, Clear, BTG" value={form.broker} onChange={set('broker')} />
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancelar
          </Button>
          <Button
            disabled={!isValid || addMut.isPending || editMut.isPending}
            onClick={() => (editPosition ? editMut.mutate() : addMut.mutate())}
          >
            {editPosition ? 'Salvar' : 'Adicionar'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ─── Portfolio Dialog ─────────────────────────────────────────────────────────

function PortfolioDialog({
  open,
  onClose,
  editPortfolio,
}: {
  open: boolean
  onClose: () => void
  editPortfolio: InvestmentPortfolio | null
}) {
  const qc = useQueryClient()
  const [name, setName] = useState('')
  const [color, setColor] = useState('#6366F1')

  useEffect(() => {
    if (open) {
      setName(editPortfolio?.name ?? '')
      setColor(editPortfolio?.color ?? '#6366F1')
    }
  }, [open, editPortfolio])

  const createMut = useMutation({
    mutationFn: () => investmentsApi.createPortfolio({ name: name.trim(), color }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success('Carteira criada')
      onClose()
    },
    onError: (e: Error) => toast.error(e.message ?? 'Erro ao criar carteira'),
  })

  const updateMut = useMutation({
    mutationFn: () =>
      investmentsApi.updatePortfolio(editPortfolio!.id, { name: name.trim(), color }),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success('Carteira atualizada')
      onClose()
    },
    onError: () => toast.error('Erro ao atualizar carteira'),
  })

  return (
    <Dialog open={open} onOpenChange={(o) => !o && onClose()}>
      <DialogContent className="max-w-sm">
        <DialogHeader>
          <DialogTitle>{editPortfolio ? 'Editar carteira' : 'Nova carteira'}</DialogTitle>
        </DialogHeader>
        <div className="grid gap-4 py-2">
          <div className="space-y-1.5">
            <Label>Nome da carteira</Label>
            <Input
              placeholder="Ex: Longo prazo, Especulativa"
              value={name}
              onChange={(e) => setName(e.target.value)}
            />
          </div>
          <div className="space-y-1.5">
            <Label>Cor</Label>
            <div className="flex gap-2 flex-wrap">
              {PORTFOLIO_COLORS.map((c) => (
                <button
                  key={c}
                  type="button"
                  onClick={() => setColor(c)}
                  className={cn(
                    'h-7 w-7 rounded-full border-2 transition-transform',
                    color === c ? 'border-foreground scale-110' : 'border-transparent',
                  )}
                  style={{ backgroundColor: c }}
                />
              ))}
            </div>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={onClose}>
            Cancelar
          </Button>
          <Button
            disabled={!name.trim() || createMut.isPending || updateMut.isPending}
            onClick={() => (editPortfolio ? updateMut.mutate() : createMut.mutate())}
          >
            {editPortfolio ? 'Salvar' : 'Criar'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// ─── Main Page ────────────────────────────────────────────────────────────────

type TabId = 'consolidated' | string // string = portfolio id

export default function InvestmentsPage() {
  const { privacyMode: isPrivacyMode } = usePrivacyMode()
  const qc = useQueryClient()

  const [activeTab, setActiveTab] = useState<TabId>('consolidated')
  const [benchmarkMonths, setBenchmarkMonths] = useState(12)
  const [positionDialog, setPositionDialog] = useState<{
    open: boolean
    portfolioId: string
    edit: InvestmentPosition | null
  }>({ open: false, portfolioId: '', edit: null })
  const [portfolioDialog, setPortfolioDialog] = useState<{
    open: boolean
    edit: InvestmentPortfolio | null
  }>({ open: false, edit: null })
  const [deleteConfirm, setDeleteConfirm] = useState<{
    type: 'position' | 'portfolio'
    id: string
    name: string
  } | null>(null)

  const { data: summary, isLoading } = useQuery({
    queryKey: ['investment-summary'],
    queryFn: () => investmentsApi.summary(),
    staleTime: 1000 * 60 * 5,
  })

  const refreshMut = useMutation({
    mutationFn: () => investmentsApi.refreshPrices(),
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success(`${res.refreshed} cotações atualizadas`)
    },
    onError: () => toast.error('Erro ao atualizar cotações'),
  })

  const deletePositionMut = useMutation({
    mutationFn: (id: string) => investmentsApi.deletePosition(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success('Posição removida')
      setDeleteConfirm(null)
    },
  })

  const deletePortfolioMut = useMutation({
    mutationFn: (id: string) => investmentsApi.deletePortfolio(id),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ['investment-summary'] })
      toast.success('Carteira removida')
      setDeleteConfirm(null)
      setActiveTab('consolidated')
    },
  })

  // Derived data based on active tab
  const activePortfolio = useMemo(() => {
    if (activeTab === 'consolidated' || !summary) return null
    return summary.portfolios.find((p) => p.id === activeTab) ?? null
  }, [activeTab, summary])

  const displayPositions = useMemo<InvestmentPosition[]>(() => {
    if (!summary) return []
    if (activeTab === 'consolidated') {
      return summary.portfolios.flatMap((p) => p.positions)
    }
    return activePortfolio?.positions ?? []
  }, [summary, activeTab, activePortfolio])

  const displayAllocation = useMemo<AllocationItem[]>(() => {
    if (!summary) return []
    if (activeTab === 'consolidated') return summary.allocation

    const totals: Record<string, number> = {}
    displayPositions.forEach((pos) => {
      const val = pos.current_value_brl ?? pos.total_invested
      totals[pos.asset_type] = (totals[pos.asset_type] ?? 0) + val
    })
    const total = Object.values(totals).reduce((a, b) => a + b, 0) || 1
    return Object.entries(totals)
      .sort((a, b) => b[1] - a[1])
      .map(([type, value]) => ({ type, value: Math.round(value * 100) / 100, pct: Math.round(value / total * 1000) / 10 }))
  }, [summary, activeTab, displayPositions])

  const displayStats = useMemo(() => {
    if (!summary) return null
    if (activeTab === 'consolidated') {
      return {
        invested: summary.total_invested_brl,
        current: summary.current_value_brl,
        gain: summary.gain_loss_brl,
        gainPct: summary.gain_loss_pct,
      }
    }
    if (activePortfolio) {
      return {
        invested: activePortfolio.total_invested,
        current: activePortfolio.current_value,
        gain: activePortfolio.gain_loss,
        gainPct: activePortfolio.gain_loss_pct,
      }
    }
    return null
  }, [summary, activeTab, activePortfolio])

  const canAddPortfolio = (summary?.portfolios.length ?? 0) < 2

  if (isLoading) {
    return (
      <div className="p-6 space-y-4">
        <Skeleton className="h-8 w-48" />
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          {[...Array(4)].map((_, i) => <Skeleton key={i} className="h-28" />)}
        </div>
        <Skeleton className="h-64" />
      </div>
    )
  }

  return (
    <div className="p-4 md:p-6 space-y-6 max-w-6xl mx-auto">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold">Investimentos</h1>
          {summary?.fx_usd_brl && (
            <p className="text-sm text-muted-foreground mt-0.5">
              USD/BRL: R$ {summary.fx_usd_brl.toFixed(4)}
            </p>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => refreshMut.mutate()}
            disabled={refreshMut.isPending}
          >
            <RefreshCw className={cn('h-4 w-4 mr-1.5', refreshMut.isPending && 'animate-spin')} />
            Atualizar cotações
          </Button>
          {canAddPortfolio && (
            <Button
              size="sm"
              onClick={() => setPortfolioDialog({ open: true, edit: null })}
            >
              <Plus className="h-4 w-4 mr-1.5" />
              Nova carteira
            </Button>
          )}
        </div>
      </div>

      {/* Portfolio Tabs */}
      {(summary?.portfolios.length ?? 0) > 0 && (
        <div className="flex items-center gap-1 border-b">
          <button
            onClick={() => setActiveTab('consolidated')}
            className={cn(
              'flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors',
              activeTab === 'consolidated'
                ? 'border-primary text-primary'
                : 'border-transparent text-muted-foreground hover:text-foreground',
            )}
          >
            <Layers className="h-4 w-4" />
            Consolidado
          </button>
          {summary?.portfolios.map((pf) => (
            <button
              key={pf.id}
              onClick={() => setActiveTab(pf.id)}
              className={cn(
                'flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors',
                activeTab === pf.id
                  ? 'border-primary text-primary'
                  : 'border-transparent text-muted-foreground hover:text-foreground',
              )}
            >
              <span
                className="h-2.5 w-2.5 rounded-full"
                style={{ backgroundColor: pf.color }}
              />
              {pf.name}
              {activeTab === pf.id && (
                <Pencil
                  className="h-3 w-3 ml-1 opacity-60 hover:opacity-100"
                  onClick={(e) => {
                    e.stopPropagation()
                    setPortfolioDialog({ open: true, edit: pf })
                  }}
                />
              )}
            </button>
          ))}
        </div>
      )}

      {/* Empty state — no portfolios */}
      {(summary?.portfolios.length ?? 0) === 0 && (
        <div className="flex flex-col items-center justify-center py-24 gap-4 text-center">
          <div className="h-16 w-16 rounded-2xl bg-primary/10 flex items-center justify-center">
            <Wallet className="h-8 w-8 text-primary" />
          </div>
          <div>
            <h2 className="text-lg font-semibold">Nenhuma carteira criada</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Crie até 2 carteiras para organizar seus investimentos
            </p>
          </div>
          <Button onClick={() => setPortfolioDialog({ open: true, edit: null })}>
            <Plus className="h-4 w-4 mr-1.5" />
            Criar carteira
          </Button>
        </div>
      )}

      {/* Summary Cards */}
      {displayStats && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <SummaryCard
            label="Patrimônio atual"
            value={fmt(displayStats.current, 'BRL', isPrivacyMode)}
            icon={Wallet}
          />
          <SummaryCard
            label="Total investido"
            value={fmt(displayStats.invested, 'BRL', isPrivacyMode)}
            icon={DollarSign}
          />
          <SummaryCard
            label="Resultado"
            value={fmt(displayStats.gain, 'BRL', isPrivacyMode)}
            sub={fmtPct(displayStats.gainPct, isPrivacyMode)}
            positive={displayStats.gain >= 0}
            icon={displayStats.gain >= 0 ? TrendingUp : TrendingDown}
          />
          <SummaryCard
            label="Posições"
            value={String(displayPositions.length)}
            icon={BarChart2}
          />
        </div>
      )}

      {/* Main grid */}
      {(summary?.portfolios.length ?? 0) > 0 && (
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Allocation */}
          <div className="rounded-xl border bg-card p-5 space-y-4">
            <h2 className="font-semibold">Alocação por tipo</h2>
            <AllocationChart allocation={displayAllocation} hideValues={isPrivacyMode} />
          </div>

          {/* Benchmark chart */}
          <div className="lg:col-span-2 rounded-xl border bg-card p-5 space-y-4">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <h2 className="font-semibold">Benchmarks (retorno acumulado)</h2>
              <div className="flex gap-1">
                {BENCHMARK_MONTHS.map((m) => (
                  <button
                    key={m}
                    onClick={() => setBenchmarkMonths(m)}
                    className={cn(
                      'text-xs px-2.5 py-1 rounded-md transition-colors',
                      benchmarkMonths === m
                        ? 'bg-primary text-primary-foreground'
                        : 'bg-muted text-muted-foreground hover:bg-muted/80',
                    )}
                  >
                    {m < 12 ? `${m}m` : `${m / 12}a`}
                  </button>
                ))}
              </div>
            </div>
            <BenchmarkChart months={benchmarkMonths} />
          </div>
        </div>
      )}

      {/* Positions table */}
      {(summary?.portfolios.length ?? 0) > 0 && (
        <div className="rounded-xl border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-4 border-b">
            <h2 className="font-semibold">Posições</h2>
            {activeTab !== 'consolidated' && (
              <div className="flex items-center gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  className="text-destructive border-destructive/30 hover:bg-destructive/10"
                  onClick={() =>
                    activePortfolio &&
                    setDeleteConfirm({
                      type: 'portfolio',
                      id: activePortfolio.id,
                      name: activePortfolio.name,
                    })
                  }
                >
                  <Trash2 className="h-3.5 w-3.5 mr-1.5" />
                  Excluir carteira
                </Button>
                <Button
                  size="sm"
                  onClick={() =>
                    setPositionDialog({
                      open: true,
                      portfolioId: activeTab,
                      edit: null,
                    })
                  }
                >
                  <Plus className="h-4 w-4 mr-1.5" />
                  Adicionar ativo
                </Button>
              </div>
            )}
          </div>

          {displayPositions.length === 0 ? (
            <div className="py-12 text-center text-muted-foreground text-sm">
              {activeTab === 'consolidated'
                ? 'Nenhuma posição em nenhuma carteira'
                : 'Nenhuma posição nesta carteira'}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/40">
                    <th className="text-left py-2.5 px-4 font-medium text-muted-foreground">Ativo</th>
                    <th className="text-left py-2.5 px-4 font-medium text-muted-foreground">Tipo</th>
                    <th className="text-right py-2.5 px-4 font-medium text-muted-foreground">Qtd</th>
                    <th className="text-right py-2.5 px-4 font-medium text-muted-foreground">Cotação</th>
                    <th className="text-right py-2.5 px-4 font-medium text-muted-foreground">Valor (R$)</th>
                    <th className="text-right py-2.5 px-4 font-medium text-muted-foreground">Resultado</th>
                    <th className="py-2.5 px-4" />
                  </tr>
                </thead>
                <tbody>
                  {displayPositions.map((pos) => (
                    <PositionRow
                      key={pos.id}
                      pos={pos}
                      hideValues={isPrivacyMode}
                      onEdit={(p) =>
                        setPositionDialog({
                          open: true,
                          portfolioId: p.portfolio_id,
                          edit: p,
                        })
                      }
                      onDelete={(p) =>
                        setDeleteConfirm({ type: 'position', id: p.id, name: p.name })
                      }
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}

      {/* Dialogs */}
      <PositionDialog
        open={positionDialog.open}
        onClose={() => setPositionDialog((s) => ({ ...s, open: false, edit: null }))}
        portfolioId={positionDialog.portfolioId}
        editPosition={positionDialog.edit}
      />

      <PortfolioDialog
        open={portfolioDialog.open}
        onClose={() => setPortfolioDialog({ open: false, edit: null })}
        editPortfolio={portfolioDialog.edit}
      />

      {/* Delete confirmation */}
      <Dialog open={!!deleteConfirm} onOpenChange={(o) => !o && setDeleteConfirm(null)}>
        <DialogContent className="max-w-sm">
          <DialogHeader>
            <DialogTitle>
              {deleteConfirm?.type === 'portfolio' ? 'Excluir carteira' : 'Remover posição'}
            </DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground py-2">
            {deleteConfirm?.type === 'portfolio'
              ? `Tem certeza que deseja excluir a carteira "${deleteConfirm?.name}"? Todas as posições serão removidas.`
              : `Tem certeza que deseja remover "${deleteConfirm?.name}" da carteira?`}
          </p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteConfirm(null)}>
              Cancelar
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                if (!deleteConfirm) return
                if (deleteConfirm.type === 'position') {
                  deletePositionMut.mutate(deleteConfirm.id)
                } else {
                  deletePortfolioMut.mutate(deleteConfirm.id)
                }
              }}
              disabled={deletePositionMut.isPending || deletePortfolioMut.isPending}
            >
              Excluir
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
