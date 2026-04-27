import { useState, useMemo, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { assets, assetGroups, currencies as currenciesApi, portfolioTimeseries } from '@/lib/api'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { DatePickerInput } from '@/components/ui/date-picker-input'
import type { Asset, AssetGroup, MarketSymbolMatch, MarketSymbolQuote } from '@/types'
import { ASSET_CLASS_OPTIONS } from '@/types'
import {
  Home,
  Car,
  Gem,
  TrendingUp,
  Package,
  Plus,
  Pencil,
  Trash2,
  ChevronDown,
  ChevronUp,
  ChevronRight,
  RefreshCw,
  Wallet,
  FolderInput,
  LineChart,
  Layers,
  Bitcoin,
  PieChart,
} from 'lucide-react'
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  CartesianGrid,
} from 'recharts'
import { PageHeader } from '@/components/page-header'
import { usePrivacyMode } from '@/hooks/use-privacy-mode'
import { useAuth } from '@/contexts/auth-context'

function formatCurrency(value: number, currency = 'USD', locale = 'en-US') {
  try {
    return new Intl.NumberFormat(locale, { style: 'currency', currency: currency || 'USD' }).format(value)
  } catch {
    return new Intl.NumberFormat(locale, { style: 'currency', currency: 'USD' }).format(value)
  }
}

// Renders a logo image when one is available, falling back to the asset's
// type-based Lucide icon on missing URL or broken image. Uses the type's
// bg color as a tinted placeholder; switches to a white card + border when
// showing a real logo so brand colors don't clash with our palette.
function AssetIcon({
  logoUrl,
  Icon,
  colorClass,
  bgClass,
  size = 20,
  tile = 'w-10 h-10',
}: {
  logoUrl: string | null | undefined
  Icon: React.ElementType
  colorClass: string
  bgClass: string
  size?: number
  tile?: string
}) {
  const [errored, setErrored] = useState(false)
  const showImage = !!logoUrl && !errored
  return (
    <div
      className={`${tile} rounded-lg flex items-center justify-center overflow-hidden shrink-0 ${
        showImage ? 'bg-white border border-border' : bgClass
      }`}
    >
      {showImage ? (
        <img
          src={logoUrl!}
          alt=""
          className="w-full h-full object-contain"
          onError={() => setErrored(true)}
        />
      ) : (
        <Icon size={size} className={colorClass} />
      )}
    </div>
  )
}

// Compact relative-time formatter ("2h ago" / "há 2h"). Used for the price
// preview "last updated" hint. Intl.RelativeTimeFormat handles the locale
// grammar so we don't hand-roll plurals. Falls back to absolute date only
// when the input is missing — otherwise always returns a relative string.
function formatRelativeTime(dateInput: string | null | undefined, locale: string): string | null {
  if (!dateInput) return null
  const then = new Date(dateInput).getTime()
  if (Number.isNaN(then)) return null
  const diffSec = (then - Date.now()) / 1000
  const absSec = Math.abs(diffSec)
  const rtf = new Intl.RelativeTimeFormat(locale, { numeric: 'auto' })
  if (absSec < 60) return rtf.format(Math.round(diffSec), 'second')
  if (absSec < 3600) return rtf.format(Math.round(diffSec / 60), 'minute')
  if (absSec < 86400) return rtf.format(Math.round(diffSec / 3600), 'hour')
  return rtf.format(Math.round(diffSec / 86400), 'day')
}

const ASSET_TYPE_CONFIG: Record<string, { icon: React.ElementType; color: string; bg: string }> = {
  real_estate: { icon: Home, color: 'text-blue-600', bg: 'bg-blue-100' },
  vehicle: { icon: Car, color: 'text-violet-600', bg: 'bg-violet-100' },
  valuable: { icon: Gem, color: 'text-amber-600', bg: 'bg-amber-100' },
  investment: { icon: TrendingUp, color: 'text-emerald-600', bg: 'bg-emerald-100' },
  stock: { icon: LineChart, color: 'text-sky-600', bg: 'bg-sky-100' },
  etf: { icon: Layers, color: 'text-teal-600', bg: 'bg-teal-100' },
  crypto: { icon: Bitcoin, color: 'text-orange-600', bg: 'bg-orange-100' },
  fund: { icon: PieChart, color: 'text-indigo-600', bg: 'bg-indigo-100' },
  other: { icon: Package, color: 'text-slate-600', bg: 'bg-slate-100' },
}

function getTypeConfig(type: string) {
  return ASSET_TYPE_CONFIG[type] ?? ASSET_TYPE_CONFIG['other']
}

const ASSET_TYPES = [
  'stock',
  'etf',
  'crypto',
  'fund',
  'real_estate',
  'vehicle',
  'valuable',
  'investment',
  'other',
] as const

// Map a yfinance `quoteType` to Securo's asset type. Lives here (not the
// backend) so if we ever swap the market-price provider the service stays
// clean — all provider-specific vocabulary is translated at the edge.
function assetTypeFromQuoteType(quoteType: string | null | undefined): string {
  switch ((quoteType || '').toUpperCase()) {
    case 'EQUITY':
      return 'stock'
    case 'ETF':
      return 'etf'
    case 'CRYPTOCURRENCY':
      return 'crypto'
    case 'MUTUALFUND':
    case 'INDEX':
      return 'fund'
    default:
      return 'investment'
  }
}
const VALUATION_METHODS = ['manual', 'growth_rule', 'market_price'] as const
const GROWTH_TYPES = ['percentage', 'absolute'] as const
const GROWTH_FREQUENCIES = ['daily', 'weekly', 'monthly', 'yearly'] as const

export default function AssetsPage() {
  const { t, i18n } = useTranslation()
  const locale = i18n.language === 'en' ? 'en-US' : i18n.language
  const { mask } = usePrivacyMode()
  const { user } = useAuth()
  const userCurrency = user?.preferences?.currency_display ?? 'USD'
  const queryClient = useQueryClient()

  const { data: supportedCurrencies } = useQuery({
    queryKey: ['currencies'],
    queryFn: currenciesApi.list,
    staleTime: Infinity,
  })

  const [dialogOpen, setDialogOpen] = useState(false)
  const [editingAsset, setEditingAsset] = useState<Asset | null>(null)
  const [deletingId, setDeletingId] = useState<string | null>(null)
  const [pendingGrowthSave, setPendingGrowthSave] = useState<Record<string, unknown> | null>(null)
  const [expandedId, setExpandedId] = useState<string | null>(null)
  // Period selector for KPIs at the top (30d / 6M / 1A / Tudo)
  const [kpiPeriod, setKpiPeriod] = useState<{ label: string; months: number; sinceStart: boolean }>(
    { label: '1A', months: 12, sinceStart: false }
  )

  // Wallet (AssetGroup) dialog state
  const [walletDialogOpen, setWalletDialogOpen] = useState(false)
  const [editingWallet, setEditingWallet] = useState<AssetGroup | null>(null)
  const [walletFormName, setWalletFormName] = useState('')
  const [walletFormColor, setWalletFormColor] = useState('#0EA5E9')
  const [deletingWalletId, setDeletingWalletId] = useState<string | null>(null)
  // Collapsed wallet IDs — default is expanded (empty set), user can collapse manually
  const [collapsedWallets, setCollapsedWallets] = useState<Set<string>>(new Set())
  // Asset being moved to a wallet (null = no picker open)
  const [movingAsset, setMovingAsset] = useState<Asset | null>(null)

  // Form state
  const [formName, setFormName] = useState('')
  const [formType, setFormType] = useState<string>('other')
  const [formCurrency, setFormCurrency] = useState(userCurrency)
  const [formMethod, setFormMethod] = useState<string>('manual')
  const [formAssetClass, setFormAssetClass] = useState<string>('')  // empty = unset
  const [formPurchaseDate, setFormPurchaseDate] = useState<string>('')
  const [formPurchasePrice, setFormPurchasePrice] = useState('')
  const [formSellDate, setFormSellDate] = useState<string>('')
  const [formSellPrice, setFormSellPrice] = useState('')
  const [formCurrentValue, setFormCurrentValue] = useState('')
  const [formGrowthType, setFormGrowthType] = useState<string>('percentage')
  const [formGrowthRate, setFormGrowthRate] = useState('')
  const [formGrowthFrequency, setFormGrowthFrequency] = useState<string>('monthly')
  const [formGrowthStartDate, setFormGrowthStartDate] = useState<string>('')
  // Market-price form state
  const [formTickerQuery, setFormTickerQuery] = useState('')
  const [tickerMatches, setTickerMatches] = useState<MarketSymbolMatch[]>([])
  const [tickerSearchLoading, setTickerSearchLoading] = useState(false)
  const [selectedQuote, setSelectedQuote] = useState<MarketSymbolQuote | null>(null)
  const [formUnits, setFormUnits] = useState('')
  const [quoteLoading, setQuoteLoading] = useState(false)

  const { data: assetsList, isLoading } = useQuery({
    queryKey: ['assets'],
    queryFn: () => assets.list(true),
  })

  const { data: portfolioData } = useQuery({
    queryKey: ['portfolio-trend'],
    queryFn: () => assets.portfolioTrend(),
  })

  // `refetchQueries` (vs. `invalidateQueries`) forces an immediate refetch
  // regardless of stale-state heuristics. Our global staleTime of 5 min
  // combined with the dialog-close re-render was sometimes leaving the
  // asset list showing pre-edit data until the user manually reloaded.
  function refetchAssetViews() {
    queryClient.refetchQueries({ queryKey: ['assets'] })
    queryClient.refetchQueries({ queryKey: ['portfolio-trend'] })
    queryClient.refetchQueries({ queryKey: ['dashboard'] })
  }

  const createMutation = useMutation({
    mutationFn: (data: Parameters<typeof assets.create>[0]) => assets.create(data),
    onSuccess: () => {
      refetchAssetViews()
      setDialogOpen(false)
      toast.success(t('assets.created'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const updateMutation = useMutation({
    mutationFn: ({ id, _regenerateGrowth, ...data }: Partial<Asset> & { id: string; _regenerateGrowth?: boolean }) =>
      assets.update(id, data, { regenerateGrowth: _regenerateGrowth }),
    onSuccess: () => {
      refetchAssetViews()
      setDialogOpen(false)
      setEditingAsset(null)
      toast.success(t('assets.updated'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => assets.delete(id),
    onSuccess: () => {
      refetchAssetViews()
      setDeletingId(null)
      if (expandedId === deletingId) setExpandedId(null)
      toast.success(t('assets.deleted'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const refreshPriceMutation = useMutation({
    mutationFn: (id: string) => assets.refreshPrice(id),
    onSuccess: (updated) => {
      // Sync the dialog's preview to the fresh quote so the user sees the
      // new price without closing the dialog. The list + chart refetch
      // via our standard helper.
      setSelectedQuote({
        symbol: updated.ticker || '',
        name: updated.name,
        exchange: updated.ticker_exchange,
        currency: updated.currency,
        price: updated.last_price ?? 0,
        quote_type: null,
      })
      setEditingAsset(updated)
      refetchAssetViews()
      toast.success(t('assets.priceRefreshed'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const { data: walletsList } = useQuery({
    queryKey: ['asset-groups'],
    queryFn: () => assetGroups.list(),
  })

  const createWalletMutation = useMutation({
    mutationFn: (data: { name: string; color: string }) =>
      assetGroups.create({ name: data.name, color: data.color, icon: 'wallet' }),
    onSuccess: () => {
      queryClient.refetchQueries({ queryKey: ['asset-groups'] })
      setWalletDialogOpen(false)
      setEditingWallet(null)
      toast.success(t('assets.walletCreated'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const updateWalletMutation = useMutation({
    mutationFn: ({ id, ...data }: { id: string; name: string; color: string }) =>
      assetGroups.update(id, { name: data.name, color: data.color }),
    onSuccess: () => {
      queryClient.refetchQueries({ queryKey: ['asset-groups'] })
      setWalletDialogOpen(false)
      setEditingWallet(null)
      toast.success(t('assets.walletUpdated'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const deleteWalletMutation = useMutation({
    mutationFn: (id: string) => assetGroups.delete(id),
    onSuccess: () => {
      // Deleting a wallet un-groups its assets (backend sets group_id=null).
      queryClient.refetchQueries({ queryKey: ['asset-groups'] })
      queryClient.refetchQueries({ queryKey: ['assets'] })
      setDeletingWalletId(null)
      toast.success(t('assets.walletDeleted'))
    },
    onError: () => toast.error(t('common.error')),
  })

  const moveAssetMutation = useMutation({
    mutationFn: ({ id, groupId }: { id: string; groupId: string | null }) =>
      assets.update(id, { group_id: groupId } as Partial<Asset>),
    onSuccess: () => {
      queryClient.refetchQueries({ queryKey: ['assets'] })
      queryClient.refetchQueries({ queryKey: ['asset-groups'] })
      setMovingAsset(null)
      toast.success(t('assets.moved'))
    },
    onError: () => toast.error(t('common.error')),
  })

  // Compute projected current value for growth_rule preview in the form
  const projectedGrowthValue = useMemo(() => {
    if (formMethod !== 'growth_rule') return null
    const baseAmount = parseFloat(formPurchasePrice)
    const rate = parseFloat(formGrowthRate)
    if (!baseAmount || !rate || !formGrowthFrequency) return null

    const startDate = formGrowthStartDate || formPurchaseDate
    if (!startDate) return null

    const today = new Date()
    today.setHours(0, 0, 0, 0)
    let current = baseAmount
    let d = new Date(startDate + 'T00:00:00')

    let iterations = 0
    while (iterations < 10000) {
      const next = new Date(d)
      if (formGrowthFrequency === 'daily') next.setDate(next.getDate() + 1)
      else if (formGrowthFrequency === 'weekly') next.setDate(next.getDate() + 7)
      else if (formGrowthFrequency === 'monthly') next.setMonth(next.getMonth() + 1)
      else if (formGrowthFrequency === 'yearly') next.setFullYear(next.getFullYear() + 1)
      else break
      if (next > today) break
      if (formGrowthType === 'percentage') {
        current = current * (1 + rate / 100)
      } else {
        current = current + rate
      }
      d = next
      iterations++
    }
    return Math.round(current * 100) / 100
  }, [formMethod, formPurchasePrice, formGrowthRate, formGrowthType, formGrowthFrequency, formGrowthStartDate, formPurchaseDate])

  const activeAssets = assetsList?.filter(a => !a.sell_date && !a.is_archived) ?? []
  const soldAssets = assetsList?.filter(a => a.sell_date) ?? []

  // Debounced ticker search. Runs only when the market-price method is
  // selected and the query is non-trivial — keeps the autocomplete snappy
  // without flooding the yfinance-backed endpoint.
  useEffect(() => {
    if (formMethod !== 'market_price') return
    const q = formTickerQuery.trim()
    // Don't search if the field matches the already-selected quote — the
    // user just picked it and we'd spam the endpoint for no reason.
    if (selectedQuote && q === selectedQuote.symbol) return
    if (q.length < 1) {
      setTickerMatches([])
      return
    }
    setTickerSearchLoading(true)
    const handle = window.setTimeout(async () => {
      try {
        const results = await assets.marketSearch(q, 10)
        setTickerMatches(results)
      } catch {
        setTickerMatches([])
      } finally {
        setTickerSearchLoading(false)
      }
    }, 300)
    return () => window.clearTimeout(handle)
  }, [formMethod, formTickerQuery, selectedQuote])

  async function pickTickerMatch(match: MarketSymbolMatch) {
    setTickerMatches([])
    setFormTickerQuery(match.symbol)
    setQuoteLoading(true)
    try {
      const quote = await assets.marketQuote(match.symbol)
      setSelectedQuote(quote)
      // Auto-fill name/currency from the authoritative quote so the user
      // doesn't have to think about it — they can still edit name after.
      if (!formName || formName === (selectedQuote?.name ?? selectedQuote?.symbol ?? '')) {
        setFormName(quote.name || quote.symbol)
      }
      setFormCurrency(quote.currency)
      // Classify the asset from the quote type (EQUITY → stock, etc.) so
      // the Tipo dropdown lands on something meaningful by default. We
      // skip this when the user already picked a non-default type, so
      // manual overrides stick.
      const suggestedType = assetTypeFromQuoteType(quote.quote_type)
      if (formType === 'other' || formType === 'investment') {
        setFormType(suggestedType)
      }
    } catch {
      toast.error(t('common.error'))
      setSelectedQuote(null)
    } finally {
      setQuoteLoading(false)
    }
  }

  function resetMarketPriceForm() {
    setFormTickerQuery('')
    setTickerMatches([])
    setSelectedQuote(null)
    setFormUnits('')
    setQuoteLoading(false)
    setTickerSearchLoading(false)
  }

  function openCreate() {
    setEditingAsset(null)
    setFormName('')
    setFormType('other')
    setFormCurrency(userCurrency)
    setFormMethod('manual')
    setFormAssetClass('')
    setFormPurchaseDate('')
    setFormPurchasePrice('')
    setFormSellDate('')
    setFormSellPrice('')
    setFormCurrentValue('')
    setFormGrowthType('percentage')
    setFormGrowthRate('')
    setFormGrowthFrequency('monthly')
    setFormGrowthStartDate('')
    resetMarketPriceForm()
    setDialogOpen(true)
  }

  function openEdit(asset: Asset) {
    setEditingAsset(asset)
    setFormName(asset.name)
    setFormType(asset.type)
    setFormCurrency(asset.currency)
    setFormMethod(asset.valuation_method)
    setFormAssetClass(asset.asset_class ?? '')
    setFormPurchaseDate(asset.purchase_date ?? '')
    setFormPurchasePrice(asset.purchase_price?.toString() ?? '')
    setFormSellDate(asset.sell_date ?? '')
    setFormSellPrice(asset.sell_price?.toString() ?? '')
    setFormCurrentValue('')
    setFormGrowthType(asset.growth_type ?? 'percentage')
    setFormGrowthRate(asset.growth_rate?.toString() ?? '')
    setFormGrowthFrequency(asset.growth_frequency ?? 'monthly')
    setFormGrowthStartDate(asset.growth_start_date ?? '')
    resetMarketPriceForm()
    if (asset.valuation_method === 'market_price' && asset.ticker) {
      setFormTickerQuery(asset.ticker)
      setFormUnits(asset.units?.toString() ?? '')
      // Synthesize a quote from the cached fields so the preview shows
      // immediately — we skip a round-trip to yfinance on edit open.
      if (asset.last_price != null) {
        setSelectedQuote({
          symbol: asset.ticker,
          name: asset.name,
          exchange: asset.ticker_exchange,
          currency: asset.currency,
          price: asset.last_price,
          quote_type: null,
        })
      }
    }
    setDialogOpen(true)
  }

  function buildPayload() {
    const payload: Record<string, unknown> = {
      name: formName,
      type: formType,
      currency: formCurrency,
      valuation_method: formMethod,
      purchase_date: formPurchaseDate || null,
      purchase_price: formPurchasePrice ? parseFloat(formPurchasePrice) : null,
      sell_date: formSellDate || null,
      sell_price: formSellPrice ? parseFloat(formSellPrice) : null,
    }

    if (formMethod === 'growth_rule') {
      payload.growth_type = formGrowthType
      payload.growth_rate = formGrowthRate ? parseFloat(formGrowthRate) : null
      payload.growth_frequency = formGrowthFrequency
      payload.growth_start_date = formGrowthStartDate || null
    }

    if (formMethod === 'market_price') {
      payload.ticker = (selectedQuote?.symbol || formTickerQuery || '').toUpperCase()
      payload.ticker_exchange = selectedQuote?.exchange ?? null
      payload.units = formUnits ? parseFloat(formUnits) : null
    }

    if (!editingAsset && formCurrentValue) {
      payload.current_value = parseFloat(formCurrentValue)
    }

    if (formAssetClass) {
      payload.asset_class = formAssetClass
    }

    return payload
  }

  function hasGrowthParamsChanged(): boolean {
    if (!editingAsset || editingAsset.valuation_method !== 'growth_rule') return false
    return (
      formGrowthType !== (editingAsset.growth_type ?? 'percentage') ||
      formGrowthRate !== (editingAsset.growth_rate?.toString() ?? '') ||
      formGrowthFrequency !== (editingAsset.growth_frequency ?? 'monthly') ||
      formGrowthStartDate !== (editingAsset.growth_start_date ?? '') ||
      formPurchasePrice !== (editingAsset.purchase_price?.toString() ?? '') ||
      formPurchaseDate !== (editingAsset.purchase_date ?? '')
    )
  }

  function handleSave() {
    const payload = buildPayload()

    if (editingAsset) {
      // If growth params changed, ask confirmation before regenerating
      if (hasGrowthParamsChanged() && editingAsset.value_count > 0) {
        setPendingGrowthSave(payload)
        return
      }
      updateMutation.mutate({ id: editingAsset.id, ...payload } as Partial<Asset> & { id: string })
    } else {
      createMutation.mutate(payload as Parameters<typeof assets.create>[0])
    }
  }

  function confirmRegenerateGrowth() {
    if (!editingAsset || !pendingGrowthSave) return
    updateMutation.mutate(
      { id: editingAsset.id, ...pendingGrowthSave, _regenerateGrowth: true } as Partial<Asset> & { id: string },
    )
    setPendingGrowthSave(null)
  }

  // ----- Subgroup helpers (Ações / ETFs / FIIs / Renda Fixa / Stocks US / ...)
  // Used to give each wallet section a 2nd-level breakdown by asset_class.

  // Hard-coded mapping in the absence of an explicit asset_class on every
  // record: tickers ending in 11 NOT in the known-ETF list are FIIs.
  const KNOWN_BR_ETFS = new Set([
    'IVVB11', 'BOVA11', 'SMAL11', 'SPXI11', 'HASH11', 'GOLD11',
    'NTNB11', 'IRFM11', 'DIVO11', 'FIND11', 'GOVE11', 'MATB11',
    'BOVB11', 'BOVS11', 'BOVV11', 'ECOO11', 'ISUS11', 'PIBB11',
  ])

  function inferDisplayClass(asset: Asset): string {
    if (asset.asset_class === 'FIIS') return 'FIIs'
    if (asset.asset_class === 'STOCKS_US') return 'Stocks (Ações Americanas)'
    if (asset.asset_class === 'CRIPTO') return 'Criptomoedas'
    if (asset.asset_class === 'RENDA_FIXA') return 'Renda Fixa'
    if (asset.asset_class === 'OUTRO') return 'Outro'
    if (asset.asset_class === 'RENDA_VARIAVEL_BR') {
      const tk = (asset.ticker || asset.name).replace('.SA', '').toUpperCase()
      if (KNOWN_BR_ETFS.has(tk)) return 'ETFs'
      if (/11$/.test(tk)) return 'FIIs'
      return 'Ações'
    }
    return 'Outro'
  }

  // Order in which subgroups should appear inside a wallet section.
  const SUBGROUP_ORDER = [
    'Ações', 'ETFs', 'FIIs', 'Stocks (Ações Americanas)',
    'Renda Fixa', 'Criptomoedas', 'Outro',
  ]

  function groupByDisplayClass(list: Asset[]): { name: string; assets: Asset[] }[] {
    const buckets = new Map<string, Asset[]>()
    for (const a of list) {
      const key = inferDisplayClass(a)
      if (!buckets.has(key)) buckets.set(key, [])
      buckets.get(key)!.push(a)
    }
    return SUBGROUP_ORDER
      .filter(k => buckets.has(k))
      .map(k => ({ name: k, assets: buckets.get(k)! }))
      .concat(
        Array.from(buckets.keys())
          .filter(k => !SUBGROUP_ORDER.includes(k))
          .map(k => ({ name: k, assets: buckets.get(k)! }))
      )
  }

  // Total active value across all wallets — used for "% do portfólio".
  // Uses current_value_primary when present (already FX-converted) so the
  // ratio is consistent across currencies.
  const totalPortfolioValue = useMemo(() => {
    return (assetsList ?? []).reduce((sum: number, a: Asset) => {
      if (a.is_archived || a.sell_date) return sum
      const v = a.current_value_primary ?? a.current_value ?? 0
      return sum + (typeof v === 'number' ? v : 0)
    }, 0)
  }, [assetsList])

  // Portfolio timeseries for the KPI bar (Resultado período).
  const { data: kpiSeries } = useQuery({
    queryKey: ['portfolio-ts-kpi', kpiPeriod.months, kpiPeriod.sinceStart],
    queryFn: () => portfolioTimeseries.series({
      months: kpiPeriod.months,
      sinceStart: kpiPeriod.sinceStart,
    }),
    staleTime: 1000 * 60,
  })

  // TWR per asset for the Rent. TWR column. ONE call returns the map.
  const { data: twrByAsset } = useQuery({
    queryKey: ['portfolio-twr-by-asset', kpiPeriod.months, kpiPeriod.sinceStart],
    queryFn: () => portfolioTimeseries.twrByAsset(
      kpiPeriod.months, kpiPeriod.sinceStart),
    staleTime: 1000 * 60,
  })

  const kpiResult = useMemo(() => {
    if (!kpiSeries || kpiSeries.length < 2) return null
    const first = kpiSeries[0]
    const last = kpiSeries[kpiSeries.length - 1]
    const baseFactor = 1 + (first.twr_cum ?? 0)
    const lastFactor = 1 + (last.twr_cum ?? 0)
    const twrPeriod = lastFactor / baseFactor - 1
    return {
      v_end: last.v_end ?? 0,
      v_start: first.v_end ?? 0,
      delta: (last.v_end ?? 0) - (first.v_end ?? 0),
      twr_period: twrPeriod,
      first_month: first.month,
      last_month: last.month,
    }
  }, [kpiSeries])

  function renderAssetCard(asset: Asset) {
    const config = getTypeConfig(asset.type)
    const Icon = config.icon
    const isExpanded = expandedId === asset.id
    const isSynced = asset.source !== 'manual'
    // Split "externally-owned" (bank/brokerage record — gets overwritten on
    // re-sync, so read-only for users) from "market-priced" (user-created
    // record where only the cached price syncs). We key on valuation_method
    // rather than the concrete source string so swapping the price provider
    // (yfinance → anything else) doesn't break this logic.
    const isMarketPriced = asset.valuation_method === 'market_price'
    const isProviderOwned = isSynced && !isMarketPriced

    return (
      <div key={asset.id} className="border border-border rounded-xl bg-card shadow-sm overflow-hidden">
        <div
          className="flex items-center gap-4 px-5 py-4 cursor-pointer hover:bg-muted/30 transition-colors"
          onClick={() => setExpandedId(isExpanded ? null : asset.id)}
        >
          <AssetIcon
            logoUrl={asset.logo_url}
            Icon={Icon}
            colorClass={config.color}
            bgClass={config.bg}
          />
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold text-foreground truncate">{asset.name}</span>
              <Badge variant="secondary" className="text-[10px] px-1.5 py-0">
                {t(`assets.type${asset.type.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase()).replace(/^./, c => c.toUpperCase())}`)}
              </Badge>
              {isMarketPriced ? (
                <Badge
                  variant="outline"
                  className="text-[10px] px-1.5 py-0 text-primary border-primary/30 gap-1"
                  title={t('assets.marketPriceSourceTooltip')}
                >
                  <TrendingUp size={9} />
                  {t('assets.marketPriceSource')}
                </Badge>
              ) : isSynced ? (
                <Badge
                  variant="outline"
                  className="text-[10px] px-1.5 py-0 text-sky-600 border-sky-200 gap-1"
                  title={t('assets.syncedFrom', { source: asset.source })}
                >
                  <RefreshCw size={9} />
                  {t('assets.synced')}
                </Badge>
              ) : null}
              {asset.maturity_date && (
                <Badge variant="outline" className="text-[10px] px-1.5 py-0 text-muted-foreground">
                  {t('assets.maturesOn', { date: new Date(asset.maturity_date).toLocaleDateString(locale) })}
                </Badge>
              )}
              {asset.valuation_method === 'growth_rule' && asset.growth_rate && (
                <Badge variant="outline" className="text-[10px] px-1.5 py-0 text-emerald-600 border-emerald-200">
                  +{asset.growth_type === 'percentage' ? `${asset.growth_rate}%` : formatCurrency(asset.growth_rate, asset.currency, locale)}
                  /{t(`assets.${asset.growth_frequency}`).toLowerCase().charAt(0)}
                </Badge>
              )}
              {asset.sell_date && (
                <Badge variant="outline" className="text-[10px] px-1.5 py-0 text-rose-600 border-rose-200">
                  {t('assets.sold')}
                </Badge>
              )}
            </div>
          </div>
          {/* Rich columns inline: Qtd | Invested | Custodiante */}
          <div className="hidden md:flex items-center gap-4 shrink-0 tabular-nums">
            <div className="text-right min-w-[64px]">
              <p className="text-[10px] uppercase text-muted-foreground tracking-wider">Qtd</p>
              <p className="text-xs font-semibold text-foreground">
                {asset.units != null
                  ? asset.units.toLocaleString(locale, { maximumFractionDigits: 4 })
                  : '—'}
              </p>
            </div>
            <div className="text-right min-w-[100px]">
              <p className="text-[10px] uppercase text-muted-foreground tracking-wider">Investido</p>
              <p className="text-xs font-semibold text-foreground">
                {asset.purchase_price != null && asset.units != null
                  ? mask(formatCurrency(asset.purchase_price * asset.units, asset.currency, locale))
                  : '—'}
              </p>
            </div>
            <div className="text-right min-w-[120px] max-w-[160px] truncate">
              <p className="text-[10px] uppercase text-muted-foreground tracking-wider">Custodiante</p>
              <p className="text-xs font-medium text-muted-foreground truncate" title={asset.custodian ?? ''}>
                {asset.custodian || '—'}
              </p>
            </div>
          </div>
          {/* Rent. TWR column — Modified Dietz over the selected period.
              Source: bulk endpoint /api/portfolio/timeseries/by-asset.
              When the period is shorter than the asset's first cashflow,
              TWR is approximate (Modified Dietz still works but the
              denominator is small). */}
          <div className="hidden lg:block text-right shrink-0 min-w-[80px]">
            <p className="text-[10px] uppercase text-muted-foreground tracking-wider">Rent. TWR</p>
            {(() => {
              const t = twrByAsset?.[asset.id]
              if (!t) return <p className="text-xs text-muted-foreground">—</p>
              const v = t.twr_cum
              return (
                <p className={`text-xs font-semibold tabular-nums ${
                  v >= 0 ? 'text-emerald-600' : 'text-rose-500'
                }`}>
                  {mask(`${v >= 0 ? '+' : ''}${(v * 100).toFixed(2)}%`)}
                </p>
              )
            })()}
          </div>
          <div className="text-right shrink-0 min-w-[110px]">
            {asset.current_value != null ? (
              <>
                <p className="text-sm font-bold tabular-nums text-foreground">
                  {mask(formatCurrency(asset.current_value, asset.currency, locale))}
                </p>
                {asset.current_value_primary != null && asset.currency !== userCurrency && (
                  <p className="text-[10px] text-muted-foreground tabular-nums">
                    ≈ {mask(formatCurrency(asset.current_value_primary, userCurrency, locale))}
                  </p>
                )}
                {asset.gain_loss != null && (() => {
                  const invested = (asset.purchase_price ?? 0) * (asset.units ?? 0)
                  const pct = invested > 0 ? (asset.gain_loss / invested) * 100 : null
                  return (
                    <p className={`text-xs font-medium tabular-nums ${asset.gain_loss >= 0 ? 'text-emerald-600' : 'text-rose-500'}`}>
                      {mask(`${asset.gain_loss >= 0 ? '+' : ''}${formatCurrency(asset.gain_loss, asset.currency, locale)}`)}
                      {pct != null && (
                        <span className="text-[10px] ml-1">({pct >= 0 ? '+' : ''}{pct.toFixed(2)}% P&L)</span>
                      )}
                    </p>
                  )
                })()}
                {totalPortfolioValue > 0 && (asset.current_value_primary ?? asset.current_value ?? 0) > 0 && (() => {
                  const v = asset.current_value_primary ?? asset.current_value ?? 0
                  const pct = (v / totalPortfolioValue) * 100
                  return (
                    <p className="text-[10px] text-muted-foreground tabular-nums">
                      {pct.toFixed(2)}% do portfólio
                    </p>
                  )
                })()}
              </>
            ) : (
              <p className="text-sm text-muted-foreground">—</p>
            )}
          </div>
          <div className="flex items-center gap-1 shrink-0">
            <button
              onClick={(e) => { e.stopPropagation(); setMovingAsset(asset) }}
              title={t('assets.moveToWallet')}
              className="p-1.5 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
            >
              <FolderInput size={14} />
            </button>
            <button
              onClick={(e) => { e.stopPropagation(); if (!isProviderOwned) openEdit(asset) }}
              disabled={isProviderOwned}
              title={isProviderOwned ? t('assets.syncedReadOnly') : undefined}
              className="p-1.5 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent"
            >
              <Pencil size={14} />
            </button>
            <button
              onClick={(e) => { e.stopPropagation(); if (!isProviderOwned) setDeletingId(asset.id) }}
              disabled={isProviderOwned}
              title={isProviderOwned ? t('assets.syncedReadOnly') : undefined}
              className="p-1.5 rounded-lg text-muted-foreground hover:text-rose-600 hover:bg-rose-50 transition-colors disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-transparent"
            >
              <Trash2 size={14} />
            </button>
            {isExpanded ? <ChevronUp size={16} className="text-muted-foreground" /> : <ChevronDown size={16} className="text-muted-foreground" />}
          </div>
        </div>

        {isExpanded && <AssetDetail assetId={asset.id} currency={asset.currency} locale={locale} purchasePrice={asset.purchase_price} purchaseDate={asset.purchase_date} valuationMethod={asset.valuation_method} />}
      </div>
    )
  }

  // Bucket active assets by group_id so each wallet renders with its
  // total and collapse toggle. Un-grouped actives go under a synthetic
  // bucket rendered at the end.
  const assetsByGroup = useMemo(() => {
    const map = new Map<string | null, Asset[]>()
    for (const a of activeAssets) {
      const key = a.group_id ?? null
      if (!map.has(key)) map.set(key, [])
      map.get(key)!.push(a)
    }
    return map
  }, [activeAssets])

  const sortedWallets = useMemo(() => {
    return (walletsList ?? []).slice().sort((a, b) => a.position - b.position || a.name.localeCompare(b.name))
  }, [walletsList])

  const ungroupedAssets = assetsByGroup.get(null) ?? []

  function toggleWalletCollapse(id: string) {
    setCollapsedWallets(prev => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function openCreateWallet() {
    setEditingWallet(null)
    setWalletFormName('')
    setWalletFormColor('#0EA5E9')
    setWalletDialogOpen(true)
  }

  function openEditWallet(wallet: AssetGroup) {
    setEditingWallet(wallet)
    setWalletFormName(wallet.name)
    setWalletFormColor(wallet.color)
    setWalletDialogOpen(true)
  }

  function handleSaveWallet() {
    const name = walletFormName.trim()
    if (!name) return
    if (editingWallet) {
      updateWalletMutation.mutate({ id: editingWallet.id, name, color: walletFormColor })
    } else {
      createWalletMutation.mutate({ name, color: walletFormColor })
    }
  }

  function renderWalletSection(wallet: AssetGroup, walletAssets: Asset[]) {
    const isCollapsed = collapsedWallets.has(wallet.id)
    const isSynced = wallet.source !== 'manual'
    // Sum in wallet's reported current_value (already computed by backend).
    // Fall back to per-asset sum if the rollup is stale after a move.
    const total = walletAssets.reduce((s, a) => s + (a.current_value_primary ?? a.current_value ?? 0), 0) || wallet.current_value_primary || wallet.current_value

    // Only show the institution as a subtitle when it's actually
    // additional information — if the user hasn't renamed the wallet,
    // name and institution are identical and the subtitle would be
    // redundant noise.
    const showInstitutionSubtitle =
      !!wallet.institution_name && wallet.institution_name !== wallet.name

    return (
      <div key={wallet.id} className="space-y-2">
        <div className="flex items-center gap-3 px-1">
          <button
            onClick={() => toggleWalletCollapse(wallet.id)}
            className="flex items-center gap-2 flex-1 min-w-0 group"
          >
            {isCollapsed ? (
              <ChevronRight size={14} className="text-muted-foreground" />
            ) : (
              <ChevronDown size={14} className="text-muted-foreground" />
            )}
            <div
              className="w-6 h-6 rounded-md flex items-center justify-center shrink-0"
              style={{ backgroundColor: `${wallet.color}20` }}
            >
              <Wallet size={13} style={{ color: wallet.color }} />
            </div>
            <div className="flex flex-col items-start min-w-0 flex-1">
              <div className="flex items-center gap-2 min-w-0 w-full">
                <span className="text-sm font-semibold text-foreground truncate">{wallet.name}</span>
                <span className="text-xs text-muted-foreground shrink-0">
                  · {walletAssets.length} {t('assets.itemsCount')}
                </span>
              </div>
              {showInstitutionSubtitle && (
                <span className="text-[11px] text-muted-foreground truncate flex items-center gap-1">
                  <RefreshCw size={9} />
                  {t('assets.syncedFrom', { source: wallet.institution_name })}
                </span>
              )}
            </div>
          </button>
          <span className="text-sm font-bold tabular-nums text-foreground shrink-0">
            {mask(formatCurrency(total, userCurrency, locale))}
          </span>
          <button
            onClick={() => openEditWallet(wallet)}
            className="p-1 rounded-lg text-muted-foreground hover:text-foreground hover:bg-muted transition-colors"
            title={t('assets.editWallet')}
          >
            <Pencil size={12} />
          </button>
          {!isSynced && (
            <button
              onClick={() => setDeletingWalletId(wallet.id)}
              className="p-1 rounded-lg text-muted-foreground hover:text-rose-600 hover:bg-rose-50 transition-colors"
              title={t('assets.deleteWallet')}
            >
              <Trash2 size={12} />
            </button>
          )}
        </div>
        {!isCollapsed && walletAssets.length > 0 && (
          <div className="space-y-3 pl-4">
            {groupByDisplayClass(walletAssets).map(({ name, assets: subAssets }) => (
              <div key={name} className="space-y-2">
                {/* Subgroup header — only shown when there's more than one
                    subgroup in the wallet (otherwise it's redundant). */}
                {groupByDisplayClass(walletAssets).length > 1 && (
                  <div className="flex items-center justify-between px-1 pt-1 border-l-2 border-muted-foreground/20 pl-3">
                    <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider">
                      {name}
                      <span className="text-[10px] text-muted-foreground/70 ml-2 normal-case">
                        ({subAssets.length})
                      </span>
                    </span>
                    <span className="text-[11px] font-bold tabular-nums text-muted-foreground">
                      {mask(formatCurrency(
                        subAssets.reduce((sum, a) =>
                          sum + (a.current_value_primary ?? a.current_value ?? 0), 0),
                        userCurrency, locale))}
                    </span>
                  </div>
                )}
                <div className="space-y-2">
                  {subAssets.map(renderAssetCard)}
                </div>
              </div>
            ))}
          </div>
        )}
        {!isCollapsed && walletAssets.length === 0 && (
          <div className="pl-4 py-3 text-xs text-muted-foreground italic">
            {t('assets.emptyWallet')}
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <PageHeader
        section={t('assets.title')}
        title={t('assets.title')}
        action={
          <div className="flex items-center gap-2">
            <Button onClick={openCreateWallet} variant="outline" className="gap-1.5">
              <Wallet size={16} />
              {t('assets.newWallet')}
            </Button>
            <Button onClick={openCreate} className="gap-1.5">
              <Plus size={16} />
              {t('assets.addAsset')}
            </Button>
          </div>
        }
      />

      {/* KPI bar: Patrimônio em hoje + Resultado (período) + período selector */}
      <div className="bg-card rounded-xl border border-border p-5 flex flex-wrap items-center gap-x-8 gap-y-3">
        <div>
          <p className="text-[11px] uppercase tracking-wider text-muted-foreground">
            Patrimônio em {new Date().toLocaleDateString(locale)}
          </p>
          <p className="text-2xl font-bold tabular-nums text-foreground">
            {mask(formatCurrency(totalPortfolioValue, userCurrency, locale))}
          </p>
        </div>
        {kpiResult && (
          <div>
            <p className="text-[11px] uppercase tracking-wider text-muted-foreground">
              Resultado ({kpiPeriod.label})
            </p>
            <p className={`text-xl font-bold tabular-nums ${
              kpiResult.delta >= 0 ? 'text-emerald-600' : 'text-rose-500'
            }`}>
              {mask(`${kpiResult.delta >= 0 ? '+' : ''}${formatCurrency(kpiResult.delta, userCurrency, locale)}`)}
              <span className="text-sm font-medium ml-2">
                ({kpiResult.twr_period >= 0 ? '+' : ''}{(kpiResult.twr_period * 100).toFixed(2)}%)
              </span>
            </p>
          </div>
        )}
        <div className="ml-auto flex items-center rounded-lg border border-border bg-muted/30 overflow-hidden">
          {([
            { label: '30d', months: 1, sinceStart: false },
            { label: '6M',  months: 6, sinceStart: false },
            { label: '1A',  months: 12, sinceStart: false },
            { label: '2A',  months: 24, sinceStart: false },
            { label: 'Tudo', months: 12, sinceStart: true },
          ] as const).map(p => (
            <button
              key={p.label}
              onClick={() => setKpiPeriod({ ...p })}
              className={`px-3 py-1.5 text-xs font-semibold transition-colors ${
                kpiPeriod.label === p.label
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
              }`}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>

      {/* Portfolio Stacked Area Chart */}
      {portfolioData && portfolioData.trend.length > 1 && (
        <PortfolioChart
          data={portfolioData}
          wallets={sortedWallets}
          currency={userCurrency}
          locale={locale}
          mask={mask}
        />
      )}

      {isLoading ? (
        <div className="space-y-3">
          {Array.from({ length: 3 }).map((_, i) => <Skeleton key={i} className="h-16 rounded-xl" />)}
        </div>
      ) : (
        <div className="space-y-6">
          {/* Wallets (active assets grouped) */}
          {(sortedWallets.length > 0 || ungroupedAssets.length > 0) && (
            <div className="space-y-4">
              {sortedWallets.map(w => renderWalletSection(w, assetsByGroup.get(w.id) ?? []))}

              {ungroupedAssets.length > 0 && (
                <div className="space-y-3">
                  <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider px-1">
                    {sortedWallets.length > 0 ? t('assets.ungrouped') : t('assets.activeAssets')}
                  </h3>
                  {groupByDisplayClass(ungroupedAssets).map(({ name, assets: subAssets }) => (
                    <div key={name} className="space-y-2">
                      {groupByDisplayClass(ungroupedAssets).length > 1 && (
                        <div className="flex items-center justify-between px-1 pt-1 border-l-2 border-muted-foreground/20 pl-3">
                          <span className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider">
                            {name}
                            <span className="text-[10px] text-muted-foreground/70 ml-2 normal-case">
                              ({subAssets.length})
                            </span>
                          </span>
                          <span className="text-[11px] font-bold tabular-nums text-muted-foreground">
                            {mask(formatCurrency(
                              subAssets.reduce((sum, a) =>
                                sum + (a.current_value_primary ?? a.current_value ?? 0), 0),
                              userCurrency, locale))}
                          </span>
                        </div>
                      )}
                      <div className="space-y-2">{subAssets.map(renderAssetCard)}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Sold Assets */}
          {soldAssets.length > 0 && (
            <div className="space-y-2">
              <h3 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider px-1">
                {t('assets.soldAssets')}
              </h3>
              <div className="space-y-2">
                {soldAssets.map(renderAssetCard)}
              </div>
            </div>
          )}

          {activeAssets.length === 0 && soldAssets.length === 0 && (
            <div className="text-center py-16">
              <Package className="mx-auto h-12 w-12 text-muted-foreground/40 mb-3" />
              <p className="text-muted-foreground">{t('assets.noAssets')}</p>
            </div>
          )}
        </div>
      )}

      {/* Create/Edit Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-lg max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editingAsset ? t('assets.editAsset') : t('assets.addAsset')}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            {/* Name */}
            <div className="space-y-2">
              <Label>{t('assets.name')}</Label>
              <Input value={formName} onChange={e => setFormName(e.target.value)} />
            </div>

            {/* Type + Currency */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>{t('assets.type')}</Label>
                <select
                  className="bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-3 py-2 rounded-lg text-foreground text-sm w-full"
                  value={formType}
                  onChange={e => setFormType(e.target.value)}
                >
                  {ASSET_TYPES.map(at => (
                    <option key={at} value={at}>
                      {t(`assets.type${at.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase()).replace(/^./, c => c.toUpperCase())}`)}
                    </option>
                  ))}
                </select>
              </div>
              <div className="space-y-2">
                <Label>{t('assets.currency')}</Label>
                <select
                  className="bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-3 py-2 rounded-lg text-foreground text-sm w-full disabled:opacity-60 disabled:cursor-not-allowed"
                  value={formCurrency}
                  disabled={formMethod === 'market_price'}
                  onChange={e => setFormCurrency(e.target.value)}
                >
                  {(supportedCurrencies ?? [{ code: userCurrency, symbol: userCurrency, name: userCurrency, flag: '' }]).map((c) => (
                    <option key={c.code} value={c.code}>{c.flag} {c.name}</option>
                  ))}
                </select>
              </div>
            </div>

            {/* Asset class — explicit taxonomy used by Investments dashboard */}
            <div className="space-y-2">
              <Label>Classe do ativo</Label>
              <select
                className="bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-3 py-2 rounded-lg text-foreground text-sm w-full"
                value={formAssetClass}
                onChange={e => setFormAssetClass(e.target.value)}
              >
                <option value="">— selecionar —</option>
                {ASSET_CLASS_OPTIONS.map(opt => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
            </div>

            {/* Valuation Method — locked on edit */}
            <div className="space-y-2">
              <Label>{t('assets.valuationMethod')}</Label>
              <div className="grid grid-cols-3 gap-2">
                {VALUATION_METHODS.map(m => (
                  <button
                    key={m}
                    type="button"
                    disabled={!!editingAsset}
                    className={`px-3 py-2.5 rounded-lg text-sm font-medium border transition-all ${
                      formMethod === m
                        ? 'border-primary bg-primary/10 text-primary shadow-sm'
                        : 'border-border text-muted-foreground hover:border-primary/50 hover:bg-muted/50'
                    } ${editingAsset ? 'opacity-50 cursor-not-allowed' : ''}`}
                    onClick={() => !editingAsset && setFormMethod(m)}
                  >
                    {m === 'market_price'
                      ? t('assets.marketPrice')
                      : m === 'growth_rule'
                        ? t('assets.growthRule')
                        : t('assets.manual')}
                  </button>
                ))}
              </div>
            </div>

            {/* Market Price (yfinance) — ticker search + quantity */}
            {formMethod === 'market_price' && (
              <div className="space-y-3 p-3.5 rounded-xl border border-primary/20 bg-primary/5">
                <div className="space-y-2">
                  <Label>{t('assets.ticker')}</Label>
                  <div className="relative">
                    <Input
                      placeholder={t('assets.tickerPlaceholder')}
                      value={formTickerQuery}
                      disabled={!!editingAsset}
                      onChange={e => {
                        setFormTickerQuery(e.target.value)
                        // Clear the quote so we don't keep the old preview
                        // while the user is editing the symbol — prevents
                        // a stale price from being saved accidentally.
                        if (selectedQuote && e.target.value.toUpperCase() !== selectedQuote.symbol) {
                          setSelectedQuote(null)
                        }
                      }}
                    />
                    {tickerMatches.length > 0 && !editingAsset && (
                      <div className="absolute z-20 mt-1 w-full max-h-60 overflow-y-auto rounded-lg border border-border bg-popover shadow-lg">
                        {tickerMatches.map(match => (
                          <button
                            key={`${match.symbol}-${match.exchange ?? ''}`}
                            type="button"
                            onClick={() => pickTickerMatch(match)}
                            className="flex flex-col w-full text-left px-3 py-2 hover:bg-muted transition-colors"
                          >
                            <div className="flex items-center justify-between gap-2">
                              <span className="font-semibold text-sm">{match.symbol}</span>
                              {match.exchange && (
                                <span className="text-xs text-muted-foreground">{match.exchange}</span>
                              )}
                            </div>
                            {match.name && (
                              <span className="text-xs text-muted-foreground truncate">{match.name}</span>
                            )}
                          </button>
                        ))}
                      </div>
                    )}
                    {tickerSearchLoading && (
                      <span className="absolute right-3 top-1/2 -translate-y-1/2 text-xs text-muted-foreground pointer-events-none">
                        {t('common.loading')}
                      </span>
                    )}
                  </div>
                </div>

                {selectedQuote && (
                  <div className="rounded-lg border border-border bg-card p-3 text-sm">
                    <div className="flex items-center justify-between">
                      <div className="flex flex-col min-w-0">
                        <span className="font-semibold">{selectedQuote.symbol}</span>
                        {selectedQuote.name && (
                          <span className="text-xs text-muted-foreground truncate">{selectedQuote.name}</span>
                        )}
                        {/* Staleness hint — only meaningful when editing an
                            existing asset (last_price_at is set). Hidden
                            during create because the quote is inline-live. */}
                        {editingAsset?.last_price_at && (
                          <span className="text-[10px] text-muted-foreground mt-0.5">
                            {t('assets.lastUpdated', { when: formatRelativeTime(editingAsset.last_price_at, locale) })}
                          </span>
                        )}
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        <div className="text-right">
                          <div className="text-base font-bold tabular-nums">
                            {formatCurrency(selectedQuote.price, selectedQuote.currency, locale)}
                          </div>
                          {selectedQuote.exchange && (
                            <div className="text-[10px] text-muted-foreground uppercase tracking-wide">
                              {selectedQuote.exchange}
                            </div>
                          )}
                        </div>
                        {/* Manual refresh — only on edit. Daily cron handles
                            the rest; this button is the escape hatch when a
                            user wants a fresh quote right now. */}
                        {editingAsset && (
                          <button
                            type="button"
                            onClick={() => refreshPriceMutation.mutate(editingAsset.id)}
                            disabled={refreshPriceMutation.isPending}
                            title={t('assets.refreshPrice')}
                            className="p-1.5 rounded-md text-muted-foreground hover:text-primary hover:bg-primary/10 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
                          >
                            <RefreshCw
                              size={14}
                              className={refreshPriceMutation.isPending ? 'animate-spin' : ''}
                            />
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                )}

                <div className="space-y-2">
                  <Label>{t('assets.quantity')}</Label>
                  <Input
                    type="number"
                    step="any"
                    min="0"
                    value={formUnits}
                    onChange={e => setFormUnits(e.target.value)}
                    placeholder="10"
                  />
                </div>

                {selectedQuote && formUnits && parseFloat(formUnits) > 0 && (
                  <div className="flex items-center justify-between p-3 rounded-lg border border-primary/30 bg-primary/10">
                    <span className="text-xs font-medium text-primary/80">
                      {t('assets.currentValue')}
                    </span>
                    <span className="text-lg font-bold tabular-nums text-primary">
                      {formatCurrency(
                        selectedQuote.price * parseFloat(formUnits),
                        selectedQuote.currency,
                        locale,
                      )}
                    </span>
                  </div>
                )}

                {quoteLoading && (
                  <div className="text-xs text-muted-foreground">{t('common.loading')}</div>
                )}
              </div>
            )}

            {/* Growth Rule Settings */}
            {formMethod === 'growth_rule' && (
              <div className="space-y-3 p-3.5 rounded-xl border border-primary/20 bg-primary/5">
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>{t('assets.growthType')}</Label>
                    <select
                      className="bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-3 py-2 rounded-lg text-foreground text-sm w-full"
                      value={formGrowthType}
                      onChange={e => setFormGrowthType(e.target.value)}
                    >
                      {GROWTH_TYPES.map(gt => (
                        <option key={gt} value={gt}>{t(`assets.${gt}`)}</option>
                      ))}
                    </select>
                  </div>
                  <div className="space-y-2">
                    <Label>{t('assets.growthRate')}</Label>
                    <div className="relative">
                      <Input type="number" step="any" value={formGrowthRate} onChange={e => setFormGrowthRate(e.target.value)} className={formGrowthType === 'percentage' ? 'pr-8' : ''} />
                      {formGrowthType === 'percentage' && (
                        <span className="absolute right-3 top-1/2 -translate-y-1/2 text-sm text-muted-foreground pointer-events-none">%</span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <Label>{t('assets.growthFrequency')}</Label>
                    <select
                      className="bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-3 py-2 rounded-lg text-foreground text-sm w-full"
                      value={formGrowthFrequency}
                      onChange={e => setFormGrowthFrequency(e.target.value)}
                    >
                      {GROWTH_FREQUENCIES.map(gf => (
                        <option key={gf} value={gf}>{t(`assets.${gf}`)}</option>
                      ))}
                    </select>
                  </div>
                  <div className="space-y-2">
                    <Label>{t('assets.growthStartDate')}</Label>
                    <DatePickerInput value={formGrowthStartDate} onChange={setFormGrowthStartDate} />
                  </div>
                </div>
              </div>
            )}

            {/* Purchase Info */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>{t('assets.purchaseDate')}</Label>
                <DatePickerInput value={formPurchaseDate} onChange={setFormPurchaseDate} />
              </div>
              <div className="space-y-2">
                <Label>{t('assets.purchasePrice')}</Label>
                <Input type="number" step="0.01" value={formPurchasePrice} onChange={e => setFormPurchasePrice(e.target.value)} />
              </div>
            </div>

            {/* Sell Info */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label>{t('assets.sellDate')}</Label>
                <DatePickerInput value={formSellDate} onChange={setFormSellDate} />
              </div>
              <div className="space-y-2">
                <Label>{t('assets.sellPrice')}</Label>
                <Input type="number" step="0.01" value={formSellPrice} onChange={e => setFormSellPrice(e.target.value)} />
              </div>
            </div>

            {/* Current Value — manual only */}
            {!editingAsset && formMethod === 'manual' && (
              <div className="space-y-2">
                <Label>{t('assets.currentValue')}</Label>
                <Input
                  type="number"
                  step="any"
                  value={formCurrentValue}
                  onChange={e => setFormCurrentValue(e.target.value)}
                />
              </div>
            )}

            {/* Projected Value — growth rule preview */}
            {formMethod === 'growth_rule' && projectedGrowthValue != null && (() => {
              const base = parseFloat(formPurchasePrice) || 0
              const isLoss = projectedGrowthValue < base
              const diff = projectedGrowthValue - base
              return (
                <div className={`flex items-center justify-between p-3.5 rounded-xl border ${isLoss ? 'bg-rose-50 dark:bg-rose-950/30 border-rose-200 dark:border-rose-800' : 'bg-emerald-50 dark:bg-emerald-950/30 border-emerald-200 dark:border-emerald-800'}`}>
                  <div>
                    <span className="text-xs font-medium text-muted-foreground">{t('assets.currentValue')}</span>
                    {base > 0 && (
                      <p className={`text-[11px] tabular-nums font-medium mt-0.5 ${isLoss ? 'text-rose-500' : 'text-emerald-600'}`}>
                        {diff >= 0 ? '+' : ''}{formatCurrency(diff, formCurrency, locale)}
                      </p>
                    )}
                  </div>
                  <span className={`text-xl font-bold tabular-nums ${isLoss ? 'text-rose-600' : 'text-emerald-600'}`}>
                    {formatCurrency(projectedGrowthValue, formCurrency, locale)}
                  </span>
                </div>
              )
            })()}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDialogOpen(false)}>
              {t('common.cancel')}
            </Button>
            <Button
              onClick={handleSave}
              disabled={
                !formName
                || createMutation.isPending
                || updateMutation.isPending
                // Market-price guard: must have a resolved ticker + quantity.
                || (formMethod === 'market_price'
                  && !editingAsset
                  && (!selectedQuote || !formUnits || parseFloat(formUnits) <= 0))
              }
            >
              {t('common.save')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Regenerate Growth Confirmation */}
      <Dialog open={!!pendingGrowthSave} onOpenChange={() => setPendingGrowthSave(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t('assets.confirmRegenerateTitle')}</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">{t('assets.confirmRegenerate')}</p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setPendingGrowthSave(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              onClick={confirmRegenerateGrowth}
              disabled={updateMutation.isPending}
            >
              {t('assets.regenerate')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <Dialog open={!!deletingId} onOpenChange={() => setDeletingId(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t('assets.confirmDeleteTitle')}</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">{t('assets.confirmDelete')}</p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeletingId(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              variant="destructive"
              onClick={() => deletingId && deleteMutation.mutate(deletingId)}
              disabled={deleteMutation.isPending}
            >
              {t('common.delete')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Wallet Create/Edit Dialog */}
      <Dialog open={walletDialogOpen} onOpenChange={setWalletDialogOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>
              {editingWallet ? t('assets.editWallet') : t('assets.newWallet')}
            </DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label>{t('assets.walletName')}</Label>
              <Input
                value={walletFormName}
                onChange={e => setWalletFormName(e.target.value)}
                placeholder={t('assets.walletNamePlaceholder')}
                autoFocus
              />
              {editingWallet?.institution_name && editingWallet.source !== 'manual' && (
                <p className="text-[11px] text-muted-foreground flex items-center gap-1">
                  <RefreshCw size={10} />
                  {t('assets.syncedFromHint', { source: editingWallet.institution_name })}
                </p>
              )}
            </div>
            <div className="space-y-2">
              <Label>{t('assets.walletColor')}</Label>
              <Input
                type="color"
                value={walletFormColor}
                onChange={e => setWalletFormColor(e.target.value)}
                className="h-9 w-20 px-1 py-1"
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setWalletDialogOpen(false)}>
              {t('common.cancel')}
            </Button>
            <Button
              onClick={handleSaveWallet}
              disabled={!walletFormName.trim() || createWalletMutation.isPending || updateWalletMutation.isPending}
            >
              {t('common.save')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Wallet Confirmation */}
      <Dialog open={!!deletingWalletId} onOpenChange={() => setDeletingWalletId(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{t('assets.confirmDeleteWalletTitle')}</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">{t('assets.confirmDeleteWallet')}</p>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeletingWalletId(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              variant="destructive"
              onClick={() => deletingWalletId && deleteWalletMutation.mutate(deletingWalletId)}
              disabled={deleteWalletMutation.isPending}
            >
              {t('common.delete')}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Move Asset to Wallet Picker */}
      <Dialog open={!!movingAsset} onOpenChange={() => setMovingAsset(null)}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>{t('assets.moveToWallet')}</DialogTitle>
          </DialogHeader>
          <div className="space-y-1 max-h-80 overflow-y-auto">
            <button
              onClick={() => movingAsset && moveAssetMutation.mutate({ id: movingAsset.id, groupId: null })}
              disabled={!movingAsset?.group_id || moveAssetMutation.isPending}
              className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors text-left"
            >
              <div className="w-6 h-6 rounded-md flex items-center justify-center bg-muted">
                <Package size={13} className="text-muted-foreground" />
              </div>
              <span className="text-sm text-foreground">{t('assets.noWallet')}</span>
            </button>
            {sortedWallets.map(w => (
              <button
                key={w.id}
                onClick={() => movingAsset && moveAssetMutation.mutate({ id: movingAsset.id, groupId: w.id })}
                disabled={movingAsset?.group_id === w.id || moveAssetMutation.isPending}
                className="w-full flex items-center gap-3 px-3 py-2.5 rounded-lg hover:bg-muted disabled:opacity-50 disabled:cursor-not-allowed transition-colors text-left"
              >
                <div
                  className="w-6 h-6 rounded-md flex items-center justify-center"
                  style={{ backgroundColor: `${w.color}20` }}
                >
                  <Wallet size={13} style={{ color: w.color }} />
                </div>
                <span className="text-sm text-foreground flex-1 truncate">{w.name}</span>
                <span className="text-xs text-muted-foreground">{w.asset_count}</span>
              </button>
            ))}
            {sortedWallets.length === 0 && (
              <p className="text-xs text-muted-foreground italic px-3 py-2">
                {t('assets.noWalletsHint')}
              </p>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </div>
  )
}

const PORTFOLIO_COLORS = ['#6366F1', '#F43F5E', '#F59E0B', '#10B981', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16']

function PortfolioChart({ data, wallets, currency, locale: loc, mask }: {
  data: { assets: { id: string; name: string; type: string; group_id: string | null }[]; trend: Record<string, unknown>[]; total: number }
  wallets: AssetGroup[]
  currency: string
  locale: string
  mask: (v: string) => string
}) {
  const { t } = useTranslation()
  // Default to wallet mode: with many synced CDBs the asset view turns
  // into a cluttered rainbow legend that's hard to parse.
  const [mode, setMode] = useState<'wallet' | 'asset'>('wallet')

  const formatCompact = (v: number) => {
    const abs = Math.abs(v)
    if (abs >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`
    if (abs >= 1_000) return `${(v / 1_000).toFixed(abs >= 10_000 ? 0 : 1)}k`
    return v.toLocaleString(loc, { maximumFractionDigits: 0 })
  }

  // Compute the series list and rewrite trend rows based on the selected
  // mode. Wallet mode rolls all assets sharing a group_id into a single
  // series (using the wallet's own color); ungrouped assets keep their
  // individual lines so nothing disappears from the chart.
  const { series, displayTrend } = useMemo(() => {
    if (mode === 'asset') {
      const s = data.assets.map((a, i) => ({
        key: a.id,
        name: a.name,
        color: PORTFOLIO_COLORS[i % PORTFOLIO_COLORS.length],
        sourceAssetIds: [a.id],
      }))
      return { series: s, displayTrend: data.trend }
    }

    const walletById = new Map<string, AssetGroup>()
    for (const w of wallets) walletById.set(w.id, w)

    const groupBuckets = new Map<string, string[]>()
    const ungroupedAssetIds: string[] = []
    for (const a of data.assets) {
      if (a.group_id) {
        if (!groupBuckets.has(a.group_id)) groupBuckets.set(a.group_id, [])
        groupBuckets.get(a.group_id)!.push(a.id)
      } else {
        ungroupedAssetIds.push(a.id)
      }
    }

    // Preserve wallet display order. Falls back to insertion order for
    // wallets that show up in the data but aren't in the wallets list
    // (e.g. race conditions between queries).
    const orderedGroupIds = [
      ...wallets.map(w => w.id).filter(id => groupBuckets.has(id)),
      ...Array.from(groupBuckets.keys()).filter(id => !walletById.has(id)),
    ]

    const s: { key: string; name: string; color: string; sourceAssetIds: string[] }[] = []
    let fallbackColorIdx = 0
    for (const gid of orderedGroupIds) {
      const wallet = walletById.get(gid)
      const assetIds = groupBuckets.get(gid)!
      s.push({
        key: `w_${gid}`,
        name: wallet?.name ?? t('assets.ungrouped'),
        color: wallet?.color ?? PORTFOLIO_COLORS[fallbackColorIdx++ % PORTFOLIO_COLORS.length],
        sourceAssetIds: assetIds,
      })
    }
    for (const aid of ungroupedAssetIds) {
      const asset = data.assets.find(a => a.id === aid)
      s.push({
        key: aid,
        name: asset?.name ?? aid,
        color: PORTFOLIO_COLORS[fallbackColorIdx++ % PORTFOLIO_COLORS.length],
        sourceAssetIds: [aid],
      })
    }

    const newTrend = data.trend.map(row => {
      const newRow: Record<string, unknown> = { date: row.date, _total: row._total }
      for (const entry of s) {
        let sum = 0
        for (const aid of entry.sourceAssetIds) {
          sum += (row[aid] as number) ?? 0
        }
        newRow[entry.key] = sum
      }
      return newRow
    })

    return { series: s, displayTrend: newTrend }
  }, [mode, data, wallets, t])

  return (
    <div className="border border-border rounded-xl bg-card shadow-sm p-5">
      <div className="flex items-center justify-between mb-4 gap-4">
        <div className="flex items-center gap-3">
          <h3 className="text-sm font-semibold text-foreground">{t('assets.portfolioValue')}</h3>
          <div className="inline-flex items-center rounded-lg border border-border p-0.5 bg-muted/40">
            <button
              onClick={() => setMode('wallet')}
              className={`px-2.5 py-1 rounded-md text-[11px] font-medium transition-colors ${mode === 'wallet' ? 'bg-card text-foreground shadow-sm' : 'text-muted-foreground hover:text-foreground'}`}
            >
              {t('assets.chartByWallet')}
            </button>
            <button
              onClick={() => setMode('asset')}
              className={`px-2.5 py-1 rounded-md text-[11px] font-medium transition-colors ${mode === 'asset' ? 'bg-card text-foreground shadow-sm' : 'text-muted-foreground hover:text-foreground'}`}
            >
              {t('assets.chartByAsset')}
            </button>
          </div>
        </div>
        <div className="text-right">
          <span className="text-xs text-muted-foreground">{t('assets.total')}</span>
          <p className="text-lg font-bold tabular-nums text-foreground">
            {mask(formatCurrency(data.total, currency, loc))}
          </p>
        </div>
      </div>
      <div className="h-56">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={displayTrend} margin={{ top: 4, right: 12, left: 0, bottom: 0 }}>
            <defs>
              {series.map(s => (
                <linearGradient key={s.key} id={`portfolio-grad-${s.key}`} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor={s.color} stopOpacity={0.5} />
                  <stop offset="100%" stopColor={s.color} stopOpacity={0.1} />
                </linearGradient>
              ))}
            </defs>
            <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" strokeOpacity={0.5} />
            <XAxis
              dataKey="date"
              tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v: string) => new Date(v + 'T00:00:00').toLocaleDateString(loc, { month: 'short', year: '2-digit' })}
            />
            <YAxis
              tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
              axisLine={false}
              tickLine={false}
              width={56}
              tickFormatter={(v: number) => mask(formatCompact(v))}
            />
            <RechartsTooltip
              content={({ active, payload, label }) => {
                if (!active || !payload?.length) return null
                const totalEntry = payload.find(p => p.dataKey === '_total')
                const dateTotal = totalEntry?.value as number ?? 0
                const items = series
                  .map(s => {
                    const row = displayTrend.find(r => r.date === label)
                    const val = row ? ((row[s.key] as number) ?? 0) : 0
                    return { key: s.key, name: s.name, value: val, color: s.color }
                  })
                  .filter(item => item.value !== 0)
                  .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
                if (items.length === 0) return null
                return (
                  <div style={{ background: 'var(--card)', color: 'var(--foreground)', border: '1px solid var(--border)', borderRadius: '0.75rem', fontSize: '12px', boxShadow: '0 4px 12px rgba(0,0,0,0.08)', padding: '10px 12px' }}>
                    <p style={{ fontWeight: 600, marginBottom: 6 }}>
                      {new Date(label + 'T00:00:00').toLocaleDateString(loc, { day: 'numeric', month: 'long', year: 'numeric' })}
                    </p>
                    {items.map(item => (
                      <div key={item.key} style={{ display: 'flex', justifyContent: 'space-between', gap: 16, marginBottom: 2 }}>
                        <span style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                          <span style={{ width: 8, height: 8, borderRadius: '50%', backgroundColor: item.color, display: 'inline-block' }} />
                          {item.name}
                        </span>
                        <span style={{ fontWeight: 500, fontVariantNumeric: 'tabular-nums' }}>{mask(formatCurrency(item.value, currency, loc))}</span>
                      </div>
                    ))}
                    <div style={{ borderTop: '1px solid var(--border)', marginTop: 6, paddingTop: 6, display: 'flex', justifyContent: 'space-between', fontWeight: 700 }}>
                      <span>{t('assets.total')}</span>
                      <span style={{ fontVariantNumeric: 'tabular-nums' }}>{mask(formatCurrency(dateTotal, currency, loc))}</span>
                    </div>
                  </div>
                )
              }}
            />
            {/* Stacked areas — one colored band per series */}
            {series.map(s => (
              <Area
                key={s.key}
                type="monotone"
                dataKey={s.key}
                stackId="portfolio"
                stroke={s.color}
                strokeWidth={1}
                fill={`url(#portfolio-grad-${s.key})`}
                dot={false}
                activeDot={{ r: 3, strokeWidth: 1.5, fill: 'var(--card)' }}
              />
            ))}
            {/* Hidden total for tooltip */}
            <Area dataKey="_total" stroke="none" fill="none" dot={false} activeDot={false} />
          </AreaChart>
        </ResponsiveContainer>
      </div>
      {/* Legend */}
      <div className="flex flex-wrap gap-x-4 gap-y-1 mt-3 px-1">
        {series.map(s => (
          <div key={s.key} className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: s.color }} />
            <span className="text-[11px] text-muted-foreground">{s.name}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

function AssetDetail({ assetId, currency, locale: loc, purchasePrice, purchaseDate, valuationMethod }: {
  assetId: string; currency: string; locale: string
  purchasePrice: number | null; purchaseDate: string | null
  valuationMethod: string
}) {
  const { t } = useTranslation()
  const { mask } = usePrivacyMode()
  const queryClient = useQueryClient()

  const [valueAmount, setValueAmount] = useState('')
  const [valueDate, setValueDate] = useState(new Date().toISOString().slice(0, 10))
  const [priceRange, setPriceRange] = useState<'1mo' | '3mo' | '6mo' | '1y' | '2y' | '5y' | 'max'>('1y')

  // Transaction form state (BUY/SELL/DIVIDEND/JCP/RENDIMENTO/RESGATE)
  const [txDate, setTxDate] = useState(new Date().toISOString().slice(0, 10))
  const [txType, setTxType] = useState<string>('BUY')
  const [txQty, setTxQty] = useState('')
  const [txPrice, setTxPrice] = useState('')
  const [txValue, setTxValue] = useState('')
  const [txNotes, setTxNotes] = useState('')

  // The asset-values list query was removed when we hid the
  // "Histórico de Valores" section. The trend query below still feeds
  // the legacy chart for manual assets.

  const { data: trend } = useQuery({
    queryKey: ['asset-trend', assetId],
    queryFn: () => assets.valueTrend(assetId),
    enabled: valuationMethod === 'manual',  // only used for manual assets fallback chart
  })

  // For market_priced assets: live cotação chart from Yahoo via backend.
  const { data: priceHistory, isLoading: priceHistoryLoading } = useQuery({
    queryKey: ['asset-price-history', assetId, priceRange],
    queryFn: () => assets.priceHistory(assetId, priceRange),
    enabled: valuationMethod === 'market_price',
    staleTime: 1000 * 60 * 30,
  })

  // Build full trend: purchase point + stored values
  const trendWithPurchase = useMemo(() => {
    if (!trend) return []
    let result = [...trend]

    // Prepend purchase point if it predates the first value
    if (purchasePrice && purchaseDate) {
      if (result.length === 0 || purchaseDate < result[0].date) {
        result = [{ date: purchaseDate, amount: purchasePrice }, ...result]
      }
    }

    return result
  }, [trend, purchasePrice, purchaseDate])

  // valuesWithPurchase removed when we hid the Histórico de Valores section.

  const addValueMutation = useMutation({
    mutationFn: ({ assetId: id, ...data }: { assetId: string; amount: number; date: string }) =>
      assets.addValue(id, data),
    onSuccess: () => {
      queryClient.refetchQueries({ queryKey: ['assets'] })
      queryClient.refetchQueries({ queryKey: ['asset-values', assetId] })
      queryClient.refetchQueries({ queryKey: ['asset-trend', assetId] })
      queryClient.refetchQueries({ queryKey: ['dashboard'] })
      setValueAmount('')
      toast.success(t('assets.valueAdded'))
    },
    onError: () => toast.error(t('common.error')),
  })

  // deleteValueMutation removed with the Histórico de Valores section.

  const { data: transactions, isLoading: transactionsLoading } = useQuery({
    queryKey: ['asset-transactions', assetId],
    queryFn: () => assets.transactions(assetId),
  })

  const invalidateAfterTx = () => {
    queryClient.refetchQueries({ queryKey: ['asset-transactions', assetId] })
    queryClient.refetchQueries({ queryKey: ['portfolio-timeseries'] })
    queryClient.refetchQueries({ queryKey: ['assets'] })
  }

  const addTxMutation = useMutation({
    mutationFn: (tx: {
      date: string; type: string;
      qty?: number | null; price?: number | null; value?: number | null;
      notes?: string | null;
    }) => assets.addTransaction(assetId, tx),
    onSuccess: () => {
      setTxQty(''); setTxPrice(''); setTxValue(''); setTxNotes('')
      invalidateAfterTx()
      toast.success('Transação registrada')
    },
    onError: (e: unknown) => {
      const msg = (e as { response?: { data?: { detail?: string } } })?.response?.data?.detail
      toast.error(msg || 'Falha ao registrar transação')
    },
  })

  const deleteTxMutation = useMutation({
    mutationFn: (txId: string) => assets.deleteTransaction(txId),
    onSuccess: () => { invalidateAfterTx(); toast.success('Transação removida') },
    onError: () => toast.error(t('common.error')),
  })

  function submitTransaction() {
    const qty = txQty ? parseFloat(txQty) : null
    const price = txPrice ? parseFloat(txPrice) : null
    let value = txValue ? parseFloat(txValue) : null
    // For BUY/SELL, derive value from qty * price if not provided
    if (!value && qty && price) value = qty * price
    if (!value && (txType === 'DIVIDEND' || txType === 'JCP' || txType === 'RENDIMENTO' || txType === 'RESGATE')) {
      toast.error('Informe o valor recebido')
      return
    }
    if (!value && (txType === 'BUY' || txType === 'SELL')) {
      toast.error('Informe quantidade e preço (ou valor total)')
      return
    }
    addTxMutation.mutate({
      date: txDate,
      type: txType,
      qty,
      price,
      value,
      notes: txNotes || null,
    })
  }

  // Determine chart color based on trend direction
  const trendIsPositive = trendWithPurchase.length >= 2
    ? trendWithPurchase[trendWithPurchase.length - 1].amount >= trendWithPurchase[0].amount
    : true
  const chartColor = trendIsPositive ? '#10B981' : '#F43F5E'

  // Determine the price chart's color from its own first/last close (so the
  // line is green only when the cotação rose during the selected range).
  const priceData = priceHistory?.data ?? []
  const priceIsPositive = priceData.length >= 2
    ? priceData[priceData.length - 1].close >= priceData[0].close
    : true
  const priceColor = priceIsPositive ? '#10B981' : '#F43F5E'

  return (
    <div className="border-t border-border px-5 py-5 space-y-5 bg-muted/5">
      {/* Cotação chart — for market_priced assets, fetch daily closes from
          Yahoo via backend. For manual assets (CDB / Tesouro), fall back to
          the user's value trend (the asset has no public quote). */}
      {valuationMethod === 'market_price' ? (
        <div>
          <div className="flex items-center justify-between mb-3">
            <p className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider">
              Cotação {priceHistory?.ticker ? `(${priceHistory.ticker})` : ''}
            </p>
            <div className="flex items-center rounded-lg border border-border bg-card overflow-hidden">
              {(['1mo', '3mo', '6mo', '1y', '2y', '5y', 'max'] as const).map(r => (
                <button
                  key={r}
                  onClick={() => setPriceRange(r)}
                  className={`px-2 py-0.5 text-[10px] font-semibold transition-colors ${
                    priceRange === r
                      ? 'bg-primary text-primary-foreground'
                      : 'text-muted-foreground hover:text-foreground hover:bg-muted/50'
                  }`}
                >
                  {r === 'max' ? 'Tudo' : r.toUpperCase().replace('MO', 'M').replace('Y', 'A')}
                </button>
              ))}
            </div>
          </div>
          <div className="h-44 -mx-1">
            {priceHistoryLoading ? (
              <Skeleton className="h-full w-full rounded" />
            ) : priceData.length > 1 ? (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={priceData} margin={{ top: 4, right: 12, left: 0, bottom: 0 }}>
                  <defs>
                    <linearGradient id={`pgradient-${assetId}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={priceColor} stopOpacity={0.18} />
                      <stop offset="100%" stopColor={priceColor} stopOpacity={0} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" vertical={false}
                    stroke="var(--border)" strokeOpacity={0.5} />
                  <XAxis dataKey="date"
                    tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                    axisLine={false} tickLine={false}
                    tickFormatter={(v: string) =>
                      new Date(v + 'T00:00:00').toLocaleDateString(loc,
                        { month: 'short', year: '2-digit' })}
                    minTickGap={40}
                  />
                  <YAxis
                    tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                    axisLine={false} tickLine={false} width={56}
                    domain={['auto', 'auto']}
                    tickFormatter={(v: number) =>
                      v >= 1000 ? v.toLocaleString(loc, { maximumFractionDigits: 0 })
                        : v.toFixed(2)}
                  />
                  <RechartsTooltip
                    formatter={(value: number | undefined) => [
                      formatCurrency(value ?? 0,
                        priceHistory?.currency || currency, loc),
                      'Cotação',
                    ]}
                    labelFormatter={(label: unknown) =>
                      new Date(String(label) + 'T00:00:00').toLocaleDateString(
                        loc, { day: 'numeric', month: 'long', year: 'numeric' })}
                    contentStyle={{
                      background: 'var(--card)', color: 'var(--foreground)',
                      border: '1px solid var(--border)',
                      borderRadius: '0.75rem', fontSize: '12px',
                      boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
                    }}
                  />
                  <Area type="monotone" dataKey="close" stroke={priceColor}
                    strokeWidth={2} fill={`url(#pgradient-${assetId})`}
                    dot={false}
                    activeDot={{ r: 4, strokeWidth: 2, fill: 'var(--card)',
                      stroke: priceColor }}
                  />
                </AreaChart>
              </ResponsiveContainer>
            ) : (
              <p className="text-xs text-muted-foreground py-12 text-center">
                Sem cotação histórica disponível
              </p>
            )}
          </div>
        </div>
      ) : trendWithPurchase.length > 1 && (
        <div>
          <p className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider mb-3">
            Evolução de valor
          </p>
          <div className="h-44 -mx-1">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={trendWithPurchase} margin={{ top: 4, right: 12, left: 0, bottom: 0 }}>
                <defs>
                  <linearGradient id={`gradient-${assetId}`} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={chartColor} stopOpacity={0.2} />
                    <stop offset="100%" stopColor={chartColor} stopOpacity={0} />
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="var(--border)" strokeOpacity={0.5} />
                <XAxis dataKey="date"
                  tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                  axisLine={false} tickLine={false}
                  tickFormatter={(v: string) => new Date(v + 'T00:00:00').toLocaleDateString(loc, { month: 'short', year: '2-digit' })}
                />
                <YAxis
                  tick={{ fontSize: 10, fill: 'var(--muted-foreground)' }}
                  axisLine={false} tickLine={false} width={56}
                  domain={['dataMin', 'dataMax']}
                  tickFormatter={(v: number) => {
                    const abs = Math.abs(v)
                    let formatted: string
                    if (abs >= 1_000_000) formatted = `${(v / 1_000_000).toFixed(1)}M`
                    else if (abs >= 1_000) formatted = `${(v / 1_000).toFixed(abs >= 10_000 ? 0 : 1)}k`
                    else formatted = v.toLocaleString(loc, { maximumFractionDigits: 0 })
                    return mask(formatted)
                  }}
                />
                <RechartsTooltip
                  formatter={(value: number | undefined) => [mask(formatCurrency(value ?? 0, currency, loc)), 'Valor']}
                  labelFormatter={(label: unknown) => new Date(String(label) + 'T00:00:00').toLocaleDateString(loc, { day: 'numeric', month: 'long', year: 'numeric' })}
                  contentStyle={{
                    background: 'var(--card)', color: 'var(--foreground)',
                    border: '1px solid var(--border)',
                    borderRadius: '0.75rem', fontSize: '12px',
                    boxShadow: '0 4px 12px rgba(0,0,0,0.08)',
                  }}
                />
                <Area type="monotone" dataKey="amount" stroke={chartColor}
                  strokeWidth={2} fill={`url(#gradient-${assetId})`}
                  dot={false}
                  activeDot={{ r: 4, strokeWidth: 2, fill: 'var(--card)', stroke: chartColor }}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Add Value Form — only for manual assets */}
      {valuationMethod === 'manual' && <div className="flex items-end gap-2">
        <div className="flex-1">
          <Label className="text-[11px] text-muted-foreground">{t('assets.amount')}</Label>
          <Input
            type="number"
            step="any"
            value={valueAmount}
            onChange={e => setValueAmount(e.target.value)}
            placeholder="0.00"
            className="h-8 text-sm"
          />
        </div>
        <div className="w-36">
          <Label className="text-[11px] text-muted-foreground">{t('assets.date')}</Label>
          <DatePickerInput value={valueDate} onChange={setValueDate} />
        </div>
        <Button
          size="sm"
          className="h-8 px-3 text-xs"
          disabled={!valueAmount || addValueMutation.isPending}
          onClick={() => {
            if (valueAmount) {
              addValueMutation.mutate({
                assetId,
                amount: parseFloat(valueAmount),
                date: valueDate,
              })
            }
          }}
        >
          <Plus size={14} className="mr-1" />
          {t('assets.addValue')}
        </Button>
      </div>}

      {/* Transactions — BUY / SELL / DIVIDEND / JCP / RENDIMENTO / RESGATE */}
      <div>
        <p className="text-[11px] font-semibold text-muted-foreground uppercase tracking-wider mb-2">
          Transações
        </p>
        <p className="text-[11px] text-muted-foreground mb-3">
          Registre compras, vendas e proventos para que a aba Investimentos
          calcule o TWR automaticamente. As transações alimentam o Modified
          Dietz mensal — o gráfico atualiza após cada lançamento.
        </p>

        {/* Add transaction form */}
        <div className="grid grid-cols-12 gap-2 items-end mb-3">
          <div className="col-span-3">
            <Label className="text-[11px] text-muted-foreground">Data</Label>
            <DatePickerInput value={txDate} onChange={setTxDate} />
          </div>
          <div className="col-span-3">
            <Label className="text-[11px] text-muted-foreground">Tipo</Label>
            <select
              className="bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-2 py-2 rounded-lg text-foreground text-sm w-full h-9"
              value={txType}
              onChange={e => setTxType(e.target.value)}
            >
              <option value="BUY">Compra</option>
              <option value="SELL">Venda</option>
              <option value="DIVIDEND">Dividendo</option>
              <option value="JCP">JCP</option>
              <option value="RENDIMENTO">Rendimento (FII)</option>
              <option value="RESGATE">Resgate</option>
            </select>
          </div>
          {(txType === 'BUY' || txType === 'SELL') && (
            <>
              <div className="col-span-2">
                <Label className="text-[11px] text-muted-foreground">Qtd.</Label>
                <Input
                  type="number" step="any"
                  value={txQty} onChange={e => setTxQty(e.target.value)}
                  placeholder="0" className="h-9 text-sm"
                />
              </div>
              <div className="col-span-2">
                <Label className="text-[11px] text-muted-foreground">Preço unit.</Label>
                <Input
                  type="number" step="any"
                  value={txPrice} onChange={e => setTxPrice(e.target.value)}
                  placeholder="0.00" className="h-9 text-sm"
                />
              </div>
            </>
          )}
          <div className={(txType === 'BUY' || txType === 'SELL') ? 'col-span-2' : 'col-span-6'}>
            <Label className="text-[11px] text-muted-foreground">
              Valor total {(txType === 'BUY' || txType === 'SELL') ? '(opcional)' : ''}
            </Label>
            <Input
              type="number" step="any"
              value={txValue} onChange={e => setTxValue(e.target.value)}
              placeholder="0.00" className="h-9 text-sm"
            />
          </div>
        </div>
        <div className="flex items-end gap-2 mb-4">
          <div className="flex-1">
            <Label className="text-[11px] text-muted-foreground">Nota (opcional)</Label>
            <Input
              value={txNotes} onChange={e => setTxNotes(e.target.value)}
              placeholder="ex: corretagem, observação..."
              className="h-9 text-sm"
            />
          </div>
          <Button
            size="sm"
            onClick={submitTransaction}
            disabled={addTxMutation.isPending}
          >
            <Plus size={14} className="mr-1" />
            Registrar
          </Button>
        </div>

        {/* Transactions list */}
        {transactionsLoading ? (
          <Skeleton className="h-16 w-full rounded-lg" />
        ) : transactions && transactions.length > 0 ? (
          <div className="rounded-lg border border-border overflow-hidden divide-y divide-border max-h-72 overflow-y-auto">
            {[...transactions].sort((a, b) => b.date.localeCompare(a.date)).map(tx => {
              const isInflow = tx.type === 'SELL' || tx.type === 'DIVIDEND'
                || tx.type === 'JCP' || tx.type === 'RENDIMENTO'
                || tx.type === 'RESGATE'
              return (
                <div key={tx.id} className="flex items-center justify-between py-2 px-3 hover:bg-muted/30 transition-colors">
                  <div className="flex items-center gap-3 min-w-0">
                    <Badge variant="outline" className={`text-[10px] px-1.5 py-0 ${
                      isInflow ? 'border-emerald-500/40 text-emerald-700'
                        : tx.type === 'BUY' ? 'border-blue-500/40 text-blue-700'
                          : ''
                    }`}>
                      {tx.type}
                    </Badge>
                    <span className="text-xs text-muted-foreground tabular-nums">
                      {new Date(tx.date + 'T00:00:00').toLocaleDateString(loc)}
                    </span>
                    {tx.qty != null && (
                      <span className="text-xs text-muted-foreground tabular-nums">
                        qty {tx.qty}
                        {tx.price != null && ` @ ${formatCurrency(tx.price, currency, loc)}`}
                      </span>
                    )}
                    {tx.notes && (
                      <span className="text-[11px] text-muted-foreground italic truncate max-w-[200px]">
                        {tx.notes}
                      </span>
                    )}
                  </div>
                  <div className="flex items-center gap-2 shrink-0">
                    <span className={`text-sm tabular-nums font-semibold ${
                      isInflow ? 'text-emerald-600' : 'text-foreground'
                    }`}>
                      {tx.value != null ? mask(formatCurrency(tx.value, currency, loc)) : '—'}
                    </span>
                    <button
                      onClick={() => {
                        if (confirm('Remover esta transação?')) {
                          deleteTxMutation.mutate(tx.id)
                        }
                      }}
                      className="p-1 rounded text-muted-foreground/40 hover:text-rose-600 transition-colors"
                      disabled={deleteTxMutation.isPending}
                    >
                      <Trash2 size={12} />
                    </button>
                  </div>
                </div>
              )
            })}
          </div>
        ) : (
          <p className="text-xs text-muted-foreground py-3 text-center">
            Nenhuma transação registrada
          </p>
        )}
      </div>

      {/* Histórico de Valores intentionally hidden — cotações individuais
          não são informação relevante pro usuário (vinham do import inicial
          ou do refresh diário do Yahoo). O gráfico de cotação acima e a
          seção Transações já cobrem tudo que importa. */}
    </div>
  )
}
