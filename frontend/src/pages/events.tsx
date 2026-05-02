import { useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { assetTransactions, assetGroups, type AssetTransactionLogItem } from '@/lib/api'
import type { AssetGroup } from '@/types'
import { Skeleton } from '@/components/ui/skeleton'
import { PageHeader } from '@/components/page-header'
import { Input } from '@/components/ui/input'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { DatePickerInput } from '@/components/ui/date-picker-input'
import { Popover, PopoverTrigger, PopoverContent } from '@/components/ui/popover'
import { usePrivacyMode } from '@/hooks/use-privacy-mode'
import { useAuth } from '@/contexts/auth-context'
import { Search, Filter, X, ChevronDown, ChevronLeft, ChevronRight, RefreshCw } from 'lucide-react'
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuCheckboxItem,
  DropdownMenuLabel,
} from '@/components/ui/dropdown-menu'

const TX_TYPES = [
  'BUY', 'SELL', 'DIVIDEND', 'JCP', 'RENDIMENTO', 'INTEREST',
  'DEPOSIT', 'WITHDRAWAL', 'RESGATE', 'FEE',
] as const

// Tailwind classes for type badges. Greens/blues for income, red for outflow,
// neutral for other movements.
const TYPE_BADGE: Record<string, string> = {
  BUY:        'bg-blue-500/10 text-blue-700 dark:text-blue-300 border-blue-500/20',
  SELL:       'bg-orange-500/10 text-orange-700 dark:text-orange-300 border-orange-500/20',
  DIVIDEND:   'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/20',
  JCP:        'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/20',
  RENDIMENTO: 'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/20',
  INTEREST:   'bg-emerald-500/10 text-emerald-700 dark:text-emerald-300 border-emerald-500/20',
  RESGATE:    'bg-violet-500/10 text-violet-700 dark:text-violet-300 border-violet-500/20',
  DEPOSIT:    'bg-blue-500/10 text-blue-700 dark:text-blue-300 border-blue-500/20',
  WITHDRAWAL: 'bg-orange-500/10 text-orange-700 dark:text-orange-300 border-orange-500/20',
  FEE:        'bg-rose-500/10 text-rose-700 dark:text-rose-300 border-rose-500/20',
}

const PAGE_SIZE = 100

function fmtCurrency(v: number | null, currency: string, locale: string) {
  if (v == null) return '—'
  try {
    return new Intl.NumberFormat(locale, { style: 'currency', currency: currency || 'BRL' }).format(v)
  } catch {
    return v.toFixed(2)
  }
}

function fmtDate(iso: string, locale: string) {
  try {
    return new Date(iso + 'T00:00:00').toLocaleDateString(locale)
  } catch {
    return iso
  }
}

function fmtQty(v: number | null) {
  if (v == null) return '—'
  // Trim trailing zeros, but keep up to 6 decimals.
  return new Intl.NumberFormat('pt-BR', { maximumFractionDigits: 6 }).format(v)
}

export default function EventsPage() {
  const { t, i18n } = useTranslation()
  const locale = i18n.language === 'en' ? 'en-US' : 'pt-BR'
  const { mask, MASK, privacyMode } = usePrivacyMode()
  const { user } = useAuth()
  const userCurrency = user?.preferences?.currency_display ?? 'BRL'

  const [q, setQ] = useState('')
  const [selectedTypes, setSelectedTypes] = useState<Set<string>>(new Set())
  const [selectedGroups, setSelectedGroups] = useState<Set<string>>(new Set())
  const [selectedSources, setSelectedSources] = useState<Set<string>>(new Set())
  const [dateFrom, setDateFrom] = useState('')
  const [dateTo, setDateTo] = useState('')
  const [page, setPage] = useState(0)

  const queryClient = useQueryClient()

  const { data: groupsList } = useQuery<AssetGroup[]>({
    queryKey: ['asset-groups'],
    queryFn: () => assetGroups.list(),
  })

  const syncDividendsMutation = useMutation({
    mutationFn: () => assetTransactions.syncDividends(),
    onSuccess: (data) => {
      const { created, skipped, fetched } = data
      if (created > 0) {
        toast.success(t('events.syncSuccess', { count: created }))
      } else {
        toast.info(t('events.syncNothingNew', { fetched, skipped }))
      }
      queryClient.invalidateQueries({ queryKey: ['asset-tx-log'] })
      queryClient.invalidateQueries({ queryKey: ['portfolio-timeseries'] })
    },
    onError: () => toast.error(t('events.syncFailed')),
  })

  const typesParam = useMemo(() => Array.from(selectedTypes), [selectedTypes])
  const groupsParam = useMemo(() => Array.from(selectedGroups), [selectedGroups])
  const sourcesParam = useMemo(() => Array.from(selectedSources), [selectedSources])

  const { data, isLoading } = useQuery({
    queryKey: ['asset-tx-log', q, typesParam, groupsParam, sourcesParam, dateFrom, dateTo, page],
    queryFn: () => assetTransactions.list({
      q: q || undefined,
      types: typesParam.length ? typesParam : undefined,
      groupIds: groupsParam.length ? groupsParam : undefined,
      sources: sourcesParam.length ? sourcesParam : undefined,
      dateFrom: dateFrom || undefined,
      dateTo: dateTo || undefined,
      limit: PAGE_SIZE,
      offset: page * PAGE_SIZE,
    }),
    staleTime: 1000 * 30,
  })

  const items: AssetTransactionLogItem[] = data?.items ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const groups = groupsList ?? []

  const hasFilters = q || selectedTypes.size > 0 || selectedGroups.size > 0
    || selectedSources.size > 0 || dateFrom || dateTo

  function clearFilters() {
    setQ('')
    setSelectedTypes(new Set())
    setSelectedGroups(new Set())
    setSelectedSources(new Set())
    setDateFrom('')
    setDateTo('')
    setPage(0)
  }

  function toggle(set: Set<string>, setter: (s: Set<string>) => void, value: string) {
    const next = new Set(set)
    if (next.has(value)) next.delete(value)
    else next.add(value)
    setter(next)
    setPage(0)
  }

  return (
    <div>
      <PageHeader
        section={t('nav.groupAnalysis')}
        title={t('events.title')}
        action={
          <div className="flex items-center gap-3">
            {total > 0 && (
              <p className="text-xs text-muted-foreground">
                {total.toLocaleString(locale)} {t('events.eventsTotal')}
              </p>
            )}
            <Button
              variant="outline"
              size="sm"
              disabled={syncDividendsMutation.isPending}
              onClick={() => syncDividendsMutation.mutate()}
              className="gap-1.5"
            >
              <RefreshCw size={13} className={syncDividendsMutation.isPending ? 'animate-spin' : ''} />
              {syncDividendsMutation.isPending ? t('events.syncing') : t('events.syncDividends')}
            </Button>
          </div>
        }
      />

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-2 mb-4">
        <div className="relative">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground" />
          <Input
            value={q}
            onChange={(e) => { setQ(e.target.value); setPage(0) }}
            placeholder={t('events.searchPlaceholder')}
            className="pl-8 w-64"
          />
        </div>

        {/* Type filter */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="inline-flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg border border-border bg-card hover:bg-muted/50 text-foreground transition-colors">
              <Filter size={12} className="text-muted-foreground" />
              {t('events.type')}
              {selectedTypes.size > 0 && (
                <span className="bg-primary text-primary-foreground rounded-full px-1.5 text-[10px]">
                  {selectedTypes.size}
                </span>
              )}
              <ChevronDown size={12} className="text-muted-foreground" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="min-w-[200px]">
            <DropdownMenuLabel className="text-[10px] uppercase tracking-wider text-muted-foreground font-normal">
              {t('events.type')}
            </DropdownMenuLabel>
            {TX_TYPES.map(type => (
              <DropdownMenuCheckboxItem
                key={type}
                checked={selectedTypes.has(type)}
                onCheckedChange={() => toggle(selectedTypes, setSelectedTypes, type)}
                onSelect={(e) => e.preventDefault()}
              >
                {type}
              </DropdownMenuCheckboxItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>

        {/* Group / Carteira filter */}
        {groups.length > 0 && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button className="inline-flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg border border-border bg-card hover:bg-muted/50 text-foreground transition-colors">
                {t('events.wallet')}
                {selectedGroups.size > 0 && (
                  <span className="bg-primary text-primary-foreground rounded-full px-1.5 text-[10px]">
                    {selectedGroups.size}
                  </span>
                )}
                <ChevronDown size={12} className="text-muted-foreground" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start" className="min-w-[200px]">
              {groups.map((g: AssetGroup) => (
                <DropdownMenuCheckboxItem
                  key={g.id}
                  checked={selectedGroups.has(g.id)}
                  onCheckedChange={() => toggle(selectedGroups, setSelectedGroups, g.id)}
                  onSelect={(e) => e.preventDefault()}
                >
                  <span className="inline-block w-2 h-2 rounded-full mr-2"
                        style={{ backgroundColor: g.color }} />
                  {g.name}
                </DropdownMenuCheckboxItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        )}

        {/* Source filter */}
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <button className="inline-flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg border border-border bg-card hover:bg-muted/50 text-foreground transition-colors">
              {t('events.source')}
              {selectedSources.size > 0 && (
                <span className="bg-primary text-primary-foreground rounded-full px-1.5 text-[10px]">
                  {selectedSources.size}
                </span>
              )}
              <ChevronDown size={12} className="text-muted-foreground" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="min-w-[180px]">
            {['manual', 'csv_import', 'yfinance', 'sync'].map(src => (
              <DropdownMenuCheckboxItem
                key={src}
                checked={selectedSources.has(src)}
                onCheckedChange={() => toggle(selectedSources, setSelectedSources, src)}
                onSelect={(e) => e.preventDefault()}
              >
                {src}
              </DropdownMenuCheckboxItem>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>

        {/* Date range */}
        <Popover>
          <PopoverTrigger asChild>
            <button className="inline-flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded-lg border border-border bg-card hover:bg-muted/50 text-foreground transition-colors">
              {t('events.dateRange')}
              {(dateFrom || dateTo) && (
                <span className="bg-primary text-primary-foreground rounded-full px-1.5 text-[10px]">•</span>
              )}
              <ChevronDown size={12} className="text-muted-foreground" />
            </button>
          </PopoverTrigger>
          <PopoverContent align="start" className="w-auto p-4 space-y-3">
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs">{t('events.from')}</Label>
              <DatePickerInput value={dateFrom} onChange={(v) => { setDateFrom(v); setPage(0) }} />
            </div>
            <div className="flex flex-col gap-1.5">
              <Label className="text-xs">{t('events.to')}</Label>
              <DatePickerInput value={dateTo} onChange={(v) => { setDateTo(v); setPage(0) }} />
            </div>
            {(dateFrom || dateTo) && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => { setDateFrom(''); setDateTo(''); setPage(0) }}
              >
                {t('events.clear')}
              </Button>
            )}
          </PopoverContent>
        </Popover>

        {hasFilters && (
          <button
            onClick={clearFilters}
            className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
          >
            <X size={12} /> {t('events.clearAll')}
          </button>
        )}
      </div>

      {/* Table */}
      <div className="bg-card rounded-xl border border-border shadow-sm overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead className="bg-muted/40 border-b border-border">
              <tr>
                <th className="text-left px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.date')}</th>
                <th className="text-left px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.type')}</th>
                <th className="text-left px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.asset')}</th>
                <th className="text-left px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.wallet')}</th>
                <th className="text-right px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.qty')}</th>
                <th className="text-right px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.price')}</th>
                <th className="text-right px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.value')}</th>
                <th className="text-left px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.source')}</th>
                <th className="text-left px-4 py-2.5 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">{t('events.notes')}</th>
              </tr>
            </thead>
            <tbody>
              {isLoading ? (
                Array.from({ length: 8 }).map((_, i) => (
                  <tr key={i} className="border-b border-border last:border-0">
                    <td colSpan={9} className="px-4 py-3">
                      <Skeleton className="h-5 w-full" />
                    </td>
                  </tr>
                ))
              ) : items.length === 0 ? (
                <tr>
                  <td colSpan={9} className="text-center text-muted-foreground py-12 text-sm">
                    {hasFilters ? t('events.emptyFiltered') : t('events.empty')}
                  </td>
                </tr>
              ) : (
                items.map((tx) => (
                  <tr key={tx.id} className="border-b border-border last:border-0 hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-2.5 whitespace-nowrap text-foreground">
                      {fmtDate(tx.date, locale)}
                    </td>
                    <td className="px-4 py-2.5">
                      <Badge variant="outline" className={TYPE_BADGE[tx.type] ?? ''}>
                        {tx.type}
                      </Badge>
                    </td>
                    <td className="px-4 py-2.5">
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="font-medium text-foreground truncate">{tx.asset.name}</span>
                        {tx.asset.ticker && (
                          <span className="text-[10px] text-muted-foreground">{tx.asset.ticker}</span>
                        )}
                      </div>
                    </td>
                    <td className="px-4 py-2.5">
                      {tx.asset.group_name && (
                        <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
                          <span className="inline-block w-2 h-2 rounded-full"
                                style={{ backgroundColor: tx.asset.group_color ?? '#999' }} />
                          {tx.asset.group_name}
                        </span>
                      )}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums text-foreground">
                      {fmtQty(tx.qty)}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums text-foreground">
                      {privacyMode ? MASK : fmtCurrency(tx.price, tx.asset.currency || userCurrency, locale)}
                    </td>
                    <td className="px-4 py-2.5 text-right tabular-nums font-medium text-foreground">
                      {privacyMode ? MASK : fmtCurrency(tx.value, tx.asset.currency || userCurrency, locale)}
                    </td>
                    <td className="px-4 py-2.5">
                      <span className="text-[10px] text-muted-foreground bg-muted/50 px-1.5 py-0.5 rounded">
                        {tx.source}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 text-muted-foreground text-xs max-w-xs truncate" title={tx.notes ?? undefined}>
                      {mask(tx.notes ?? '')}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {total > PAGE_SIZE && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-border bg-muted/20">
            <p className="text-xs text-muted-foreground">
              {(page * PAGE_SIZE + 1).toLocaleString(locale)}–
              {Math.min((page + 1) * PAGE_SIZE, total).toLocaleString(locale)} {t('events.of')} {total.toLocaleString(locale)}
            </p>
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="sm"
                disabled={page === 0}
                onClick={() => setPage(p => Math.max(0, p - 1))}
              >
                <ChevronLeft size={14} /> {t('events.prev')}
              </Button>
              <span className="text-xs text-muted-foreground px-2">
                {page + 1} / {totalPages}
              </span>
              <Button
                variant="ghost"
                size="sm"
                disabled={page + 1 >= totalPages}
                onClick={() => setPage(p => p + 1)}
              >
                {t('events.next')} <ChevronRight size={14} />
              </Button>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
