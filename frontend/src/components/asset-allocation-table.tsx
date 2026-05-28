/**
 * "Onde Aportar" — global asset-allocation widget for the dashboard.
 *
 * Lets the user set a target percentage per asset class and reads back
 * which buckets are below target and how much they need to receive (in
 * R$ and in % of the total deficit) to rebalance. Replaces the monthly
 * growth bar chart that previously occupied this slot.
 *
 * The math runs on the backend (see asset_allocation_service); the
 * component is a thin editor over the targets dict in user.preferences.
 *
 * Bastter-style layout: alternating rows, the "% atual − % alvo" delta
 * colored as a +/- chip on the right, a green/rose hint on rows that
 * need attention. The "Onde Aportar" pair (R$ + %) only renders when a
 * row is below target — otherwise we leave the cells blank to avoid
 * implying "sell this".
 */
import { useEffect, useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { assetAllocation } from '@/lib/api'
import type { AssetAllocationResponse, AportePlanResponse } from '@/lib/api'
import { Button } from '@/components/ui/button'

interface AssetAllocationTableProps {
  locale: string
  mask: (s: string) => string
}

function fmtMoney(v: number, currency: string, locale: string): string {
  return new Intl.NumberFormat(locale, {
    style: 'currency',
    currency,
    maximumFractionDigits: 0,
  }).format(v)
}

function fmtPct(v: number, digits = 2): string {
  return `${v.toFixed(digits)}%`
}

export function AssetAllocationTable({ locale, mask }: AssetAllocationTableProps) {
  const qc = useQueryClient()
  const { data, isLoading } = useQuery<AssetAllocationResponse>({
    queryKey: ['asset-allocation'],
    queryFn: () => assetAllocation.get(),
    staleTime: 1000 * 60,
  })

  // Local edit state, indexed by bucket id. Seeded from the server payload
  // on first arrival and kept in sync if the user saves (causing a refetch).
  const [drafts, setDrafts] = useState<Record<string, string>>({})
  useEffect(() => {
    if (!data) return
    const seed: Record<string, string> = {}
    for (const c of data.categories) {
      // Empty string when target is exactly 0 so the input doesn't show "0"
      // and force the user to clear it before typing.
      seed[c.id] = c.target_pct > 0 ? c.target_pct.toString() : ''
    }
    setDrafts(seed)
  }, [data])

  const saveMutation = useMutation({
    mutationFn: (targets: Record<string, number>) => assetAllocation.saveTargets(targets),
    onSuccess: () => {
      toast.success('Metas salvas')
      qc.invalidateQueries({ queryKey: ['asset-allocation'] })
    },
    onError: () => toast.error('Erro ao salvar metas'),
  })

  // Live recomputed totals reflecting the user's in-progress edits — we
  // want the "soma 100%" indicator to update as they type, without
  // hitting the server until they hit Salvar.
  const draftSum = useMemo(() => {
    return Object.values(drafts).reduce((sum, raw) => {
      const n = parseFloat((raw || '0').replace(',', '.'))
      return sum + (isFinite(n) ? n : 0)
    }, 0)
  }, [drafts])

  const sumOk = Math.abs(draftSum - 100) < 0.01
  const currency = data?.primary_currency ?? 'BRL'

  function setDraft(id: string, raw: string) {
    setDrafts(prev => ({ ...prev, [id]: raw }))
  }

  function handleSave() {
    const targets: Record<string, number> = {}
    for (const [id, raw] of Object.entries(drafts)) {
      const n = parseFloat((raw || '0').replace(',', '.'))
      targets[id] = isFinite(n) ? n : 0
    }
    saveMutation.mutate(targets)
  }

  // "Tenho R$ X pra aportar" planner. Debounce the input so we don't fire
  // a request per keystroke, then ask the backend to split the contribution
  // and report the REAL resulting allocation (computed vs the post-aporte
  // total — see asset_allocation_service.compute_aporte_plan).
  const [aporteRaw, setAporteRaw] = useState('')
  const [aporteDebounced, setAporteDebounced] = useState(0)
  useEffect(() => {
    const n = parseFloat((aporteRaw || '0').replace(/\./g, '').replace(',', '.'))
    const amount = isFinite(n) ? n : 0
    const h = setTimeout(() => setAporteDebounced(amount), 350)
    return () => clearTimeout(h)
  }, [aporteRaw])
  const { data: aportePlan } = useQuery<AportePlanResponse>({
    queryKey: ['aporte-plan', aporteDebounced],
    queryFn: () => assetAllocation.aportePlan(aporteDebounced),
    enabled: aporteDebounced > 0,
    staleTime: 1000 * 30,
  })

  if (isLoading || !data) {
    return (
      <div className="bg-card rounded-xl border border-border shadow-sm p-5">
        <div className="h-64 animate-pulse bg-muted/30 rounded-lg" />
      </div>
    )
  }

  // Recompute deficits live based on draft targets, so the user can see
  // the "Onde Aportar" column update as they type even before Salvar.
  // Falls back to backend numbers if total or drafts are inconsistent.
  const liveRows = data.categories.map(c => {
    const draftRaw = drafts[c.id] ?? ''
    const draftTarget = parseFloat((draftRaw || '0').replace(',', '.'))
    const tgt = isFinite(draftTarget) ? draftTarget : c.target_pct
    const deficit = Math.max(0, (tgt / 100) * data.total_brl - c.total_brl)
    return { ...c, target_pct_live: tgt, deficit_live: deficit }
  })
  const liveDeficitTotal = liveRows.reduce((s, r) => s + r.deficit_live, 0)

  return (
    <div className="bg-card rounded-xl border border-border shadow-sm">
      <div className="px-5 py-4 border-b border-border flex items-center justify-between">
        <div>
          <p className="text-sm font-semibold text-foreground">Onde Aportar</p>
          <p className="text-xs text-muted-foreground mt-0.5">
            Defina seu objetivo por classe — o sistema indica quanto aportar pra rebalancear.
            Visão global (todas as carteiras).
          </p>
        </div>
        <div className="text-right">
          <p className="text-xs text-muted-foreground">Patrimônio total</p>
          <p className="text-base font-bold tabular-nums">
            {mask(fmtMoney(data.total_brl, currency, locale))}
          </p>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-b border-border bg-muted/30">
            <tr className="text-xs uppercase text-muted-foreground">
              <th className="text-left px-5 py-3 font-medium">Classe</th>
              <th className="text-right px-3 py-3 font-medium">Total</th>
              <th className="text-right px-3 py-3 font-medium">% atual</th>
              <th className="text-right px-3 py-3 font-medium">% alvo</th>
              <th className="text-right px-3 py-3 font-medium">Δ pp</th>
              <th className="text-right px-3 py-3 font-medium">Aportar R$</th>
              <th className="text-right px-5 py-3 font-medium">% do aporte</th>
            </tr>
          </thead>
          <tbody>
            {liveRows.map((c, i) => {
              const isBelowTarget = c.deficit_live > 0
              const aporteShare = liveDeficitTotal > 0
                ? (c.deficit_live / liveDeficitTotal) * 100
                : 0
              return (
                <tr
                  key={c.id}
                  className={`border-b border-border last:border-b-0 ${
                    i % 2 === 0 ? '' : 'bg-muted/10'
                  } ${isBelowTarget ? 'bg-emerald-500/5' : ''}`}
                >
                  <td className="px-5 py-3 font-medium text-foreground">{c.label}</td>
                  <td className="text-right px-3 py-3 tabular-nums">
                    {mask(fmtMoney(c.total_brl, currency, locale))}
                  </td>
                  <td className="text-right px-3 py-3 tabular-nums text-muted-foreground">
                    {fmtPct(c.current_pct)}
                  </td>
                  <td className="text-right px-3 py-3 tabular-nums">
                    <input
                      type="text"
                      inputMode="decimal"
                      className="w-20 text-right bg-card border border-border focus:outline-none focus:ring-2 focus:ring-primary px-2 py-1 rounded text-sm tabular-nums"
                      value={drafts[c.id] ?? ''}
                      onChange={(e) => setDraft(c.id, e.target.value)}
                      placeholder="0"
                    />
                    <span className="text-muted-foreground ml-1">%</span>
                  </td>
                  <td className="text-right px-3 py-3 tabular-nums">
                    {(() => {
                      const delta = c.current_pct - c.target_pct_live
                      // Δ pp: positive = above target (we don't want to aporte here),
                      // negative = below target. Color matches the Bastter convention.
                      if (Math.abs(delta) < 0.005) return <span className="text-muted-foreground">0,00pp</span>
                      const cls = delta >= 0 ? 'text-rose-600 bg-rose-500/10' : 'text-emerald-600 bg-emerald-500/10'
                      return (
                        <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${cls}`}>
                          {delta >= 0 ? '+' : ''}{delta.toFixed(2)}pp
                        </span>
                      )
                    })()}
                  </td>
                  <td className="text-right px-3 py-3 tabular-nums font-medium">
                    {isBelowTarget
                      ? <span className="text-emerald-700">{mask(fmtMoney(c.deficit_live, currency, locale))}</span>
                      : <span className="text-muted-foreground">—</span>}
                  </td>
                  <td className="text-right px-5 py-3 tabular-nums font-medium">
                    {isBelowTarget
                      ? <span className="text-emerald-700">{aporteShare.toFixed(1)}%</span>
                      : <span className="text-muted-foreground">—</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
          <tfoot className="border-t border-border bg-muted/20">
            <tr className="font-semibold">
              <td className="px-5 py-3">Total</td>
              <td className="text-right px-3 py-3 tabular-nums">
                {mask(fmtMoney(data.total_brl, currency, locale))}
              </td>
              <td className="text-right px-3 py-3 tabular-nums">
                {fmtPct(liveRows.reduce((s, r) => s + r.current_pct, 0))}
              </td>
              <td className="text-right px-3 py-3 tabular-nums">
                <span className={`inline-block px-2 py-0.5 rounded text-xs ${
                  sumOk ? 'bg-emerald-500/10 text-emerald-700'
                        : 'bg-amber-500/10 text-amber-700'
                }`}>
                  {fmtPct(draftSum)}
                </span>
              </td>
              <td></td>
              <td className="text-right px-3 py-3 tabular-nums text-emerald-700">
                {mask(fmtMoney(liveDeficitTotal, currency, locale))}
              </td>
              <td className="text-right px-5 py-3 text-muted-foreground text-xs">
                aporte total pra meta
              </td>
            </tr>
          </tfoot>
        </table>
      </div>

      <div className="px-5 py-3 border-t border-border flex items-center justify-between gap-3">
        <p className="text-xs text-muted-foreground">
          {sumOk
            ? '✓ Soma dos % alvo = 100%'
            : `⚠ Soma dos % alvo = ${draftSum.toFixed(2)}% (deve ser 100% pra meta ser consistente)`}
        </p>
        <Button
          onClick={handleSave}
          disabled={saveMutation.isPending}
          size="sm"
        >
          {saveMutation.isPending ? 'Salvando...' : 'Salvar metas'}
        </Button>
      </div>

      {/* "Tenho R$ X pra aportar" — distributes a one-shot contribution and
          shows the REAL resulting allocation (% computed against the
          post-aporte total, not the static deficit). */}
      <div className="px-5 py-4 border-t border-border bg-muted/20 rounded-b-xl">
        <div className="flex items-center gap-3 flex-wrap">
          <label className="text-sm font-medium text-foreground">
            Tenho pra aportar:
          </label>
          <div className="flex items-center gap-1.5">
            <span className="text-sm text-muted-foreground">R$</span>
            <input
              type="text"
              inputMode="decimal"
              value={aporteRaw}
              onChange={e => setAporteRaw(e.target.value)}
              placeholder="50.000"
              className="w-32 rounded-md border border-border bg-background px-2 py-1 text-sm tabular-nums focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>
          {aporteRaw && (
            <button
              onClick={() => setAporteRaw('')}
              className="text-xs text-muted-foreground hover:text-foreground"
            >
              limpar
            </button>
          )}
        </div>

        {aportePlan && aportePlan.aporte_brl > 0 && (
          <div className="mt-3 overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-xs uppercase text-muted-foreground">
                <tr>
                  <th className="text-left py-2 font-medium">Classe</th>
                  <th className="text-right px-3 py-2 font-medium">Aportar</th>
                  <th className="text-right px-3 py-2 font-medium">% do aporte</th>
                  <th className="text-right px-3 py-2 font-medium">% atual</th>
                  <th className="text-right px-3 py-2 font-medium">→ % após</th>
                  <th className="text-right py-2 font-medium">% alvo</th>
                </tr>
              </thead>
              <tbody>
                {aportePlan.categories
                  .filter(c => c.aporte_brl > 0.005)
                  .sort((a, b) => b.aporte_brl - a.aporte_brl)
                  .map(c => (
                    <tr key={c.id} className="border-t border-border/50">
                      <td className="py-2 text-foreground">{c.label}</td>
                      <td className="text-right px-3 py-2 tabular-nums font-medium text-emerald-700 dark:text-emerald-400">
                        {mask(fmtMoney(c.aporte_brl, currency, locale))}
                      </td>
                      <td className="text-right px-3 py-2 tabular-nums text-muted-foreground">
                        {fmtPct(c.aporte_share_pct, 1)}
                      </td>
                      <td className="text-right px-3 py-2 tabular-nums text-muted-foreground">
                        {fmtPct(c.current_pct)}
                      </td>
                      <td className="text-right px-3 py-2 tabular-nums font-medium">
                        {fmtPct(c.result_pct)}
                      </td>
                      <td className="text-right py-2 tabular-nums text-muted-foreground">
                        {fmtPct(c.target_pct, 0)}
                      </td>
                    </tr>
                  ))}
              </tbody>
            </table>
            <p className="mt-2 text-xs text-muted-foreground">
              Distribuído proporcional ao déficit (classes mais defasadas recebem mais).
              {aportePlan.remaining_deficit_brl > 1 && (
                <> Ainda faltariam {mask(fmtMoney(aportePlan.remaining_deficit_brl, currency, locale))} pra bater todas as metas.</>
              )}
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
