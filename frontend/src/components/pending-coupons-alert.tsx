/**
 * Alerta de cupons semestrais do Tesouro pendentes de lançamento.
 *
 * O Securo não consegue puxar esses créditos sozinho (ver
 * tesouro_coupon_service no backend), então em vez de calcular o valor —
 * e errar centavos por causa do VNA/IPCA — a gente só avisa que a data
 * chegou e deixa o usuário digitar o valor exato do extrato.
 *
 * Não renderiza nada quando não há pendências, pra não ocupar espaço no
 * dashboard no outro 99% do ano (são ~2 cupons por título por ano).
 */
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { toast } from 'sonner'
import { CalendarClock } from 'lucide-react'
import { assets } from '@/lib/api'
import type { PendingCoupon } from '@/lib/api'
import { Button } from '@/components/ui/button'
import { CurrencyInput } from '@/components/ui/currency-input'

interface PendingCouponsAlertProps {
  locale: string
}

function formatDate(iso: string, locale: string): string {
  return new Date(iso + 'T00:00:00').toLocaleDateString(locale)
}

export function PendingCouponsAlert({ locale }: PendingCouponsAlertProps) {
  const qc = useQueryClient()
  const { data: coupons } = useQuery<PendingCoupon[]>({
    queryKey: ['pending-coupons'],
    queryFn: () => assets.pendingCoupons(),
    staleTime: 1000 * 60 * 30,
  })

  // Valor digitado por cupom, indexado por "assetId|couponDate" — cada
  // linha tem seu próprio input porque o usuário pode ter mais de um
  // título com cupom vencido no mesmo período. Guardamos a string crua
  // que o CurrencyInput devolve e convertemos só no envio.
  const [drafts, setDrafts] = useState<Record<string, string>>({})

  const recordMutation = useMutation({
    mutationFn: ({ c, value }: { c: PendingCoupon; value: number }) =>
      assets.addTransaction(c.asset_id, {
        date: c.coupon_date,
        type: 'INTEREST',
        value,
        notes: 'Cupom semestral',
        // Idempotente: reenviar o mesmo cupom não duplica o lançamento.
        external_id: `td-juros-${c.asset_id}-${c.coupon_date}`,
      }),
    onSuccess: () => {
      toast.success('Cupom lançado')
      // O pendente some da lista assim que o INTEREST existe; as views de
      // patrimônio/investimentos também mudam, então revalida as duas.
      qc.invalidateQueries({ queryKey: ['pending-coupons'] })
      qc.invalidateQueries({ queryKey: ['assets'] })
      qc.invalidateQueries({ queryKey: ['dashboard'] })
    },
    onError: () => toast.error('Erro ao lançar cupom'),
  })

  if (!coupons || coupons.length === 0) return null

  return (
    <div className="bg-amber-500/5 border border-amber-500/30 rounded-xl mb-5">
      <div className="px-5 py-4 border-b border-amber-500/20 flex items-center gap-2">
        <CalendarClock size={16} className="text-amber-600 shrink-0" />
        <div>
          <p className="text-sm font-semibold text-foreground">
            {coupons.length === 1
              ? 'Cupom do Tesouro a lançar'
              : `${coupons.length} cupons do Tesouro a lançar`}
          </p>
          <p className="text-xs text-muted-foreground mt-0.5">
            A data de pagamento chegou. Confira o valor creditado no extrato e lance aqui —
            o Securo não consegue puxar esse crédito automaticamente.
          </p>
        </div>
      </div>

      <div className="divide-y divide-amber-500/15">
        {coupons.map(c => {
          const key = `${c.asset_id}|${c.coupon_date}`
          const raw = drafts[key] ?? ''
          const parsed = parseFloat(raw)
          const value = Number.isFinite(parsed) ? parsed : null
          const pending = recordMutation.isPending
          return (
            <div key={key} className="px-5 py-3 flex flex-wrap items-center gap-3">
              <div className="flex-1 min-w-[200px]">
                <p className="text-sm font-medium text-foreground">{c.asset_name}</p>
                <p className="text-xs text-muted-foreground">
                  Cupom de {formatDate(c.coupon_date, locale)}
                  {c.days_late > 0 && ` · ${c.days_late} dia${c.days_late > 1 ? 's' : ''} atrás`}
                </p>
              </div>
              <CurrencyInput
                currency={c.currency}
                value={raw}
                onChange={(v) => setDrafts(prev => ({ ...prev, [key]: v }))}
                placeholder="Valor recebido"
                className="w-40"
              />
              <Button
                size="sm"
                disabled={pending || value == null || value <= 0}
                onClick={() => recordMutation.mutate({ c, value: value as number })}
              >
                {pending ? 'Lançando...' : 'Lançar'}
              </Button>
            </div>
          )
        })}
      </div>
    </div>
  )
}
