// Discrete USD/BRL header badge — lets the user verify which dollar
// rate the portfolio consolidation is using. Hidden when the FX table
// is empty or the API errors out, so it never shows misleading info.
import { useQuery } from '@tanstack/react-query'
import api from '@/lib/api'

export function FxBadge() {
  const { data } = useQuery({
    queryKey: ['fx-current', 'BRL'],
    queryFn: async () => {
      const r = await api.get('/fx-rates/current?quote=BRL')
      return r.data as {
        rate: number | null
        date: string | null
        source: string | null
      }
    },
    // Cheap call (single-row read), so we always re-fetch on mount
    // and on window focus. Avoids the badge silently sitting on a
    // stale rate after the daily Celery sync runs.
    staleTime: 0,
    refetchOnMount: 'always',
    refetchOnWindowFocus: true,
  })

  if (!data || data.rate == null) return null

  const dateStr = data.date
    ? new Date(data.date + 'T00:00:00').toLocaleDateString('pt-BR', {
        day: '2-digit',
        month: '2-digit',
      })
    : ''

  return (
    <div
      className="text-[11px] text-muted-foreground tabular-nums whitespace-nowrap"
      title={`Cotação USD/BRL usada nos cálculos. Fonte: ${data.source || 'BCB PTAX'} • Data: ${data.date || ''}`}
    >
      USD/BRL: <span className="font-semibold text-foreground">
        R$ {data.rate.toFixed(4).replace('.', ',')}
      </span>
      {dateStr && <span className="text-muted-foreground/70"> · {dateStr}</span>}
    </div>
  )
}
