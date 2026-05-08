/**
 * CurrencyInput — locale-aware money input with live thousand/decimal
 * separators that follow the asset's selected currency.
 *
 * Why not <input type="number">? It always treats period as decimal and
 * cannot show thousand separators. A user typing "49.051,94" (the BR
 * convention for forty-nine thousand fifty-one and ninety-four cents)
 * was seeing the input parse it as 49.05194 (forty-nine point zero five),
 * silently giving a 1000× misread on every price.
 *
 * This component:
 * - Stores the raw numeric string in the parent's state (e.g. "49051.94")
 *   so all backend payloads stay locale-agnostic.
 * - Displays it formatted with the locale appropriate to the chosen
 *   currency (BRL → pt-BR; USD/EUR/etc. → en-US).
 * - Reformats on every keystroke without resetting cursor position by
 *   counting digits to the right of the cursor and re-anchoring after
 *   reformat.
 * - Accepts both `.` and `,` as decimal separators while typing so
 *   muscle-memory from either locale doesn't fight the user.
 */
import { useRef, useEffect, useCallback } from 'react'

interface Props {
  value: string                        // raw, e.g. "49051.94"
  onChange: (raw: string) => void
  currency?: string                    // 'BRL' | 'USD' | … (drives separator choice)
  placeholder?: string
  className?: string
  step?: string
  disabled?: boolean
  id?: string
  onBlur?: () => void
}

function localeForCurrency(currency: string): string {
  // Currencies that use BR-style formatting (period thousands, comma decimal).
  // Add to this list as more BR-region currencies show up.
  if (currency === 'BRL') return 'pt-BR'
  return 'en-US'
}

function separators(locale: string): { thousand: string; decimal: string } {
  if (locale === 'pt-BR') return { thousand: '.', decimal: ',' }
  return { thousand: ',', decimal: '.' }
}

/**
 * Convert raw "49051.94" into formatted display per locale.
 * Empty / invalid values pass through as-is so the user can keep typing.
 */
function formatRaw(raw: string, locale: string): string {
  if (!raw) return ''
  // Allow trailing decimal sep ("49051." while user is mid-typing)
  const num = parseFloat(raw)
  if (isNaN(num)) return raw
  // Preserve up to 8 decimals (crypto needs more than 2)
  const decPart = raw.includes('.') ? raw.split('.')[1] : ''
  const minFrac = 0
  const maxFrac = Math.min(Math.max(decPart.length, 0), 8)
  return new Intl.NumberFormat(locale, {
    minimumFractionDigits: minFrac,
    maximumFractionDigits: maxFrac,
  }).format(num)
}

/**
 * Convert a user-typed formatted string back to raw "49051.94".
 * Strips thousand separators and normalises the decimal point.
 */
function parseFormatted(s: string, locale: string): string {
  const { thousand, decimal } = separators(locale)
  // Drop everything except digits, the two separators, and minus.
  let cleaned = ''
  for (const ch of s) {
    if (/\d/.test(ch) || ch === thousand || ch === decimal || ch === '-') {
      cleaned += ch
    }
  }
  // Now strip thousand separators and normalise decimal to '.'.
  cleaned = cleaned.split(thousand).join('')
  cleaned = cleaned.replace(decimal, '.')
  // Allow alternate decimal: if user types '.' in BR mode (or ',' in US),
  // accept it after we've already converted the canonical separator.
  // This means "49051.94" in BR mode still parses (period is treated as
  // a stray, becomes the decimal sep on second pass).
  if (!cleaned.includes('.')) {
    const altDecimal = decimal === ',' ? '.' : ','
    if (s.includes(altDecimal)) {
      const lastIdx = s.lastIndexOf(altDecimal)
      // Only treat the alt as decimal if there's exactly one occurrence —
      // otherwise it's a thousand separator from the other locale.
      if (s.indexOf(altDecimal) === lastIdx) {
        cleaned = ''
        for (const ch of s) {
          if (/\d/.test(ch) || ch === '-') cleaned += ch
          else if (ch === altDecimal) cleaned += '.'
        }
      }
    }
  }
  return cleaned
}

export function CurrencyInput({
  value,
  onChange,
  currency = 'BRL',
  placeholder,
  className,
  step,
  disabled,
  id,
  onBlur,
}: Props) {
  const inputRef = useRef<HTMLInputElement>(null)
  const locale = localeForCurrency(currency)

  // Sync displayed text when the parent's raw value changes externally
  // (e.g. openEdit populating from an Asset).
  useEffect(() => {
    if (!inputRef.current) return
    if (document.activeElement === inputRef.current) return  // don't yank focus mid-type
    inputRef.current.value = formatRaw(value, locale)
  }, [value, locale])

  const handleInput = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const el = e.target
    const oldText = el.value
    const cursor = el.selectionStart ?? oldText.length

    // Number of digits to the right of the cursor in the OLD text — use
    // this as the anchor point when we re-set the cursor after reformat.
    const digitsRightOfCursor = oldText.slice(cursor).replace(/\D/g, '').length

    const raw = parseFormatted(oldText, locale)
    onChange(raw)

    const newText = formatRaw(raw, locale)
    // If the user just typed the decimal separator and the parsed raw
    // ends with a trailing dot ("49051."), formatRaw drops it because
    // parseFloat collapses it. Re-append so they can keep typing.
    const needsTrailingDecimal =
      raw.endsWith('.')
      && !newText.includes(separators(locale).decimal)
    const finalText = needsTrailingDecimal
      ? newText + separators(locale).decimal
      : newText

    el.value = finalText

    // Re-anchor cursor by digits-from-right.
    if (digitsRightOfCursor === 0) {
      el.setSelectionRange(finalText.length, finalText.length)
      return
    }
    let count = 0
    let pos = finalText.length
    for (let i = finalText.length - 1; i >= 0; i--) {
      if (/\d/.test(finalText[i])) {
        count++
        if (count === digitsRightOfCursor) {
          pos = i
          break
        }
      }
    }
    el.setSelectionRange(pos, pos)
  }, [locale, onChange])

  return (
    <input
      ref={inputRef}
      type="text"
      inputMode="decimal"
      defaultValue={formatRaw(value, locale)}
      onChange={handleInput}
      onBlur={onBlur}
      placeholder={placeholder}
      className={className ?? 'flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50'}
      disabled={disabled}
      id={id}
      data-step={step}
    />
  )
}
