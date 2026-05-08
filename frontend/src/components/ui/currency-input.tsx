/**
 * CurrencyInput — locale-aware money input.
 *
 * Wraps `react-number-format`'s NumericFormat (industry-standard React
 * library for masked numeric inputs — battle-tested for cursor
 * preservation, paste handling, IME composition, and mobile keyboards).
 *
 * Why a wrapper?
 * - The rest of the codebase passes raw decimal strings around
 *   ("49051.94") so the backend payloads stay locale-agnostic. We expose
 *   that contract via `value: string` / `onChange(raw: string)`.
 * - Separator selection happens here (BR-style for BRL, US-style for
 *   USD/EUR/etc.) so the consumer just passes `currency`.
 *
 * Previous home-rolled version was reformatting on every keystroke and
 * imperatively setting `el.value`, which clashed with React's controlled
 * inputs — typing a decimal flipped the separator back and forth and
 * jumped the cursor around. NumericFormat handles all of that for us.
 */
import { NumericFormat, type NumberFormatValues } from 'react-number-format'

interface Props {
  value: string                        // raw, e.g. "49051.94"
  onChange: (raw: string) => void
  currency?: string                    // 'BRL' | 'USD' | … (drives separators)
  placeholder?: string
  className?: string
  disabled?: boolean
  id?: string
  onBlur?: () => void
}

/** BRL uses '.' thousand + ',' decimal; everything else uses ',' + '.'. */
function separators(currency: string): { thousand: string; decimal: string } {
  if (currency === 'BRL') return { thousand: '.', decimal: ',' }
  return { thousand: ',', decimal: '.' }
}

const DEFAULT_CLASS = 'flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm transition-colors file:border-0 file:bg-transparent file:text-sm file:font-medium placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-50'

export function CurrencyInput({
  value,
  onChange,
  currency = 'BRL',
  placeholder,
  className,
  disabled,
  id,
  onBlur,
}: Props) {
  const { thousand, decimal } = separators(currency)
  // NumericFormat works with the raw float as `value`. We accept a string
  // (consistent with our other form fields) and convert at the boundary.
  const numericValue = value === '' || value == null ? '' : value

  return (
    <NumericFormat
      // Display config: locale-appropriate separators, up to 8 decimals
      // (crypto needs more than 2; the per-currency decimal scale is
      // configurable later if we ever want strict 2-digit BRL etc.).
      thousandSeparator={thousand}
      decimalSeparator={decimal}
      decimalScale={8}
      // Don't force 2 decimals — let the user type "49051" without seeing
      // ".00" appended automatically (annoying when filling many fields).
      fixedDecimalScale={false}
      allowNegative={true}
      // Controlled value: pass the raw string in, get the raw string out
      // via onValueChange (NumericFormat's standard event for getting at
      // the parsed `value` rather than the formatted display).
      value={numericValue}
      onValueChange={(values: NumberFormatValues) => {
        // values.value is the raw decimal-string ("49051.94"); ideal
        // for our backend contract. values.floatValue would coerce to
        // number which loses precision for crypto.
        onChange(values.value)
      }}
      onBlur={onBlur}
      placeholder={placeholder}
      className={className ?? DEFAULT_CLASS}
      disabled={disabled}
      id={id}
      // inputMode="decimal" surfaces the right keyboard on mobile.
      inputMode="decimal"
    />
  )
}
