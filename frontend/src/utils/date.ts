const DEFAULT_LOCALE = import.meta.env.VITE_APP_LOCALE || 'en-IN'
const DEFAULT_TIMEZONE = import.meta.env.VITE_SERVER_TIMEZONE || 'Asia/Kolkata'

/**
 * Formats a timestamp string, Date object, or epoch number using environment locale and timezone.
 */
export const formatConfiguredDateTime = (
  timestamp: string | number | Date | null | undefined,
  options: Intl.DateTimeFormatOptions = {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
  }
): string => {
  if (!timestamp) return '-'
  try {
    const date = typeof timestamp === 'string' || typeof timestamp === 'number'
      ? new Date(timestamp)
      : timestamp

    return date.toLocaleString(DEFAULT_LOCALE, {
      timeZone: DEFAULT_TIMEZONE,
      ...options,
    })
  } catch {
    return String(timestamp)
  }
}