/**
 * Historify charts: the full charting engine over locally downloaded data.
 *
 * Everything drawn here comes out of the DuckDB store, never a broker. There is
 * no live feed and no order path anywhere on the page, and neither is switched
 * off by a setting: the feed implements no subscription and the chart component
 * has no order callback to pass. See `createHistorifyFeed` and `OpenAlgoChart`.
 *
 * The page owns instrument selection and the grid; each pane owns its own
 * chart. The symbol list is restricted to the catalog, which is the whole point
 * of the surface: it shows you what you hold, not what you could trade.
 */

import { useQuery } from '@tanstack/react-query'
import { ArrowLeft, RefreshCw } from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router'
import { historifyApi, historifyError } from '@/api/historify'
import { ChartGrid, ChartLayoutPicker } from '@/components/chart/ChartGrid'
import { HistorifyChartPane, type PaneState } from '@/components/historify/HistorifyChartPane'
import { Navbar } from '@/components/layout/Navbar'
import { Button } from '@/components/ui/button'
import { createHistorifyFeed } from '@/lib/chart/feeds/historifyFeed'
import { layoutById } from '@/lib/chart/layouts'
import { findCatalogSymbol, toCatalogSymbols } from '@/lib/historify/catalog'
import { availableIntervals } from '@/lib/historify/intervals'
import { showToast } from '@/utils/toast'

const LAYOUT_KEY = 'historify-charts-layout'
const PANES_KEY = 'historify-charts-panes'

const BLANK_PANE: PaneState = {
  symbol: '',
  exchange: 'NSE',
  interval: 'D',
  chartType: 'candlestick',
}

function readLayout(): string {
  try {
    return localStorage.getItem(LAYOUT_KEY) ?? 'single'
  } catch {
    return 'single'
  }
}

/**
 * One stored pane, with every field checked rather than asserted.
 *
 * `JSON.parse` returns `any`, and casting it to `PaneState` is a claim about
 * data this code did not write. Anything that survived a release that changed
 * the shape, or a hand-edited store, arrives here; a non-string `symbol` then
 * reaches `symbol.toUpperCase()` in the API layer and throws where nothing is
 * expecting a throw. A wrong field is replaced with the blank pane's value,
 * because a chart that opens on a default is a better answer than one that
 * does not open.
 */
export function toPaneState(value: unknown): PaneState {
  const row = (value ?? {}) as Record<string, unknown>
  const text = (field: unknown, fallback: string) =>
    typeof field === 'string' && field.length <= 64 ? field : fallback

  return {
    symbol: text(row.symbol, BLANK_PANE.symbol),
    exchange: text(row.exchange, BLANK_PANE.exchange),
    interval: text(row.interval, BLANK_PANE.interval),
    chartType: text(row.chartType, BLANK_PANE.chartType),
  }
}

function readPanes(): PaneState[] {
  try {
    const raw = localStorage.getItem(PANES_KEY)
    if (!raw) return []
    const parsed: unknown = JSON.parse(raw)
    // Capped at the largest grid so a tampered or stale store cannot make the
    // page build an unbounded number of panes.
    return Array.isArray(parsed) ? parsed.slice(0, 8).map(toPaneState) : []
  } catch {
    // A cleared or blocked store is not an error worth reporting. The page
    // simply opens on a blank pane, which is where a first visit starts anyway.
    return []
  }
}

export default function HistorifyCharts() {
  const navigate = useNavigate()
  const { symbol: urlSymbol } = useParams()
  const [searchParams] = useSearchParams()

  const handleLogout = async () => {
    try {
      await authApi.logout()
      logout()
      navigate('/login')
      showToast.success('Logged out successfully', 'historify')
    } catch {
      logout()
      navigate('/login')
    }
  }

  const handleModeToggle = async () => {
    const result = await toggleAppMode()
    if (result.success) {
      const newMode = useThemeStore.getState().appMode
      showToast.success(`Switched to ${newMode === 'live' ? 'Live' : 'Analyze'} mode`, 'historify')
    } else {
      showToast.error(result.message || 'Failed to toggle mode', 'historify')
    }
  }

  // State
  const [catalog, setCatalog] = useState<CatalogItem[]>([])
  const [isLoading, setIsLoading] = useState(false)
  const [isFullscreen, setIsFullscreen] = useState(false)

  // Symbol selection
  const [selectedSymbol, setSelectedSymbol] = useState(urlSymbol || '')
  const [selectedExchange, setSelectedExchange] = useState(searchParams.get('exchange') || 'NSE')
  const [selectedInterval, setSelectedInterval] = useState(searchParams.get('interval') || 'D')
  const [symbolSearchOpen, setSymbolSearchOpen] = useState(false)
  const [symbolSearch, setSymbolSearch] = useState('')

  // Custom interval state
  const [isCustomInterval, setIsCustomInterval] = useState(false)
  const [customIntervalValue, setCustomIntervalValue] = useState('25')
  const [customIntervalUnit, setCustomIntervalUnit] = useState<'m' | 'h' | 'W' | 'M' | 'Q' | 'Y'>(
    'm'
  )

  // Date range
  const [startDate, setStartDate] = useState(() => {
    const d = new Date()
    d.setMonth(d.getMonth() - 6)
    return d.toISOString().split('T')[0]
  })
  const [endDate, setEndDate] = useState(() => new Date().toISOString().split('T')[0])

  // Chart data
  const [chartData, setChartData] = useState<OHLCVData[]>([])
  const [dataInfo, setDataInfo] = useState<CatalogItem | null>(null)

  // Chart refs
  const chartContainerRef = useRef<HTMLDivElement>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const candleSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null)
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null)

  // Standard chart timeframes (not broker-specific)
  const CHART_TIMEFRAMES = [
    '1m',
    '2m',
    '3m',
    '5m',
    '10m',
    '15m',
    '30m',
    '1h',
    '2h',
    '4h',
    'D',
    'W',
    'M',
    'Q',
    'Y',
  ]

  // Effective interval (either selected standard interval or custom)
  const effectiveInterval = useMemo(() => {
    if (isCustomInterval) {
      // For W, MO, Q, Y with value 1, just use the unit (e.g., 'W', 'M')
      // For values > 1, use value + unit (e.g., '2W', '3MO')
      if (['W', 'M', 'Q', 'Y'].includes(customIntervalUnit)) {
        const val = parseInt(customIntervalValue, 10) || 1
        return val === 1 ? customIntervalUnit : `${val}${customIntervalUnit}`
      }
      return `${customIntervalValue}${customIntervalUnit}`
    }
    return selectedInterval
  }, [isCustomInterval, customIntervalValue, customIntervalUnit, selectedInterval])

  // Check if current interval is intraday (for chart display)
  const isIntradayInterval = useMemo(() => {
    const intradayPatterns = [
      '1s',
      '5s',
      '10s',
      '15s',
      '30s',
      '1m',
      '2m',
      '3m',
      '5m',
      '10m',
      '15m',
      '30m',
      '1h',
      '2h',
      '4h',
    ]
    if (intradayPatterns.includes(effectiveInterval)) return true
    // Custom intervals with 'm' or 'h' are intraday
    if (isCustomInterval && ['m', 'h'].includes(customIntervalUnit)) return true
    // W, MO, Q, Y are NOT intraday
    return false
  }, [effectiveInterval, isCustomInterval, customIntervalUnit])

  // Unique symbols from catalog
  const uniqueSymbols = useMemo(() => {
    const symbolMap = new Map<string, { symbol: string; exchange: string; intervals: string[] }>()
    catalog.forEach((item) => {
      const key = `${item.symbol}:${item.exchange}`
      if (!symbolMap.has(key)) {
        symbolMap.set(key, {
          symbol: item.symbol,
          exchange: item.exchange,
          intervals: [item.interval],
        })
      } else {
        symbolMap.get(key)!.intervals.push(item.interval)
      }
    })
    return Array.from(symbolMap.values())
  }, [catalog])

  // Filtered symbols for search
  const filteredSymbols = useMemo(() => {
    if (!symbolSearch) return uniqueSymbols
    const search = symbolSearch.toLowerCase()
    return uniqueSymbols.filter(
      (s) => s.symbol.toLowerCase().includes(search) || s.exchange.toLowerCase().includes(search)
    )
  }, [uniqueSymbols, symbolSearch])

  const TIMEZONE = import.meta.env.VITE_SERVER_TIMEZONE || 'Asia/Kolkata'
  const LOCALE = import.meta.env.VITE_APP_LOCALE || 'en-IN'

  // Load catalog on mount
  // biome-ignore lint/correctness/useExhaustiveDependencies: one-time catalog fetch on mount; loadCatalog is a non-memoized fetch helper and must not re-run on every render
  useEffect(() => {
    if (!catalog.isError) return
    showToast.error(
      historifyError(
        catalog.error,
        'Your downloaded data could not be listed. Reload the page, and check that OpenAlgo is still running if it keeps failing.'
      ),
      'historify'
    )
  }, [catalog.isError, catalog.error])

  useEffect(() => {
    try {
      localStorage.setItem(LAYOUT_KEY, layoutId)
    } catch {
      // Not remembering the layout is a small loss and never worth an error.
    }
  }, [layoutId])

  useEffect(() => {
    try {
      localStorage.setItem(PANES_KEY, JSON.stringify(panes))
    } catch {
      // As above.
    }
  }, [panes])

  const paneAt = useCallback((index: number) => panes[index] ?? BLANK_PANE, [panes])

  const updatePane = useCallback((index: number, next: Partial<PaneState>) => {
    setPanes((previous) => {
      const copy = [...previous]
      while (copy.length <= index) copy.push({ ...BLANK_PANE })
      copy[index] = { ...copy[index], ...next }
      return copy
    })
  }, [])

  /**
   * Adopt a deep link from the Historify manager, once the catalog is known.
   *
   * It waits for the catalog because the link carries no timeframe guarantee:
   * a symbol downloaded at 1m only cannot open on D, and landing on an empty
   * chart is a worse answer than landing on a timeframe it has.
   */
  const [linkApplied, setLinkApplied] = useState(false)
  useEffect(() => {
    if (!chartContainerRef.current) return

    // Small delay for container sizing
    const initTimer = setTimeout(() => {
      if (!chartContainerRef.current) return

      // Clean up existing chart
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }

      const container = chartContainerRef.current
      const containerWidth = container.offsetWidth || 800
      const containerHeight = container.offsetHeight || 600

      // Use the computed isIntradayInterval for chart formatting
      const isIntraday = isIntradayInterval

      // Check if this is a daily-aggregated interval (W, MO, Q, Y)
      // These intervals return timestamps that already represent IST dates
      const isDailyAggregated =
        isCustomInterval && ['W', 'M', 'Q', 'Y'].includes(customIntervalUnit)

      // Helper to format time/date dynamically based on interval
      const formatChartTime = (time: number) => {
        const date = new Date(time * 1000)
        
        // Daily-aggregated intervals are already aligned to UTC midnight by the backend
        const targetTimeZone = isDailyAggregated ? 'UTC' : TIMEZONE

        const options: Intl.DateTimeFormatOptions = {
          timeZone: targetTimeZone,
          year: '2-digit',
          month: '2-digit',
          day: '2-digit',
          hour12: false,
        }

        if (isIntraday) {
          options.hour = '2-digit'
          options.minute = '2-digit'
        }

        return new Intl.DateTimeFormat(LOCALE, options).format(date)
      }

      const chart = createChart(container, {
        width: containerWidth,
        height: Math.max(containerHeight, 500),
        layout: {
          background: { type: ColorType.Solid, color: 'transparent' },
          textColor: isDarkMode ? '#a6adbb' : '#333',
        },
        grid: {
          vertLines: {
            color: isDarkMode ? 'rgba(166, 173, 187, 0.1)' : 'rgba(0, 0, 0, 0.1)',
          },
          horzLines: {
            color: isDarkMode ? 'rgba(166, 173, 187, 0.1)' : 'rgba(0, 0, 0, 0.1)',
          },
        },
        localization: {
          timeFormatter: formatChartTime,
        },
        rightPriceScale: {
          borderColor: isDarkMode ? 'rgba(166, 173, 187, 0.2)' : 'rgba(0, 0, 0, 0.2)',
          scaleMargins: {
            top: 0.1,
            bottom: 0.25,
          },
        },
        timeScale: {
          borderColor: isDarkMode ? 'rgba(166, 173, 187, 0.2)' : 'rgba(0, 0, 0, 0.2)',
          timeVisible: isIntraday,
          secondsVisible: false,
          tickMarkFormatter: (time: number, tickMarkType: number) => {
            const date = new Date(time * 1000)
            const targetTimeZone = isDailyAggregated ? 'UTC' : TIMEZONE

            const formatter = new Intl.DateTimeFormat(LOCALE, {
              timeZone: targetTimeZone,
              year: 'numeric',
              month: 'short',
              day: 'numeric',
              hour: '2-digit',
              minute: '2-digit',
              hour12: false,
            })

            const parts = formatter.formatToParts(date)
            const getPart = (type: string) => parts.find((p) => p.type === type)?.value || ''

            if (isIntraday) {
              return `${getPart('hour')}:${getPart('minute')}`
            } else {
              if (tickMarkType === 0) return getPart('year')
              if (tickMarkType === 1) return getPart('month')
              return getPart('day')
            }
          },
        },
        crosshair: {
          mode: CrosshairMode.Normal,
          vertLine: {
            width: 1,
            color: isDarkMode ? 'rgba(166, 173, 187, 0.5)' : 'rgba(0, 0, 0, 0.3)',
            style: 2,
            labelVisible: true,
            labelBackgroundColor: isDarkMode ? '#1f2937' : '#374151',
          },
          horzLine: {
            width: 1,
            color: isDarkMode ? 'rgba(166, 173, 187, 0.5)' : 'rgba(0, 0, 0, 0.3)',
            style: 2,
            labelBackgroundColor: isDarkMode ? '#1f2937' : '#374151',
          },
        },
      })

      // Add candlestick series
      const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: '#22c55e',
        downColor: '#ef4444',
        borderVisible: false,
        wickUpColor: '#22c55e',
        wickDownColor: '#ef4444',
      })

      // Add volume series
      const volumeSeries = chart.addSeries(HistogramSeries, {
        color: '#26a69a',
        priceFormat: {
          type: 'volume',
        },
        priceScaleId: '',
      })
      volumeSeries.priceScale().applyOptions({
        scaleMargins: {
          top: 0.8,
          bottom: 0,
        },
      })

      chartRef.current = chart
      candleSeriesRef.current = candleSeries
      volumeSeriesRef.current = volumeSeries

      // Set data if available
      if (chartData.length > 0) {
        const candleData = chartData.map((d) => ({
          time: d.timestamp as unknown as Parameters<typeof candleSeries.setData>[0][0]['time'],
          open: d.open,
          high: d.high,
          low: d.low,
          close: d.close,
        }))
        candleSeries.setData(candleData)

        const volumeData = chartData.map((d) => ({
          time: d.timestamp as unknown as Parameters<typeof volumeSeries.setData>[0][0]['time'],
          value: d.volume,
          color: d.close >= d.open ? 'rgba(34, 197, 94, 0.5)' : 'rgba(239, 68, 68, 0.5)',
        }))
        volumeSeries.setData(volumeData)

        chart.timeScale().fitContent()
      }
    }, 100)

    // Handle resize with ResizeObserver for better flex layout support
    const handleResize = () => {
      if (chartRef.current && chartContainerRef.current) {
        const container = chartContainerRef.current
        const width = container.offsetWidth
        const height = container.offsetHeight
        if (width > 0 && height > 0) {
          chartRef.current.applyOptions({ width, height })
        }
      }
    }

    const resizeObserver = new ResizeObserver(handleResize)
    if (chartContainerRef.current) {
      resizeObserver.observe(chartContainerRef.current)
    }
    window.addEventListener('resize', handleResize)

    return () => {
      clearTimeout(initTimer)
      resizeObserver.disconnect()
      window.removeEventListener('resize', handleResize)
      if (chartRef.current) {
        chartRef.current.remove()
        chartRef.current = null
      }
    }
  }, [isDarkMode, chartData, isIntradayInterval, isCustomInterval, customIntervalUnit])

  const loadCatalog = async () => {
    try {
      const response = await fetch('/historify/api/catalog', { credentials: 'include' })
      const data = await response.json()
      if (data.status === 'success') {
        setCatalog(data.data || [])
      }
    } catch (_error) {}
  }

  const loadChartData = useCallback(async () => {
    if (!selectedSymbol || !selectedExchange) return

    setIsLoading(true)
    try {
      const params = new URLSearchParams({
        symbol: selectedSymbol,
        exchange: selectedExchange,
        interval: effectiveInterval,
        start_date: startDate,
        end_date: endDate,
      })

      const response = await fetch(`/historify/api/data?${params}`, { credentials: 'include' })
      const data = await response.json()

      if (data.status === 'success') {
        setChartData(data.data || [])
        if (data.count === 0) {
          showToast.info(
            'No data available for this range. Make sure 1m data is downloaded for custom intervals.',
            'historify'
          )
        }
      } else {
        showToast.error(data.message || 'Failed to load chart data', 'historify')
      }
    } catch (_error) {
      showToast.error('Failed to load chart data', 'historify')
    } finally {
      setIsLoading(false)
    }
  }, [selectedSymbol, selectedExchange, effectiveInterval, startDate, endDate])

  const updateDataInfo = useCallback(() => {
    const info = catalog.find(
      (c) =>
        c.symbol === selectedSymbol &&
        c.exchange === selectedExchange &&
        c.interval === selectedInterval
    )
    setDataInfo(info || null)
  }, [catalog, selectedSymbol, selectedExchange, selectedInterval])

  const handleSymbolSelect = (symbol: string, exchange: string) => {
    setSelectedSymbol(symbol)
    setSelectedExchange(exchange)
    setSymbolSearchOpen(false)
    setSymbolSearch('')
  }

  const toggleFullscreen = () => {
    if (!document.fullscreenElement) {
      document.documentElement.requestFullscreen()
      setIsFullscreen(true)
    } else {
      document.exitFullscreen()
      setIsFullscreen(false)
    }
  }

  // Listen for fullscreen changes
  useEffect(() => {
    if (!lead.symbol) return
    const next = `/historify/charts/${encodeURIComponent(lead.symbol)}?exchange=${encodeURIComponent(lead.exchange)}&interval=${encodeURIComponent(lead.interval)}`
    navigate(next, { replace: true })
  }, [lead.symbol, lead.exchange, lead.interval, navigate])

  /**
   * The page's own controls, folded into the first pane's toolbar.
   *
   * They used to sit on a full-width strip, which put a third row of chrome
   * above the chart where /trading manages with two, and spent that height on
   * four controls. They are page-level, so only pane zero is given them.
   */
  const pageControls = (
    <>
      <Button variant="ghost" size="icon" className="h-7 w-7 shrink-0" asChild>
        <Link to="/historify" title="Back to Historify">
          <ArrowLeft className="h-4 w-4" />
        </Link>
      </Button>
      <ChartLayoutPicker layoutId={layoutId} onChange={setLayoutId} className="h-7 w-7 shrink-0" />
      <Button
        variant="ghost"
        size="icon"
        className="h-7 w-7 shrink-0"
        title={
          catalog.isLoading
            ? 'Reading your local data'
            : `Reload downloaded symbols (${symbols.length})`
        }
        onClick={() => void catalog.refetch()}
        disabled={catalog.isFetching}
      >
        <RefreshCw className={catalog.isFetching ? 'h-4 w-4 animate-spin' : 'h-4 w-4'} />
      </Button>
    </>
  )

  return (
    <>
      <Navbar fluid />
      <div className="flex flex-1 flex-col overflow-hidden">
        <ChartGrid
          preset={preset}
          renderPane={(paneId, index) => (
            <HistorifyChartPane
              paneId={paneId}
              feed={feed}
              symbols={symbols}
              state={paneAt(index)}
              onChange={(next) => updatePane(index, next)}
              loading={catalog.isLoading}
              focused={focused === index && preset.cells.length > 1}
              onFocus={() => setFocused(index)}
              persistKey={`historify-charts-pane-${index}`}
              pageControls={index === 0 ? pageControls : undefined}
            />
          )}
        />
      </div>
    </>
  )
}
