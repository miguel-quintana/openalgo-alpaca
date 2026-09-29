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
import { toCatalogSymbols } from '@/lib/historify/catalog'
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

  // We explicitly unwrap the signal from the React-Query context object 
  // to match what historifyApi.catalog expects.
  const catalog = useQuery({
    queryKey: ['historify', 'catalog'],
    queryFn: async ({ signal }) => await historifyApi.catalog(signal),
    staleTime: 60 * 1000,
  })

  // We provide fallback empty arrays to guarantee the type is never 'unknown' or 'undefined'
  // while the query is still loading.
  const symbols = useMemo(() => toCatalogSymbols(catalog.data ?? []), [catalog.data])
  const feed = useMemo(() => createHistorifyFeed(), [])

  const [layoutId, setLayoutId] = useState<string>(readLayout)
  const [focused, setFocused] = useState(0)

  // Initialize panes by merging localStorage with URL parameters
  const [panes, setPanes] = useState<PaneState[]>(() => {
    const cached = readPanes()
    const initial = cached.length > 0 ? cached : [{ ...BLANK_PANE }]
    
    // Inject URL parameters into the lead pane on first load if provided
    if (urlSymbol) {
      initial[0] = {
        ...initial[0],
        symbol: urlSymbol.toUpperCase(),
        exchange: searchParams.get('exchange')?.toUpperCase() || initial[0].exchange,
        interval: searchParams.get('interval') || initial[0].interval,
      }
    }
    return initial
  })

  const preset = layoutById(layoutId)
  const lead = panes[0] ?? BLANK_PANE

  // Load catalog on mount
  // biome-ignore lint/correctness/useExhaustiveDependencies: one-time catalog fetch on mount
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

  // Update the URL to match pane 0 so the back button behaves rationally.
  useEffect(() => {
    if (!lead.symbol) return
    const next = `/historify/charts/${encodeURIComponent(lead.symbol)}?exchange=${encodeURIComponent(lead.exchange)}&interval=${encodeURIComponent(lead.interval)}`
    navigate(next, { replace: true })
  }, [lead.symbol, lead.exchange, lead.interval, navigate])

  /**
   * The page's own controls, folded into the first pane's toolbar.
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