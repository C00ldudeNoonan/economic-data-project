---
name: React Data Dashboard Architecture
description: React component patterns, hooks, and state management for data visualization dashboards
version: 1.0.0
tags: [react, typescript, architecture, hooks, tanstack-query, zustand]
---

# React Data Dashboard Architecture

## Compound Component Pattern
```typescript
import React, { createContext, useContext, useState, useMemo, ReactNode } from 'react';

interface ChartContextValue {
  data: DataPoint[];
  selectedPoint: DataPoint | null;
  setSelectedPoint: (point: DataPoint | null) => void;
}

const ChartContext = createContext<ChartContextValue | undefined>(undefined);

function useChartContext() {
  const ctx = useContext(ChartContext);
  if (!ctx) throw new Error('Must be used within Chart');
  return ctx;
}

export function Chart({ data, children }: { data: DataPoint[]; children: ReactNode }) {
  const [selectedPoint, setSelectedPoint] = useState<DataPoint | null>(null);
  const value = useMemo(() => ({ data, selectedPoint, setSelectedPoint }), [data, selectedPoint]);
  return <ChartContext.Provider value={value}>{children}</ChartContext.Provider>;
}

Chart.Line = function Line({ color = '#1e40af' }) {
  const { data } = useChartContext();
  return <path d={generatePath(data)} stroke={color} />;
};

Chart.Tooltip = function Tooltip() {
  const { selectedPoint } = useChartContext();
  if (!selectedPoint) return null;
  return <div className="tooltip">{selectedPoint.value}</div>;
};

// Usage
<Chart data={financialData}>
  <Chart.Line color="#10b981" />
  <Chart.Tooltip />
</Chart>
```

## TanStack Query for Data Fetching
```typescript
import { useQuery } from '@tanstack/react-query';

export function useStockData(symbol: string) {
  return useQuery({
    queryKey: ['stock', symbol],
    queryFn: () => fetchStockData(symbol),
    staleTime: 30 * 1000,
    refetchInterval: 5000,
  });
}

export function useHistoricalData(symbol: string, timeRange: TimeRange) {
  return useQuery({
    queryKey: ['historical', symbol, timeRange],
    queryFn: () => fetchHistoricalData(symbol, timeRange),
    staleTime: 5 * 60 * 1000,
    placeholderData: (prev) => prev,
  });
}
```

## Zustand for Dashboard State
```typescript
import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface DashboardState {
  selectedSymbols: string[];
  timeRange: TimeRange;
  chartType: 'line' | 'candlestick' | 'area';
  setTimeRange: (range: TimeRange) => void;
  toggleSymbol: (symbol: string) => void;
}

export const useDashboardStore = create<DashboardState>()(
  persist(
    (set) => ({
      selectedSymbols: ['AAPL'],
      timeRange: '1M',
      chartType: 'line',
      setTimeRange: (range) => set({ timeRange: range }),
      toggleSymbol: (symbol) => set((s) => ({
        selectedSymbols: s.selectedSymbols.includes(symbol)
          ? s.selectedSymbols.filter(x => x !== symbol)
          : [...s.selectedSymbols, symbol]
      })),
    }),
    { name: 'dashboard-storage' }
  )
);
```

## useEffect Best Practices

### Async Effects with Cleanup
Always handle component unmount for async operations to prevent state updates on unmounted components:

```typescript
useEffect(() => {
  let isMounted = true;

  async function fetchData() {
    try {
      setLoading(true);
      const data = await api.getData();
      if (isMounted) {
        setData(data);
      }
    } catch (err) {
      if (isMounted) {
        setError(err instanceof Error ? err.message : 'Unknown error');
      }
    } finally {
      if (isMounted) {
        setLoading(false);
      }
    }
  }

  fetchData();
  return () => {
    isMounted = false;
  };
}, []);
```

### AbortController for Fetch Requests
Use AbortController for cancelable network requests:

```typescript
useEffect(() => {
  const controller = new AbortController();

  async function fetchData() {
    try {
      const response = await fetch(url, { signal: controller.signal });
      const data = await response.json();
      setData(data);
    } catch (err) {
      if (err instanceof Error && err.name !== 'AbortError') {
        setError(err.message);
      }
    }
  }

  fetchData();
  return () => controller.abort();
}, [url]);
```

### Ref-Based Cleanup for Long-Lived Components
Use refs for cleanup flags when multiple effects need access:

```typescript
function useAsyncOperation() {
  const isMountedRef = useRef(true);

  useEffect(() => {
    isMountedRef.current = true;
    return () => {
      isMountedRef.current = false;
    };
  }, []);

  const execute = useCallback(async () => {
    const result = await api.call();
    if (isMountedRef.current) {
      setResult(result);
    }
  }, []);

  return { execute };
}
```

### Race Condition Prevention
Prevent stale closures when dependencies change rapidly:

```typescript
useEffect(() => {
  let isCurrentRequest = true;

  async function loadData() {
    const data = await fetchData(id);
    if (isCurrentRequest) {
      setData(data);
    }
  }

  loadData();
  return () => {
    isCurrentRequest = false;
  };
}, [id]);
```

### Context Value Memoization
Always memoize context values to prevent unnecessary re-renders:

```typescript
function MyProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState(initialState);

  const doSomething = useCallback(() => {
    setState(prev => ({ ...prev, updated: true }));
  }, []);

  const value = useMemo(
    () => ({ state, doSomething }),
    [state, doSomething]
  );

  return <MyContext.Provider value={value}>{children}</MyContext.Provider>;
}
```

### When NOT to Use useEffect
Prefer alternatives when possible:

```typescript
// BAD: Deriving state in useEffect
useEffect(() => {
  setFilteredItems(items.filter(i => i.active));
}, [items]);

// GOOD: Derive during render with useMemo
const filteredItems = useMemo(() => items.filter(i => i.active), [items]);

// BAD: Resetting state on prop change
useEffect(() => {
  setSelectedId(null);
}, [items]);

// GOOD: Use key to reset component state
<ItemList key={listId} items={items} />

// BAD: Fetching in useEffect when React Query is available
useEffect(() => {
  fetchData().then(setData);
}, []);

// GOOD: Use TanStack Query
const { data } = useQuery({ queryKey: ['data'], queryFn: fetchData });
```

### Synchronous Effects (No Cleanup Needed)
Simple DOM updates and localStorage writes don't need cleanup:

```typescript
// localStorage sync - no cleanup needed
useEffect(() => {
  localStorage.setItem('theme', theme);
}, [theme]);

// DOM class updates - no cleanup needed
useEffect(() => {
  document.documentElement.classList.toggle('dark', isDark);
}, [isDark]);

// Scroll to element - no cleanup needed
useEffect(() => {
  messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
}, [messages]);
```

## Custom Hooks

### useChartDimensions
```typescript
export function useChartDimensions(margins = { top: 40, right: 30, bottom: 40, left: 75 }) {
  const ref = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 0, height: 0, boundedWidth: 0, boundedHeight: 0 });

  useLayoutEffect(() => {
    if (!ref.current) return;
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      setDimensions({
        width, height,
        boundedWidth: Math.max(width - margins.left - margins.right, 0),
        boundedHeight: Math.max(height - margins.top - margins.bottom, 0),
      });
    });
    observer.observe(ref.current);
    return () => observer.disconnect();
  }, [margins]);

  return [ref, dimensions] as const;
}
```

### useTimeRange
```typescript
export function useTimeRange(initial: TimeRange = '1M') {
  const [range, setRange] = useState<TimeRange>(initial);

  const { startDate, endDate, interval } = useMemo(() => {
    const end = new Date();
    let start: Date, interval: string;
    switch (range) {
      case '1D': start = new Date(end.getTime() - 86400000); interval = 'minute'; break;
      case '1W': start = new Date(end.getTime() - 7 * 86400000); interval = 'hour'; break;
      case '1M': start = new Date(end.getFullYear(), end.getMonth() - 1, end.getDate()); interval = 'day'; break;
      case '1Y': start = new Date(end.getFullYear() - 1, end.getMonth(), end.getDate()); interval = 'week'; break;
      default: start = new Date(2000, 0, 1); interval = 'month';
    }
    return { startDate: start, endDate: end, interval };
  }, [range]);

  return { range, setRange, startDate, endDate, interval };
}
```

### useChartInteraction
```typescript
export function useChartInteraction<T>() {
  const [hoveredPoint, setHoveredPoint] = useState<T | null>(null);
  const [selectedPoints, setSelectedPoints] = useState<T[]>([]);
  const [brushRange, setBrushRange] = useState<[Date, Date] | null>(null);

  const handlers = useMemo(() => ({
    onHover: setHoveredPoint,
    onSelect: (point: T) => setSelectedPoints(prev =>
      prev.includes(point) ? prev.filter(p => p !== point) : [...prev, point]
    ),
    onBrush: setBrushRange,
    onClear: () => { setSelectedPoints([]); setBrushRange(null); },
  }), []);

  return { hoveredPoint, selectedPoints, brushRange, ...handlers };
}
```

## Performance Patterns
```typescript
// Memoized chart component
const LineChart = React.memo(function LineChart({ data, config }: Props) {
  const processedData = useMemo(() => transformData(data), [data]);
  return <svg><path d={generatePath(processedData)} /></svg>;
});

// Memoized event handlers
function InteractiveChart({ data, onPointSelect }: Props) {
  const handleMouseMove = useCallback((e: MouseEvent) => {
    const point = findNearestPoint(e, data);
    if (point) onPointSelect(point);
  }, [data, onPointSelect]);

  return <svg onMouseMove={handleMouseMove} />;
}
```

## Error Boundary
```typescript
class ChartErrorBoundary extends Component<{ children: ReactNode; fallback?: ReactNode }, { hasError: boolean }> {
  state = { hasError: false };
  static getDerivedStateFromError() { return { hasError: true }; }
  render() {
    if (this.state.hasError) {
      return this.props.fallback || <div className="chart-error">Chart failed to load</div>;
    }
    return this.props.children;
  }
}
```

## Suspense Pattern
```typescript
function Dashboard() {
  return (
    <div className="dashboard">
      <Suspense fallback={<KPICardsSkeleton />}>
        <KPICards />
      </Suspense>
      <Suspense fallback={<ChartSkeleton height={400} />}>
        <MainChart />
      </Suspense>
    </div>
  );
}
```

## Accessibility
```typescript
export function AccessibleChart({ data, title }: Props) {
  const [focusedIndex, setFocusedIndex] = useState(-1);

  const handleKeyDown = useCallback((e: KeyboardEvent) => {
    switch (e.key) {
      case 'ArrowRight': setFocusedIndex(i => Math.min(i + 1, data.length - 1)); break;
      case 'ArrowLeft': setFocusedIndex(i => Math.max(i - 1, 0)); break;
    }
  }, [data.length]);

  return (
    <figure role="figure" aria-label={title} tabIndex={0} onKeyDown={handleKeyDown}>
      <svg>{/* Chart */}</svg>
      <div aria-live="polite" className="sr-only">
        {focusedIndex >= 0 && `${data[focusedIndex].label}: ${data[focusedIndex].value}`}
      </div>
    </figure>
  );
}
```

## Testing
```typescript
import { render, screen, fireEvent } from '@testing-library/react';
import { renderHook, act } from '@testing-library/react';

describe('Chart', () => {
  it('renders data points', () => {
    render(<Chart data={mockData} />);
    expect(screen.getByRole('figure')).toBeInTheDocument();
  });
});

describe('useTimeRange', () => {
  it('updates range correctly', () => {
    const { result } = renderHook(() => useTimeRange());
    act(() => result.current.setRange('1Y'));
    expect(result.current.range).toBe('1Y');
  });
});
```

## File Structure
src/
├── features/
│   ├── dashboard/
│   │   ├── components/
│   │   ├── hooks/
│   │   ├── store/
│   │   └── types/
│   └── charts/
│       ├── components/
│       ├── hooks/
│       └── types/
├── shared/
│   ├── components/ui/
│   ├── hooks/
│   └── utils/
└── lib/
└── queryClient.ts