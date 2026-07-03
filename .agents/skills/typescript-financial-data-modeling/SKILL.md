---
name: TypeScript Financial Data Modeling
description: Type-safe data modeling patterns for financial and economic data visualization applications
version: 1.0.0
tags: [typescript, financial-data, type-safety, zod, branded-types]
---

# TypeScript Financial Data Modeling

## Branded Types for Domain Safety
```typescript
type Brand<T, B extends string> = T & { readonly __brand: B };

// Currency types
type USD = Brand<number, 'USD'>;
type EUR = Brand<number, 'EUR'>;

export function usd(value: number): USD { return value as USD; }
export function eur(value: number): EUR { return value as EUR; }

// Type-safe operations
export function addUSD(a: USD, b: USD): USD { return (a + b) as USD; }
// addUSD(usd(100), eur(50)); // Compile error!

// Percentage and Ratio
type Percentage = Brand<number, 'Percentage'>;
type Ratio = Brand<number, 'Ratio'>;
type BasisPoints = Brand<number, 'BasisPoints'>;

export function percentage(value: number): Percentage {
  if (value < 0 || value > 100) throw new Error(`Invalid percentage: ${value}`);
  return value as Percentage;
}

export function percentageToRatio(pct: Percentage): Ratio {
  return (pct / 100) as Ratio;
}
```

## Time Series Data Structures
```typescript
export interface TimeSeriesPoint<T = number> {
  readonly timestamp: Date;
  readonly value: T;
}

export interface TimeSeries<T = number> {
  readonly id: string;
  readonly name: string;
  readonly unit: string;
  readonly frequency: 'daily' | 'weekly' | 'monthly' | 'quarterly' | 'yearly';
  readonly data: readonly TimeSeriesPoint<T>[];
  readonly metadata?: {
    readonly source: string;
    readonly lastUpdated: Date;
    readonly seasonallyAdjusted?: boolean;
  };
}

export interface OHLCPoint {
  readonly timestamp: Date;
  readonly open: number;
  readonly high: number;
  readonly low: number;
  readonly close: number;
  readonly volume?: number;
}

export interface StockSeries {
  readonly symbol: string;
  readonly exchange: string;
  readonly currency: 'USD' | 'EUR' | 'GBP';
  readonly interval: '1min' | '5min' | 'daily' | 'weekly';
  readonly data: readonly OHLCPoint[];
}
```

## Economic Indicators with Discriminated Unions
```typescript
interface BaseEconomicIndicator {
  readonly id: string;
  readonly country: string;
  readonly releaseDate: Date;
  readonly period: string;
}

export interface GDPIndicator extends BaseEconomicIndicator {
  readonly type: 'gdp';
  readonly value: number;
  readonly growthRate: number;
  readonly unit: 'billions_usd' | 'trillions_usd';
  readonly seasonallyAdjusted: boolean;
}

export interface InflationIndicator extends BaseEconomicIndicator {
  readonly type: 'inflation';
  readonly rate: number;
  readonly monthOverMonth: number;
  readonly yearOverYear: number;
}

export interface UnemploymentIndicator extends BaseEconomicIndicator {
  readonly type: 'unemployment';
  readonly rate: number;
  readonly laborForceParticipation: number;
}

export type EconomicIndicator = GDPIndicator | InflationIndicator | UnemploymentIndicator;

// Type guards
export function isGDPIndicator(i: EconomicIndicator): i is GDPIndicator {
  return i.type === 'gdp';
}

// Usage with narrowing
function formatIndicator(indicator: EconomicIndicator): string {
  switch (indicator.type) {
    case 'gdp': return `GDP: $${indicator.value}B (${indicator.growthRate}% growth)`;
    case 'inflation': return `Inflation: ${indicator.yearOverYear}% YoY`;
    case 'unemployment': return `Unemployment: ${indicator.rate}%`;
  }
}
```

## Zod Runtime Validation
```typescript
import { z } from 'zod';

export const OHLCSchema = z.object({
  timestamp: z.coerce.date(),
  open: z.number().positive(),
  high: z.number().positive(),
  low: z.number().positive(),
  close: z.number().positive(),
  volume: z.number().int().nonnegative().optional(),
}).refine(
  (data) => data.high >= data.low && data.high >= data.open && data.low <= data.close,
  { message: 'Invalid OHLC: high >= all, low <= all' }
);

export const TimeSeriesSchema = z.object({
  id: z.string().min(1),
  name: z.string().min(1),
  frequency: z.enum(['daily', 'weekly', 'monthly', 'quarterly', 'yearly']),
  data: z.array(z.object({
    timestamp: z.coerce.date(),
    value: z.number(),
  })),
});

// API response transformation
export const AlphaVantageQuoteSchema = z.object({
  'Global Quote': z.object({
    '01. symbol': z.string(),
    '05. price': z.string().transform(Number),
    '09. change': z.string().transform(Number),
    '10. change percent': z.string().transform(s => parseFloat(s.replace('%', ''))),
  }),
}).transform((data) => ({
  symbol: data['Global Quote']['01. symbol'],
  price: data['Global Quote']['05. price'],
  change: data['Global Quote']['09. change'],
  changePercent: data['Global Quote']['10. change percent'],
}));
```

## Nivo Integration Types
```typescript
import type { Serie, Datum } from '@nivo/line';

export interface FinancialDatum extends Datum {
  x: Date | string | number;
  y: number | null;
  metadata?: { volume?: number; change?: number };
}

export interface FinancialSerie extends Omit<Serie, 'data'> {
  id: string;
  data: readonly FinancialDatum[];
  color?: string;
}

export function toNivoSeries(timeSeries: TimeSeries[]): FinancialSerie[] {
  return timeSeries.map((series) => ({
    id: series.id,
    data: series.data.map((point) => ({ x: point.timestamp, y: point.value })),
  }));
}
```

## Data Transformation Utilities
```typescript
export function calculateMovingAverage(
  data: readonly TimeSeriesPoint<number>[],
  windowSize: number
): readonly TimeSeriesPoint<number>[] {
  return data.map((point, index) => {
    const start = Math.max(0, index - windowSize + 1);
    const window = data.slice(start, index + 1);
    const avg = window.reduce((sum, p) => sum + p.value, 0) / window.length;
    return { timestamp: point.timestamp, value: avg };
  });
}

export interface ChangeResult {
  readonly absolute: number;
  readonly percentage: number;
  readonly direction: 'up' | 'down' | 'unchanged';
}

export function calculateChange(current: number, previous: number): ChangeResult {
  const absolute = current - previous;
  const percentage = previous !== 0 ? (absolute / previous) * 100 : 0;
  return {
    absolute,
    percentage,
    direction: absolute > 0 ? 'up' : absolute < 0 ? 'down' : 'unchanged',
  };
}
```

## API Response Types
```typescript
export type ApiResponse<T> =
  | { status: 'success'; data: T; timestamp: Date }
  | { status: 'error'; error: { code: string; message: string } }
  | { status: 'loading' };

export interface PaginatedResponse<T> {
  data: readonly T[];
  pagination: {
    page: number;
    pageSize: number;
    totalItems: number;
    hasNext: boolean;
  };
}
```

## Testing Utilities
```typescript
type DataFactory<T> = (overrides?: Partial<T>) => T;

export const createMockOHLC: DataFactory<OHLCPoint> = (overrides = {}) => ({
  timestamp: new Date(),
  open: 100,
  high: 105,
  low: 98,
  close: 102,
  volume: 1000000,
  ...overrides,
});

export function generatePriceData(startPrice: number, days: number, volatility = 0.02): OHLCPoint[] {
  let price = startPrice;
  return Array.from({ length: days }, (_, i) => {
    const change = (Math.random() - 0.5) * volatility * price;
    const open = price;
    const close = price + change;
    const result = {
      timestamp: new Date(2024, 0, i + 1),
      open, close,
      high: Math.max(open, close) * 1.01,
      low: Math.min(open, close) * 0.99,
      volume: Math.floor(Math.random() * 10000000),
    };
    price = close;
    return result;
  });
}
```