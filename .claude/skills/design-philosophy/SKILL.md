---
name: design-philosophy
description: Data-first visualization design combining Tufte principles with Jobs/Ive simplicity for React + Nivo dashboards.
version: 1.1.0
tags: [design, tufte, data-visualization, philosophy, nivo, react, typography]
---

# Design Philosophy: Economic Data Visualization

This skill replaces the prior `design-philosphy`, `nivo-charts`, and `tufte-visualization` aliases as the single canonical guide.

## Use When

- Creating or revising charts, dashboards, or data-dense UI
- Choosing chart types, encodings, or annotations
- Assessing visual integrity, readability, or hierarchy

## Core Principle

**Show the data.** Every visual choice must serve comprehension. Remove elements that do not improve clarity.

## Tufte Principles (Applied)

### 1. Data-Ink Ratio
- Remove ornamental elements (heavy grids, gradients, 3D effects).
- Favor thin strokes, direct labels, and whitespace.

```typescript
// ✅ Clean Nivo Line
<ResponsiveLine
  data={data}
  enableGridX={false}
  enableGridY={true}
  enableArea={false}
  enablePoints={false}
  lineWidth={2}
  colors={['#1f2937']}
/>
```

### 2. Graphical Integrity
- Bar charts start at zero.
- Use linear scales unless data is explicitly log-scaled.
- Avoid perspective, drop-shadows, or faux depth.

```typescript
const integrityConfig = {
  yScale: { type: 'linear', min: 0, max: 'auto' },
};
```

### 3. Chartjunk Elimination
Remove:
- Background textures
- Excessive ticks
- Redundant legends
- Non-data icons

### 4. Data Density
Prefer small multiples over cramming unrelated series.

```typescript
function SmallMultiples<T>({ data, renderChart }: {
  data: Record<string, T[]>;
  renderChart: (data: T[], key: string) => React.ReactNode;
}) {
  return (
    <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
      {Object.entries(data).map(([key, values]) => (
        <div key={key} className="space-y-2">
          <div className="text-xs font-medium tracking-wide text-slate-600 uppercase">{key}</div>
          <div className="h-32">{renderChart(values, key)}</div>
        </div>
      ))}
    </div>
  );
}
```

### 5. Sparklines for Quick Context
Use sparklines to show trend and direction inline.

```typescript
function Sparkline({ data, color = '#0f766e' }: { data: number[]; color?: string }) {
  const series = [{ id: 'spark', data: data.map((y, x) => ({ x, y })) }];
  return (
    <ResponsiveLine
      data={series}
      width={120}
      height={24}
      margin={{ top: 2, right: 2, bottom: 2, left: 2 }}
      axisLeft={null}
      axisBottom={null}
      enableGridX={false}
      enableGridY={false}
      enablePoints={false}
      colors={[color]}
      lineWidth={1.5}
      isInteractive={false}
      animate={false}
    />
  );
}
```

## Jobs/Ive Simplicity (Applied)

- **Depth over decoration**: hierarchy through spacing, not ornament.
- **Single visual story**: every view should answer one question clearly.
- **Tactile clarity**: typography and rhythm make data feel deliberate.

## Typography and Hierarchy

- Use a strong humanist sans + mono pairing (e.g., `"IBM Plex Sans"` and `"IBM Plex Mono"`).
- Use tabular numerals for metrics.

```css
:root {
  --font-sans: "IBM Plex Sans", "Source Sans 3", system-ui, sans-serif;
  --font-mono: "IBM Plex Mono", ui-monospace, monospace;
}
```

## Canonical Nivo Theme

```typescript
import { Theme } from '@nivo/core';

export const dataFirstTheme: Theme = {
  background: 'transparent',
  text: {
    fontSize: 11,
    fontFamily: 'var(--font-sans)',
    fill: '#334155',
  },
  axis: {
    domain: { line: { stroke: '#94a3b8', strokeWidth: 1 } },
    ticks: {
      line: { stroke: 'transparent', strokeWidth: 0 },
      text: { fontSize: 10, fill: '#64748b' },
    },
    legend: {
      text: { fontSize: 11, fill: '#334155', fontWeight: 500 },
    },
  },
  grid: {
    line: { stroke: '#e2e8f0', strokeWidth: 1 },
  },
  tooltip: {
    container: {
      background: '#ffffff',
      fontSize: 11,
      borderRadius: 4,
      boxShadow: '0 2px 8px rgba(0,0,0,0.12)',
      padding: '8px 10px',
    },
  },
};
```

## Chart Selection Guide

- Time series trend: line chart
- Comparisons across categories: bar chart
- Part-to-whole with few categories: stacked bar (avoid pie)
- Distributions: histogram or box plot

## Do / Avoid

- Do: direct labels, minimal palettes, consistent scales
- Avoid: heavy borders, rainbow palettes, 3D, non-zero baselines for bars

## Related Skills

- `/tailwind-css-data-viz`
- `/react-component-architecture`
- `/typescript-financial-data-modeling`
