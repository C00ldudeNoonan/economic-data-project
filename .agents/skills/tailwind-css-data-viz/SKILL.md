---
name: Tailwind CSS Data Visualization Patterns
description: Tailwind CSS configurations and patterns for building data-dense financial dashboard UIs
version: 1.0.0
tags: [tailwind, css, dashboard, data-visualization, dark-mode]
---

# Tailwind CSS Data Visualization Patterns

## Configuration
```typescript
// tailwind.config.ts
import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['./src/**/*.{js,ts,jsx,tsx,mdx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        gain: { 500: '#10b981', 600: '#059669', 700: '#047857' },
        loss: { 500: '#ef4444', 600: '#dc2626', 700: '#b91c1c' },
        dashboard: {
          bg: '#f8fafc', 'bg-dark': '#0f172a',
          card: '#ffffff', 'card-dark': '#1e293b',
          border: '#e2e8f0', 'border-dark': '#334155',
        },
        chart: { primary: '#1e40af', grid: '#f3f4f6', 'grid-dark': '#1e293b' },
      },
      fontSize: {
        'metric-xs': ['0.75rem', { lineHeight: '1rem' }],
        'metric-sm': ['0.875rem', { lineHeight: '1.25rem' }],
        'kpi': ['1.5rem', { lineHeight: '2rem', fontWeight: '600' }],
        'kpi-lg': ['2rem', { lineHeight: '2.5rem', fontWeight: '700' }],
      },
      spacing: { card: '1rem', 'card-lg': '1.5rem', section: '2rem' },
      gridTemplateColumns: {
        sidebar: 'minmax(200px, 260px) 1fr',
        metrics: 'repeat(auto-fit, minmax(200px, 1fr))',
      },
      animation: {
        'flash-gain': 'flashGain 600ms ease-out',
        'flash-loss': 'flashLoss 600ms ease-out',
      },
      keyframes: {
        flashGain: { '0%': { backgroundColor: 'rgba(16, 185, 129, 0.25)' }, '100%': { backgroundColor: 'transparent' } },
        flashLoss: { '0%': { backgroundColor: 'rgba(239, 68, 68, 0.25)' }, '100%': { backgroundColor: 'transparent' } },
      },
      boxShadow: {
        card: '0 1px 3px 0 rgba(0, 0, 0, 0.08)',
        'card-hover': '0 4px 12px 0 rgba(0, 0, 0, 0.1)',
      },
    },
  },
};
export default config;
```

## Dashboard Layout
```html
<div class="grid grid-cols-sidebar min-h-screen bg-dashboard-bg dark:bg-dashboard-bg-dark">
  <aside class="sticky top-0 h-screen overflow-y-auto bg-white dark:bg-dashboard-card-dark
                border-r border-dashboard-border dark:border-dashboard-border-dark">
    <nav class="p-4 space-y-1"><!-- Nav --></nav>
  </aside>
  <main class="min-h-screen">
    <header class="sticky top-0 z-20 bg-white/90 dark:bg-dashboard-card-dark/90 backdrop-blur-sm
                   border-b border-dashboard-border dark:border-dashboard-border-dark px-6 py-4">
      <h1 class="text-xl font-semibold text-gray-900 dark:text-white">Dashboard</h1>
    </header>
    <div class="p-section"><!-- Content --></div>
  </main>
</div>
```

## KPI Card
```html
<div class="bg-white dark:bg-dashboard-card-dark rounded-lg border border-dashboard-border
            dark:border-dashboard-border-dark p-card-lg hover:shadow-card-hover transition-shadow">
  <div class="flex items-start justify-between">
    <div>
      <p class="text-metric-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">
        Total Revenue
      </p>
      <p class="text-kpi-lg text-gray-900 dark:text-white tabular-nums lining-nums mt-1">
        $2,847,293
      </p>
    </div>
    <div class="p-2 rounded-lg bg-blue-50 dark:bg-blue-900/30">
      <svg class="w-5 h-5 text-blue-600 dark:text-blue-400"><!-- Icon --></svg>
    </div>
  </div>
  <div class="flex items-center gap-2 mt-4">
    <span class="inline-flex items-center text-metric-sm font-medium text-gain-600">
      <span class="tabular-nums">+12.5%</span>
    </span>
    <span class="text-metric-xs text-gray-400">vs last period</span>
  </div>
</div>
```

## Chart Container
```html
<div class="bg-white dark:bg-dashboard-card-dark rounded-lg border border-dashboard-border
            dark:border-dashboard-border-dark overflow-hidden">
  <div class="px-card-lg pt-card-lg pb-2 border-b border-dashboard-border dark:border-dashboard-border-dark">
    <h3 class="text-metric-sm font-semibold text-gray-900 dark:text-white">GDP Growth Rate</h3>
    <p class="text-metric-xs text-gray-500 dark:text-gray-400 mt-1">Quarterly, seasonally adjusted</p>
  </div>
  <div class="p-card aspect-[16/9]"><!-- Nivo chart --></div>
</div>
```

## Data Table
```html
<div class="overflow-hidden rounded-lg border border-dashboard-border dark:border-dashboard-border-dark">
  <table class="min-w-full divide-y divide-dashboard-border dark:divide-dashboard-border-dark">
    <thead class="bg-gray-50 dark:bg-gray-800/50">
      <tr>
        <th class="px-4 py-3 text-left text-metric-xs font-medium text-gray-500 uppercase">Symbol</th>
        <th class="px-4 py-3 text-right text-metric-xs font-medium text-gray-500 uppercase">Price</th>
        <th class="px-4 py-3 text-right text-metric-xs font-medium text-gray-500 uppercase">Change</th>
      </tr>
    </thead>
    <tbody class="bg-white dark:bg-dashboard-card-dark divide-y divide-dashboard-border">
      <tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
        <td class="px-4 py-3 text-metric-sm font-medium text-gray-900 dark:text-white">AAPL</td>
        <td class="px-4 py-3 text-metric-sm tabular-nums text-right text-gray-900">$178.23</td>
        <td class="px-4 py-3 text-metric-sm tabular-nums text-right text-gain-500">+2.34%</td>
      </tr>
    </tbody>
  </table>
</div>
```

## States: Loading, Error, Empty
```html
<!-- Loading Skeleton -->
<div class="animate-pulse">
  <div class="h-8 bg-gray-200 dark:bg-gray-700 rounded w-48 mb-4"></div>
  <div class="aspect-video bg-gray-200 dark:bg-gray-700 rounded"></div>
</div>

<!-- Error State -->
<div class="flex flex-col items-center justify-center aspect-video bg-red-50 dark:bg-red-950/20
            rounded-lg border border-red-200 dark:border-red-800 p-6">
  <p class="text-red-600 dark:text-red-400 font-medium">Failed to load data</p>
  <button class="mt-3 text-sm text-red-500 hover:text-red-600 underline">Retry</button>
</div>

<!-- Empty State -->
<div class="flex flex-col items-center justify-center aspect-video bg-gray-50 dark:bg-gray-800/50
            rounded-lg border-2 border-dashed border-gray-300 dark:border-gray-600 p-6">
  <p class="text-gray-500 dark:text-gray-400 font-medium">No data available</p>
</div>
```

## Tabular Numbers
```html
<span class="tabular-nums lining-nums slashed-zero text-right">$1,234,567.89</span>
<span class="tabular-nums text-gain-500">+12.34%</span>
<span class="tabular-nums text-loss-500">-5.67%</span>
```

## Dark Mode Toggle
```javascript
function toggleDarkMode() {
  document.documentElement.classList.toggle('dark');
  localStorage.setItem('theme', document.documentElement.classList.contains('dark') ? 'dark' : 'light');
}

// Initialize
if (localStorage.theme === 'dark' || (!localStorage.theme && window.matchMedia('(prefers-color-scheme: dark)').matches)) {
  document.documentElement.classList.add('dark');
}
```