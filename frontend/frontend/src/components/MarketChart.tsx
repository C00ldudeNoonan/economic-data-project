/** Plotly.js chart component for market data visualization */
import React from 'react';
import Plot from 'react-plotly.js';
import { MarketSummaryData, MarketAnalysisData } from '../types/market';

interface MarketChartProps {
  data: MarketSummaryData[] | MarketAnalysisData[];
  chartType: 'returns' | 'volatility' | 'comparison' | 'time-series';
  title?: string;
}

export const MarketChart: React.FC<MarketChartProps> = ({ data, chartType, title }) => {
  if (data.length === 0) {
    return <div>No data available</div>;
  }

  const isSummaryData = (d: MarketSummaryData | MarketAnalysisData): d is MarketSummaryData => {
    return 'total_return_pct' in d;
  };

  const layout = {
    title: title || 'Market Data Visualization',
    xaxis: { title: '' },
    yaxis: { title: '' },
    hovermode: 'closest' as const,
    showlegend: true,
    height: 500,
  };

  if (chartType === 'time-series' && !isSummaryData(data[0])) {
    // Time series chart for analysis data
    const analysisData = data as MarketAnalysisData[];
    const symbols = Array.from(new Set(analysisData.map(d => d.symbol)));

    const traces = symbols.map(symbol => {
      const symbolData = analysisData.filter(d => d.symbol === symbol);
      return {
        x: symbolData.map(d => d.month_date),
        y: symbolData.map(d => d.monthly_avg_close),
        type: 'scatter' as const,
        mode: 'lines+markers' as const,
        name: symbol,
      };
    });

    return (
      <Plot
        data={traces}
        layout={{
          ...layout,
          xaxis: { title: 'Date' },
          yaxis: { title: 'Price' },
          title: title || 'Price Over Time',
        }}
        style={{ width: '100%', height: '100%' }}
      />
    );
  }

  if (chartType === 'returns' && isSummaryData(data[0])) {
    // Returns comparison chart
    const summaryData = data as MarketSummaryData[];
    const symbols = summaryData.map(d => d.symbol);
    const returns = summaryData.map(d => d.total_return_pct);

    return (
      <Plot
        data={[
          {
            x: symbols,
            y: returns,
            type: 'bar',
            marker: {
              color: returns.map(r => (r >= 0 ? '#2ecc71' : '#e74c3c')),
            },
          },
        ]}
        layout={{
          ...layout,
          xaxis: { title: 'Symbol' },
          yaxis: { title: 'Total Return (%)' },
          title: title || 'Total Returns by Symbol',
        }}
        style={{ width: '100%', height: '100%' }}
      />
    );
  }

  if (chartType === 'volatility' && isSummaryData(data[0])) {
    // Risk-return scatter plot
    const summaryData = data as MarketSummaryData[];
    const returns = summaryData.map(d => d.total_return_pct);
    const volatility = summaryData.map(d => d.volatility_pct);
    const symbols = summaryData.map(d => d.symbol);

    return (
      <Plot
        data={[
          {
            x: volatility,
            y: returns,
            mode: 'markers',
            type: 'scatter',
            text: symbols,
            marker: {
              size: 10,
              color: returns,
              colorscale: 'RdYlGn',
              showscale: true,
            },
          },
        ]}
        layout={{
          ...layout,
          xaxis: { title: 'Volatility (%)' },
          yaxis: { title: 'Total Return (%)' },
          title: title || 'Risk-Return Analysis',
        }}
        style={{ width: '100%', height: '100%' }}
      />
    );
  }

  if (chartType === 'comparison' && isSummaryData(data[0])) {
    // Multi-metric comparison
    const summaryData = data as MarketSummaryData[];
    const symbols = summaryData.map(d => d.symbol);
    const returns = summaryData.map(d => d.total_return_pct);
    const volatility = summaryData.map(d => d.volatility_pct);
    const winRate = summaryData.map(d => d.win_rate_pct);

    return (
      <Plot
        data={[
          {
            x: symbols,
            y: returns,
            name: 'Total Return (%)',
            type: 'bar',
            yaxis: 'y',
          },
          {
            x: symbols,
            y: volatility,
            name: 'Volatility (%)',
            type: 'bar',
            yaxis: 'y2',
          },
          {
            x: symbols,
            y: winRate,
            name: 'Win Rate (%)',
            type: 'scatter',
            mode: 'lines+markers',
            yaxis: 'y3',
          },
        ]}
        layout={{
          ...layout,
          xaxis: { title: 'Symbol' },
          yaxis: { title: 'Total Return (%)', side: 'left' },
          yaxis2: { title: 'Volatility (%)', side: 'right', overlaying: 'y' },
          yaxis3: { title: 'Win Rate (%)', side: 'right', overlaying: 'y', anchor: 'x' },
          title: title || 'Performance Comparison',
        }}
        style={{ width: '100%', height: '100%' }}
      />
    );
  }

  return <div>Unsupported chart type for this data</div>;
};

