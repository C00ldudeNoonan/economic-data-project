/** Main dashboard component for market data visualization */
import React, { useState, useEffect } from 'react';
import { apiClient } from '../services/api';
import { MarketChart } from './MarketChart';
import { DataTable } from './DataTable';
import { MarketCategory, MarketSummaryData, MarketAnalysisData, SymbolInfo } from '../types/market';

export const Dashboard: React.FC = () => {
  const [categories, setCategories] = useState<MarketCategory[]>([]);
  const [selectedCategory, setSelectedCategory] = useState<MarketCategory>('major_indicies');
  const [symbols, setSymbols] = useState<SymbolInfo[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('');
  const [summaryData, setSummaryData] = useState<MarketSummaryData[]>([]);
  const [analysisData, setAnalysisData] = useState<MarketAnalysisData[]>([]);
  const [chartType, setChartType] = useState<'returns' | 'volatility' | 'comparison' | 'time-series'>('returns');
  const [timePeriod, setTimePeriod] = useState<string>('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'summary' | 'analysis'>('summary');

  useEffect(() => {
    loadCategories();
  }, []);

  useEffect(() => {
    if (selectedCategory) {
      loadSymbols();
    }
  }, [selectedCategory]);

  useEffect(() => {
    if (selectedCategory) {
      loadData();
    }
  }, [selectedCategory, selectedSymbol, timePeriod, viewMode]);

  const loadCategories = async () => {
    try {
      const cats = await apiClient.getCategories();
      setCategories(cats);
      if (cats.length > 0 && !selectedCategory) {
        setSelectedCategory(cats[0]);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load categories');
    }
  };

  const loadSymbols = async () => {
    try {
      const syms = await apiClient.getSymbols(selectedCategory);
      setSymbols(syms);
      if (syms.length > 0 && !selectedSymbol) {
        setSelectedSymbol(syms[0].symbol);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load symbols');
    }
  };

  const loadData = async () => {
    setLoading(true);
    setError(null);
    try {
      if (viewMode === 'summary') {
        const response = await apiClient.getSummaryData(selectedCategory, {
          symbol: selectedSymbol || undefined,
          timePeriod: timePeriod || undefined,
          limit: 100,
        });
        setSummaryData(response.data);
      } else {
        const response = await apiClient.getAnalysisData(selectedCategory, {
          symbol: selectedSymbol || undefined,
          limit: 100,
        });
        setAnalysisData(response.data);
        setChartType('time-series');
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const currentData = viewMode === 'summary' ? summaryData : analysisData;

  return (
    <div style={{ padding: '20px', fontFamily: 'Arial, sans-serif' }}>
      <h1 style={{ marginBottom: '30px' }}>Economic Data Dashboard</h1>

      {/* Controls */}
      <div style={{ marginBottom: '20px', display: 'flex', gap: '15px', flexWrap: 'wrap', alignItems: 'center' }}>
        <div>
          <label style={{ marginRight: '5px' }}>Category:</label>
          <select
            value={selectedCategory}
            onChange={(e) => {
              setSelectedCategory(e.target.value as MarketCategory);
              setSelectedSymbol('');
            }}
            style={{ padding: '5px 10px' }}
          >
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat.replace(/_/g, ' ').toUpperCase()}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label style={{ marginRight: '5px' }}>Symbol:</label>
          <select
            value={selectedSymbol}
            onChange={(e) => setSelectedSymbol(e.target.value)}
            style={{ padding: '5px 10px' }}
          >
            <option value="">All Symbols</option>
            {symbols.map((sym) => (
              <option key={sym.symbol} value={sym.symbol}>
                {sym.symbol} - {sym.name}
              </option>
            ))}
          </select>
        </div>

        {viewMode === 'summary' && (
          <div>
            <label style={{ marginRight: '5px' }}>Time Period:</label>
            <select
              value={timePeriod}
              onChange={(e) => setTimePeriod(e.target.value)}
              style={{ padding: '5px 10px' }}
            >
              <option value="">All Periods</option>
              <option value="12_weeks">12 Weeks</option>
              <option value="6_months">6 Months</option>
              <option value="1_year">1 Year</option>
              <option value="5_years">5 Years</option>
            </select>
          </div>
        )}

        <div>
          <label style={{ marginRight: '5px' }}>View:</label>
          <select
            value={viewMode}
            onChange={(e) => setViewMode(e.target.value as 'summary' | 'analysis')}
            style={{ padding: '5px 10px' }}
          >
            <option value="summary">Summary</option>
            <option value="analysis">Time Series</option>
          </select>
        </div>

        {viewMode === 'summary' && (
          <div>
            <label style={{ marginRight: '5px' }}>Chart Type:</label>
            <select
              value={chartType}
              onChange={(e) => setChartType(e.target.value as any)}
              style={{ padding: '5px 10px' }}
            >
              <option value="returns">Returns</option>
              <option value="volatility">Risk-Return</option>
              <option value="comparison">Comparison</option>
            </select>
          </div>
        )}
      </div>

      {error && (
        <div style={{ padding: '10px', backgroundColor: '#fee', color: '#c33', marginBottom: '20px', borderRadius: '4px' }}>
          Error: {error}
        </div>
      )}

      {loading ? (
        <div>Loading...</div>
      ) : (
        <>
          {/* Chart */}
          {currentData.length > 0 && (
            <div style={{ marginBottom: '30px', backgroundColor: '#fff', padding: '20px', borderRadius: '8px', boxShadow: '0 2px 4px rgba(0,0,0,0.1)' }}>
              <MarketChart
                data={currentData}
                chartType={chartType}
                title={`${selectedCategory.replace(/_/g, ' ').toUpperCase()} - ${chartType.toUpperCase()}`}
              />
            </div>
          )}

          {/* Data Table */}
          {currentData.length > 0 && (
            <DataTable
              data={currentData}
              title={`${selectedCategory.replace(/_/g, ' ').toUpperCase()} Data`}
            />
          )}
        </>
      )}
    </div>
  );
};

