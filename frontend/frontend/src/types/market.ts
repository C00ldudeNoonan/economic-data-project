/** TypeScript types for market data structures */

export type MarketCategory = 'currency' | 'global_markets' | 'major_indicies' | 'us_sector';

export type TimePeriod = '12_weeks' | '6_months' | '1_year' | '5_years';

export interface MarketSummaryData {
  symbol: string;
  asset_type: string;
  time_period: string;
  exchange: string;
  name: string;
  period_start_date: string;
  period_end_date: string;
  trading_days: number;
  positive_days: number;
  negative_days: number;
  neutral_days: number;
  total_return_pct: number;
  avg_daily_return_pct: number;
  volatility_pct: number;
  win_rate_pct: number;
  total_price_change: number;
  avg_daily_price_change: number;
  worst_day_change: number;
  best_day_change: number;
  period_start_price: number;
  period_end_price: number;
}

export interface MarketAnalysisData {
  symbol: string;
  month_date: string;
  quarter_num: number;
  year_val: number;
  monthly_avg_close: number;
  monthly_avg_volume: number;
  quarterly_avg_close: number;
  quarterly_avg_volume: number;
  pct_change_q1_forward: number;
  pct_change_q2_forward: number;
  pct_change_q3_forward: number;
  pct_change_q4_forward: number;
  category: string;
}

export interface SymbolInfo {
  symbol: string;
  name: string;
  exchange: string;
  asset_type: string;
}

export interface ApiResponse<T> {
  category: string;
  count: number;
  data: T[];
}

export interface CategoriesResponse {
  categories: MarketCategory[];
}

export interface SymbolsResponse {
  category: string;
  count: number;
  symbols: SymbolInfo[];
}

