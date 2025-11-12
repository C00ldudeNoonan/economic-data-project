/** API client for communicating with the backend */
import axios, { AxiosInstance } from 'axios';
import {
  MarketCategory,
  MarketSummaryData,
  MarketAnalysisData,
  ApiResponse,
  CategoriesResponse,
  SymbolsResponse,
  SymbolInfo,
} from '../types/market';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const API_KEY = import.meta.env.VITE_API_KEY || '';

class ApiClient {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: API_URL,
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': API_KEY,
      },
    });
  }

  /**
   * Get list of available market data categories
   */
  async getCategories(): Promise<MarketCategory[]> {
    const response = await this.client.get<CategoriesResponse>('/api/market-data/categories');
    return response.data.categories;
  }

  /**
   * Get summary data for a specific category
   */
  async getSummaryData(
    category: MarketCategory,
    options?: {
      symbol?: string;
      timePeriod?: string;
      limit?: number;
    }
  ): Promise<ApiResponse<MarketSummaryData>> {
    const params = new URLSearchParams();
    if (options?.symbol) params.append('symbol', options.symbol);
    if (options?.timePeriod) params.append('time_period', options.timePeriod);
    if (options?.limit) params.append('limit', options.limit.toString());

    const response = await this.client.get<ApiResponse<MarketSummaryData>>(
      `/api/market-data/summary/${category}${params.toString() ? `?${params.toString()}` : ''}`
    );
    return response.data;
  }

  /**
   * Get analysis return data for a specific category (time-series)
   */
  async getAnalysisData(
    category: MarketCategory,
    options?: {
      symbol?: string;
      limit?: number;
    }
  ): Promise<ApiResponse<MarketAnalysisData>> {
    const params = new URLSearchParams();
    if (options?.symbol) params.append('symbol', options.symbol);
    if (options?.limit) params.append('limit', options.limit.toString());

    const response = await this.client.get<ApiResponse<MarketAnalysisData>>(
      `/api/market-data/analysis/${category}${params.toString() ? `?${params.toString()}` : ''}`
    );
    return response.data;
  }

  /**
   * Get list of symbols for a specific category
   */
  async getSymbols(category: MarketCategory): Promise<SymbolInfo[]> {
    const response = await this.client.get<SymbolsResponse>(`/api/market-data/symbols/${category}`);
    return response.data.symbols;
  }
}

export const apiClient = new ApiClient();

