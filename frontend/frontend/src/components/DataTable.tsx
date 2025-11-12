/** Data table component for displaying raw market data */
import React from 'react';
import { MarketSummaryData, MarketAnalysisData } from '../types/market';

interface DataTableProps {
  data: MarketSummaryData[] | MarketAnalysisData[];
  title?: string;
}

export const DataTable: React.FC<DataTableProps> = ({ data, title }) => {
  if (data.length === 0) {
    return <div>No data available</div>;
  }

  const isSummaryData = (d: MarketSummaryData | MarketAnalysisData): d is MarketSummaryData => {
    return 'total_return_pct' in d;
  };

  const columns = isSummaryData(data[0])
    ? Object.keys(data[0] as MarketSummaryData)
    : Object.keys(data[0] as MarketAnalysisData);

  return (
    <div style={{ overflowX: 'auto', marginTop: '20px' }}>
      {title && <h3>{title}</h3>}
      <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '14px' }}>
        <thead>
          <tr style={{ backgroundColor: '#f5f5f5', borderBottom: '2px solid #ddd' }}>
            {columns.map((col) => (
              <th
                key={col}
                style={{
                  padding: '12px',
                  textAlign: 'left',
                  fontWeight: 'bold',
                  textTransform: 'capitalize',
                }}
              >
                {col.replace(/_/g, ' ')}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, idx) => (
            <tr
              key={idx}
              style={{
                borderBottom: '1px solid #ddd',
                backgroundColor: idx % 2 === 0 ? '#fff' : '#f9f9f9',
              }}
            >
              {columns.map((col) => (
                <td key={col} style={{ padding: '10px' }}>
                  {typeof (row as any)[col] === 'number'
                    ? (row as any)[col].toLocaleString(undefined, {
                        maximumFractionDigits: 2,
                      })
                    : String((row as any)[col] || '-')}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

