import dagster as dg

from macro_agents.defs.domains.sec.bi import sec_filing_business_intelligence
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset(
    group_name="transformation",
    kinds={"database", "summary"},
    deps=[sec_filing_business_intelligence],
    description="Create summary table with BI signals aggregated by company",
)
def sec_company_bi_summary(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Create a summary table aggregating BI signals by company.

    This table provides a single-row-per-company view of all
    business intelligence signals, useful for screening and filtering.

    Columns include signal counts for each category and overall scores.
    """
    conn = None
    try:
        conn = bq.get_connection()

        # Create summary table
        conn.query("""
            CREATE TABLE IF NOT EXISTS sec_company_bi_summary (
                symbol VARCHAR PRIMARY KEY,
                company_name VARCHAR,
                latest_10k_date DATE,
                latest_10q_date DATE,
                total_filings INTEGER,
                total_signals INTEGER,
                growth_signals_count INTEGER DEFAULT 0,
                hiring_signals_count INTEGER DEFAULT 0,
                market_expansion_count INTEGER DEFAULT 0,
                capex_signals_count INTEGER DEFAULT 0,
                innovation_signals_count INTEGER DEFAULT 0,
                efficiency_signals_count INTEGER DEFAULT 0,
                risk_signals_count INTEGER DEFAULT 0,
                financial_health_count INTEGER DEFAULT 0,
                strategic_signals_count INTEGER DEFAULT 0,
                avg_confidence DECIMAL(5,4),
                growth_score DECIMAL(5,4),
                risk_score DECIMAL(5,4),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # Get all companies with filings
        companies = bq.fetchall("""
            SELECT DISTINCT
                f.symbol,
                c.company_name
            FROM sec_filings f
            LEFT JOIN sec_company_cik c ON f.symbol = c.symbol
            WHERE f.symbol IS NOT NULL
        """)

        context.log.debug(f"Calculating BI summary for {len(companies)} companies")

        total_updated = 0

        for symbol, company_name in companies:
            # Get filing dates
            filing_dates = bq.fetchone(
                """
                SELECT
                    MAX(CASE WHEN form_type = '10-K' THEN filing_date END) as latest_10k,
                    MAX(CASE WHEN form_type = '10-Q' THEN filing_date END) as latest_10q,
                    COUNT(*) as total_filings
                FROM sec_filings
                WHERE symbol = ?
            """,
                [symbol],)
            if filing_dates:
                latest_10k, latest_10q, total_filings = filing_dates
            else:
                latest_10k = None
                latest_10q = None
                total_filings = 0

            # Get signal counts by category
            signal_counts = bq.fetchall(
                """
                SELECT
                    st.term_category,
                    COUNT(*) as count,
                    AVG(st.confidence_score) as avg_conf
                FROM sec_filing_search_terms st
                JOIN sec_filings f ON st.filing_id = f.filing_id
                WHERE f.symbol = ?
                GROUP BY st.term_category
            """,
                [symbol],)

            # Initialize counts
            category_counts = {
                "growth_signals": 0,
                "hiring_plans": 0,
                "market_expansion": 0,
                "capex_investment": 0,
                "product_innovation": 0,
                "cost_efficiency": 0,
                "risk_factors": 0,
                "financial_health": 0,
                "strategic_initiatives": 0,
            }
            total_signals = 0
            confidence_sum = 0
            confidence_count = 0

            for category, count, avg_conf in signal_counts:
                if category in category_counts:
                    category_counts[category] = count
                total_signals += count
                if avg_conf:
                    confidence_sum += avg_conf * count
                    confidence_count += count

            avg_confidence = (
                confidence_sum / confidence_count if confidence_count > 0 else 0
            )

            # Calculate composite scores
            # Growth score: weighted combination of positive signals
            growth_score = min(
                1.0,
                (
                    category_counts["growth_signals"] * 0.3
                    + category_counts["hiring_plans"] * 0.2
                    + category_counts["market_expansion"] * 0.2
                    + category_counts["product_innovation"] * 0.15
                    + category_counts["capex_investment"] * 0.15
                )
                / 10,
            )  # Normalize

            # Risk score: based on risk signal density
            risk_score = min(1.0, category_counts["risk_factors"] / 20)

            # Upsert summary record
            conn.query(
                """
                INSERT INTO sec_company_bi_summary (
                    symbol, company_name, latest_10k_date, latest_10q_date,
                    total_filings, total_signals,
                    growth_signals_count, hiring_signals_count,
                    market_expansion_count, capex_signals_count,
                    innovation_signals_count, efficiency_signals_count,
                    risk_signals_count, financial_health_count,
                    strategic_signals_count, avg_confidence,
                    growth_score, risk_score, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT (symbol) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    latest_10k_date = EXCLUDED.latest_10k_date,
                    latest_10q_date = EXCLUDED.latest_10q_date,
                    total_filings = EXCLUDED.total_filings,
                    total_signals = EXCLUDED.total_signals,
                    growth_signals_count = EXCLUDED.growth_signals_count,
                    hiring_signals_count = EXCLUDED.hiring_signals_count,
                    market_expansion_count = EXCLUDED.market_expansion_count,
                    capex_signals_count = EXCLUDED.capex_signals_count,
                    innovation_signals_count = EXCLUDED.innovation_signals_count,
                    efficiency_signals_count = EXCLUDED.efficiency_signals_count,
                    risk_signals_count = EXCLUDED.risk_signals_count,
                    financial_health_count = EXCLUDED.financial_health_count,
                    strategic_signals_count = EXCLUDED.strategic_signals_count,
                    avg_confidence = EXCLUDED.avg_confidence,
                    growth_score = EXCLUDED.growth_score,
                    risk_score = EXCLUDED.risk_score,
                    updated_at = CURRENT_TIMESTAMP
            """,
                [
                    symbol,
                    company_name,
                    latest_10k,
                    latest_10q,
                    total_filings,
                    total_signals,
                    category_counts["growth_signals"],
                    category_counts["hiring_plans"],
                    category_counts["market_expansion"],
                    category_counts["capex_investment"],
                    category_counts["product_innovation"],
                    category_counts["cost_efficiency"],
                    category_counts["risk_factors"],
                    category_counts["financial_health"],
                    category_counts["strategic_initiatives"],
                    avg_confidence,
                    growth_score,
                    risk_score,
                ],
            ).result()

            total_updated += 1
        # Get summary statistics
        stats = bq.fetchone("""
            SELECT
                COUNT(*) as total_companies,
                AVG(total_signals) as avg_signals_per_company,
                AVG(growth_score) as avg_growth_score,
                AVG(risk_score) as avg_risk_score,
                SUM(total_signals) as total_all_signals
            FROM sec_company_bi_summary
        """)
        if stats is None:
            stats = (0, 0, 0, 0, 0)

        context.log.debug(
            f"BI summary complete. Updated {total_updated} companies. "
            f"Avg signals per company: {stats[1]:.1f}"
        )

        return dg.MaterializeResult(
            metadata={
                "companies_updated": total_updated,
                "total_companies": stats[0] or 0,
                "avg_signals_per_company": round(stats[1] or 0, 2),
                "avg_growth_score": round(stats[2] or 0, 4),
                "avg_risk_score": round(stats[3] or 0, 4),
                "total_signals": stats[4] or 0,
            }
        )

    finally:
        if conn:
            conn.close()
