import dagster as dg

from macro_agents.defs.domains.sec.metadata import sec_filing_metadata
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


@dg.asset(
    group_name="transformation",
    kinds={"database", "views"},
    deps=[sec_filing_metadata],
    description="Create database views for SEC filing dashboard queries. Reads from database tables populated by partitioned processing assets.",
)
def sec_filing_dashboard_views(
    context: dg.AssetExecutionContext,
    md: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Create database views for SEC filing analytics and dashboards.

    Views created:
    - sec_filing_overview: Summary of all filings with company info
    - sec_bi_signals_by_category: BI signals aggregated by category
    - sec_bi_signals_by_company: BI signals aggregated by company
    - sec_filing_trends: Time-series data for trend analysis
    - sec_growth_indicators: Companies showing growth signals
    - sec_risk_summary: Risk factor summary by company
    """
    conn = None
    try:
        conn = md.get_connection()

        # 1. Filing Overview View
        conn.query("""
            CREATE OR REPLACE VIEW sec_filing_overview AS
            SELECT
                f.filing_id,
                f.symbol,
                c.company_name,
                f.cik,
                f.form_type,
                f.filing_date,
                f.report_date,
                f.size_bytes,
                f.is_xbrl,
                f.gcs_path,
                f.processed,
                (SELECT COUNT(*) FROM sec_filing_content fc
                 WHERE fc.filing_id = f.filing_id) as section_count,
                (SELECT COUNT(*) FROM sec_filing_search_terms st
                 WHERE st.filing_id = f.filing_id) as signal_count
            FROM sec_filings f
            LEFT JOIN sec_company_cik c ON f.symbol = c.symbol
            ORDER BY f.filing_date DESC
        """).result()
        context.log.debug("Created view: sec_filing_overview")

        # 2. BI Signals by Category View
        conn.query("""
            CREATE OR REPLACE VIEW sec_bi_signals_by_category AS
            SELECT
                term_category,
                COUNT(*) as signal_count,
                COUNT(DISTINCT filing_id) as filing_count,
                AVG(confidence_score) as avg_confidence,
                MAX(confidence_score) as max_confidence
            FROM sec_filing_search_terms
            GROUP BY term_category
            ORDER BY signal_count DESC
        """).result()
        context.log.debug("Created view: sec_bi_signals_by_category")

        # 3. BI Signals by Company View
        conn.query("""
            CREATE OR REPLACE VIEW sec_bi_signals_by_company AS
            SELECT
                f.symbol,
                c.company_name,
                st.term_category,
                COUNT(*) as signal_count,
                AVG(st.confidence_score) as avg_confidence
            FROM sec_filing_search_terms st
            JOIN sec_filings f ON st.filing_id = f.filing_id
            LEFT JOIN sec_company_cik c ON f.symbol = c.symbol
            GROUP BY f.symbol, c.company_name, st.term_category
            ORDER BY f.symbol, signal_count DESC
        """).result()
        context.log.debug("Created view: sec_bi_signals_by_company")

        # 4. Filing Trends View (quarterly aggregation)
        conn.query("""
            CREATE OR REPLACE VIEW sec_filing_trends AS
            SELECT
                DATE_TRUNC('quarter', filing_date) as quarter,
                form_type,
                COUNT(*) as filing_count,
                COUNT(DISTINCT symbol) as company_count,
                AVG(size_bytes) as avg_filing_size
            FROM sec_filings
            WHERE filing_date IS NOT NULL
            GROUP BY DATE_TRUNC('quarter', filing_date), form_type
            ORDER BY quarter DESC, form_type
        """).result()
        context.log.debug("Created view: sec_filing_trends")

        # 5. Growth Indicators View
        conn.query("""
            CREATE OR REPLACE VIEW sec_growth_indicators AS
            SELECT
                f.symbol,
                c.company_name,
                f.form_type,
                f.filing_date,
                st.term_text,
                st.context_text,
                st.confidence_score,
                st.section_name
            FROM sec_filing_search_terms st
            JOIN sec_filings f ON st.filing_id = f.filing_id
            LEFT JOIN sec_company_cik c ON f.symbol = c.symbol
            WHERE st.term_category IN ('growth_signals', 'hiring_plans', 'market_expansion')
            AND st.confidence_score >= 0.6
            ORDER BY f.filing_date DESC, st.confidence_score DESC
        """).result()
        context.log.debug("Created view: sec_growth_indicators")

        # 6. Risk Summary View
        conn.query("""
            CREATE OR REPLACE VIEW sec_risk_summary AS
            SELECT
                f.symbol,
                c.company_name,
                f.form_type,
                f.filing_date,
                st.term_text,
                st.context_text,
                st.confidence_score
            FROM sec_filing_search_terms st
            JOIN sec_filings f ON st.filing_id = f.filing_id
            LEFT JOIN sec_company_cik c ON f.symbol = c.symbol
            WHERE st.term_category = 'risk_factors'
            AND st.confidence_score >= 0.5
            ORDER BY f.filing_date DESC, st.confidence_score DESC
        """).result()
        context.log.debug("Created view: sec_risk_summary")

        # 7. Company BI Profile View (latest signals per company)
        conn.query("""
            CREATE OR REPLACE VIEW sec_company_bi_profile AS
            WITH latest_filings AS (
                SELECT symbol, MAX(filing_date) as latest_filing_date
                FROM sec_filings
                WHERE form_type IN ('10-K', '10-Q')
                GROUP BY symbol
            ),
            category_signals AS (
                SELECT
                    f.symbol,
                    st.term_category,
                    COUNT(*) as category_count,
                    AVG(st.confidence_score) as avg_confidence
                FROM sec_filing_search_terms st
                JOIN sec_filings f ON st.filing_id = f.filing_id
                JOIN latest_filings lf ON f.symbol = lf.symbol
                    AND f.filing_date = lf.latest_filing_date
                GROUP BY f.symbol, st.term_category
            )
            SELECT
                cs.symbol,
                c.company_name,
                lf.latest_filing_date,
                cs.term_category,
                cs.category_count,
                cs.avg_confidence
            FROM category_signals cs
            JOIN latest_filings lf ON cs.symbol = lf.symbol
            LEFT JOIN sec_company_cik c ON cs.symbol = c.symbol
            ORDER BY cs.symbol, cs.category_count DESC
        """).result()
        context.log.debug("Created view: sec_company_bi_profile")

        # Embedding coverage view (only if chunks table exists)
        try:
            conn.query("""
                CREATE OR REPLACE VIEW sec_embedding_coverage AS
                SELECT
                    ch.symbol,
                    c.company_name,
                    COUNT(DISTINCT ch.filing_id) as filings_with_embeddings,
                    COUNT(*) as total_chunks,
                    SUM(ch.word_count) as total_words_embedded,
                    ch.model_name
                FROM sec_filing_chunks ch
                LEFT JOIN sec_company_cik c ON ch.symbol = c.symbol
                GROUP BY ch.symbol, c.company_name, ch.model_name
                ORDER BY total_chunks DESC
            """).result()
            context.log.debug("Created view: sec_embedding_coverage")
        except Exception:
            context.log.debug(
                "Skipped sec_embedding_coverage (sec_filing_chunks table not ready)"
            )
        # Get view statistics
        views = [
            "sec_filing_overview",
            "sec_bi_signals_by_category",
            "sec_bi_signals_by_company",
            "sec_filing_trends",
            "sec_growth_indicators",
            "sec_risk_summary",
            "sec_company_bi_profile",
        ]

        view_stats = {}
        for view in views:
            try:
                result = md.fetchone(f"SELECT COUNT(*) FROM {view}")
                view_stats[view] = result[0] if result else 0
            except Exception as e:
                view_stats[view] = f"Error: {e}"

        context.log.debug(f"Dashboard views created. Row counts: {view_stats}")

        return dg.MaterializeResult(
            metadata={
                "views_created": len(views),
                "view_names": ", ".join(views),
                **{f"{view}_rows": count for view, count in view_stats.items()},
            }
        )

    finally:
        if conn:
            conn.close()
