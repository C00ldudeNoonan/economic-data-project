"""LLM-powered SEC filing analysis asset.

Generates structured summaries and vector embeddings for semantic search,
partitioned by ticker, using DSPy + Ollama.
"""

import hashlib

import dagster as dg
import dspy

from macro_agents.defs.domains.markets.partitions import sp500_company_tickers
from macro_agents.defs.domains.sec.tables import ensure_sec_filing_llm_metadata_table
from macro_agents.defs.domains.sec.config import MAX_ERROR_DETAILS
from macro_agents.defs.domains.sec.text import sec_filing_text_extracted
from macro_agents.defs.resources.bigquery_query import QueryArrayParameter
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.resources.ollama import OllamaResource
from macro_agents.defs.utils.sec_llm_analyzer import SECFilingAnalyzer

# Sections worth analyzing with LLM
ANALYSIS_SECTIONS = ("Business", "Risk Factors", "Management Discussion and Analysis")


def _generate_metadata_id(filing_id: str, section_name: str) -> str:
    """Generate a deterministic metadata ID for a filing section."""
    raw = f"{filing_id}:{section_name}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


@dg.asset(
    name="sec_filing_llm_analysis",
    group_name="transformation",
    kinds={"llm", "bigquery", "ollama"},
    partitions_def=sp500_company_tickers,
    deps=[sec_filing_text_extracted],
    description="Generate LLM summaries and embeddings for SEC filings (partitioned by ticker)",
)
def sec_filing_llm_analysis(
    context: dg.AssetExecutionContext,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
    ollama: OllamaResource,
) -> dg.MaterializeResult:
    """
    Per-ticker LLM analysis of SEC filing sections.

    For the given ticker partition:
    1. Query sec_filing_content for filings missing LLM metadata
    2. For each key section, run DSPy SECFilingAnalyzer for structured summary
    3. Generate vector embeddings via Ollama
    4. Upsert results into sec_filing_llm_metadata
    """
    ticker = context.partition_key

    conn = None
    try:
        conn = bq.get_connection()
        ensure_sec_filing_llm_metadata_table(conn)

        # Find filing sections for this ticker that don't have LLM metadata yet
        sections_to_process = bq.execute_query(
            f"""
            SELECT c.content_id, c.filing_id, c.section_name, c.gcs_path,
                   f.symbol, f.form_type
            FROM sec_filing_content c
            JOIN sec_filings f ON c.filing_id = f.filing_id
            WHERE f.symbol = '{ticker}'
            AND c.section_name IN ({", ".join(f"'{s}'" for s in ANALYSIS_SECTIONS)})
            AND NOT EXISTS (
                SELECT 1 FROM sec_filing_llm_metadata m
                WHERE m.filing_id = c.filing_id
                AND m.section_name = c.section_name
            )
            LIMIT 10
            """
        )

        if sections_to_process.is_empty():
            context.log.debug(f"No sections to analyze for {ticker}")
            return dg.MaterializeResult(
                metadata={
                    "status": "no_sections",
                    "ticker": ticker,
                    "sections_analyzed": 0,
                }
            )

        context.log.info(f"Analyzing {len(sections_to_process)} sections for {ticker}")

        # Configure DSPy with Ollama LM for this execution
        lm = ollama.get_dspy_lm()
        dspy.settings.configure(lm=lm)
        analyzer = SECFilingAnalyzer()

        total_analyzed = 0
        total_errors = 0
        errors = []

        for row in sections_to_process.iter_rows(named=True):
            filing_id = row["filing_id"]
            section_name = row["section_name"]
            gcs_path = row["gcs_path"]
            form_type = row["form_type"]

            try:
                # Download section content from GCS
                section_data = gcs.download_json(gcs_path, context=context)
                if not section_data or "content" not in section_data:
                    context.log.warning(
                        f"No content in GCS for {filing_id}/{section_name}"
                    )
                    continue

                content_text = section_data["content"]

                # Run DSPy analysis
                result = analyzer.forward(
                    filing_text=content_text,
                    form_type=form_type,
                    section_name=section_name,
                )

                # Generate embedding from the executive summary + key topics
                embed_text = f"{result.executive_summary} {result.key_topics}"
                embeddings = ollama.get_embeddings([embed_text])
                embedding = embeddings[0] if embeddings else None

                # Build metadata record
                metadata_id = _generate_metadata_id(filing_id, section_name)

                bq.execute_query(
                    """
                    MERGE sec_filing_llm_metadata AS target
                    USING (
                        SELECT
                            @metadata_id AS metadata_id,
                            @filing_id AS filing_id,
                            @symbol AS symbol,
                            @section_name AS section_name,
                            @executive_summary AS executive_summary,
                            @key_topics AS key_topics,
                            @sentiment AS sentiment,
                            @named_entities AS named_entities,
                            @financial_metrics AS financial_metrics,
                            @forward_looking_statements AS forward_looking_statements,
                            @risk_factors AS risk_factors,
                            @embedding AS embedding,
                            @model_name AS model_name
                    ) AS source
                    ON target.metadata_id = source.metadata_id
                    WHEN MATCHED THEN UPDATE SET
                        executive_summary = source.executive_summary,
                        key_topics = source.key_topics,
                        sentiment = source.sentiment,
                        named_entities = source.named_entities,
                        financial_metrics = source.financial_metrics,
                        forward_looking_statements = source.forward_looking_statements,
                        risk_factors = source.risk_factors,
                        embedding = source.embedding,
                        model_name = source.model_name
                    WHEN NOT MATCHED THEN INSERT (
                        metadata_id, filing_id, symbol, section_name,
                        executive_summary, key_topics, sentiment,
                        named_entities, financial_metrics,
                        forward_looking_statements, risk_factors,
                        embedding, model_name
                    ) VALUES (
                        source.metadata_id, source.filing_id, source.symbol,
                        source.section_name, source.executive_summary,
                        source.key_topics, source.sentiment,
                        source.named_entities, source.financial_metrics,
                        source.forward_looking_statements, source.risk_factors,
                        source.embedding, source.model_name
                    )
                    """,
                    read_only=False,
                    params={
                        "metadata_id": metadata_id,
                        "filing_id": filing_id,
                        "symbol": ticker,
                        "section_name": section_name,
                        "executive_summary": str(result.executive_summary),
                        "key_topics": str(result.key_topics),
                        "sentiment": str(result.sentiment),
                        "named_entities": str(result.named_entities),
                        "financial_metrics": str(result.financial_metrics),
                        "forward_looking_statements": str(
                            result.forward_looking_statements
                        ),
                        "risk_factors": str(result.risk_factors),
                        "embedding": QueryArrayParameter(embedding or [], "FLOAT64"),
                        "model_name": ollama._model,
                    },
                )
                total_analyzed += 1
                context.log.debug(f"Analyzed {section_name} for {ticker} ({filing_id})")

            except Exception as e:
                error_msg = (
                    f"Error analyzing {section_name} for {ticker} ({filing_id}): {e}"
                )
                context.log.error(error_msg)
                errors.append(error_msg)
                total_errors += 1
                continue

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "ticker": ticker,
                "sections_analyzed": total_analyzed,
                "errors": total_errors,
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
            }
        )

    finally:
        if conn:
            conn.close()
