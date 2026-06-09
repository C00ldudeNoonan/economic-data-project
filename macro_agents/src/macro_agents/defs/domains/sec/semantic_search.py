"""Hybrid semantic + keyword search over SEC filings.

Provides a Dagster asset that materializes a search-ready view and a
reusable search function that combines vector cosine similarity with
DuckDB full-text search for AI agent workflows.
"""

import dagster as dg
import polars as pl
from metaxy.ext.dagster import metaxify
from metaxy.metadata_store.base import MetadataStore

from macro_agents.defs.domains.sec import lineage  # noqa: F401 — register features
from macro_agents.defs.domains.sec.fts import FTS_TABLE, sec_filing_fts_index
from macro_agents.defs.domains.sec.search import sec_filing_search_index
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource


def vector_search(
    conn,
    query_embedding: list[float],
    *,
    symbol: str | None = None,
    section_name: str | None = None,
    top_k: int = 10,
) -> pl.DataFrame:
    """Search sec_filing_chunks by cosine similarity.

    Args:
        conn: DuckDB connection.
        query_embedding: 768-dim embedding of the search query.
        symbol: Optional filter by ticker symbol.
        section_name: Optional filter by section name.
        top_k: Number of results to return.

    Returns:
        Polars DataFrame with chunk_id, filing_id, symbol, section_name,
        chunk_text, word_count, similarity_score.
    """
    filters = []
    params = []

    if symbol:
        filters.append("c.symbol = ?")
        params.append(symbol.upper())
    if section_name:
        filters.append("c.section_name = ?")
        params.append(section_name)

    filters.append("c.embedding IS NOT NULL")
    where_clause = "WHERE " + " AND ".join(filters)

    sql = f"""
        SELECT c.chunk_id, c.filing_id, c.symbol, c.section_name,
               c.chunk_text, c.word_count,
               list_cosine_similarity(c.embedding, ?::FLOAT[768]) AS similarity
        FROM sec_filing_chunks c
        {where_clause}
        ORDER BY similarity DESC
        LIMIT {top_k}
    """
    return conn.execute(sql, [query_embedding, *params]).pl()


def keyword_search(
    conn,
    query_text: str,
    *,
    symbol: str | None = None,
    section_name: str | None = None,
    top_k: int = 10,
) -> pl.DataFrame:
    """Search sec_filing_fts_content using DuckDB full-text search.

    Args:
        conn: DuckDB connection.
        query_text: Keyword search query.
        symbol: Optional filter by ticker symbol.
        section_name: Optional filter by section name.
        top_k: Number of results to return.

    Returns:
        Polars DataFrame with content_id, filing_id, symbol, form_type,
        filing_date, section_name, content_text (snippet), fts_score.
    """
    filters = []
    params = []

    if symbol:
        filters.append("fts.symbol = ?")
        params.append(symbol.upper())
    if section_name:
        filters.append("fts.section_name = ?")
        params.append(section_name)

    where_clause = f"AND {' AND '.join(filters)}" if filters else ""

    sql = f"""
        SELECT fts.content_id, fts.filing_id, fts.symbol, fts.form_type,
               fts.filing_date, fts.section_name,
               LEFT(fts.content_text, 500) AS content_snippet,
               fts_main_{FTS_TABLE}.match_bm25(
                   fts.content_id, ?, fields := 'content_text'
               ) AS fts_score
        FROM {FTS_TABLE} fts
        WHERE fts_score IS NOT NULL
        {where_clause}
        ORDER BY fts_score DESC
        LIMIT {top_k}
    """
    return conn.execute(sql, [query_text, *params]).pl()


def hybrid_search(
    conn,
    query_text: str,
    query_embedding: list[float],
    *,
    symbol: str | None = None,
    section_name: str | None = None,
    top_k: int = 10,
    vector_weight: float = 0.6,
    keyword_weight: float = 0.4,
) -> pl.DataFrame:
    """Combine vector similarity and keyword FTS via reciprocal rank fusion.

    Returns results ranked by a weighted combination of vector and keyword
    scores, deduped by filing_id + section_name.

    Args:
        conn: DuckDB connection.
        query_text: Natural language query.
        query_embedding: 768-dim embedding of the query.
        symbol: Optional filter by ticker.
        section_name: Optional filter by section.
        top_k: Number of results.
        vector_weight: Weight for vector similarity (0-1).
        keyword_weight: Weight for keyword FTS (0-1).

    Returns:
        Polars DataFrame with combined results and hybrid_score.
    """
    # Get vector results
    vec_results = vector_search(
        conn,
        query_embedding,
        symbol=symbol,
        section_name=section_name,
        top_k=top_k * 2,
    )

    # Get keyword results
    try:
        kw_results = keyword_search(
            conn,
            query_text,
            symbol=symbol,
            section_name=section_name,
            top_k=top_k * 2,
        )
    except Exception:
        # FTS index may not exist yet — fall back to vector-only
        kw_results = pl.DataFrame()

    if vec_results.is_empty() and kw_results.is_empty():
        return pl.DataFrame()

    # Reciprocal rank fusion: score = weight / (rank + 60)
    # The constant 60 prevents top-ranked items from dominating
    rrf_k = 60
    scored: dict[str, dict] = {}

    for rank, row in enumerate(vec_results.iter_rows(named=True)):
        key = f"{row['filing_id']}:{row['section_name']}:{row.get('chunk_index', rank)}"
        scored[key] = {
            "filing_id": row["filing_id"],
            "symbol": row["symbol"],
            "section_name": row["section_name"],
            "text": row["chunk_text"],
            "word_count": row["word_count"],
            "vector_similarity": row["similarity"],
            "hybrid_score": vector_weight / (rank + rrf_k),
        }

    for rank, row in enumerate(kw_results.iter_rows(named=True)):
        key = f"{row['filing_id']}:{row['section_name']}:kw"
        if key in scored:
            scored[key]["hybrid_score"] += keyword_weight / (rank + rrf_k)
            scored[key]["fts_score"] = row["fts_score"]
        else:
            scored[key] = {
                "filing_id": row["filing_id"],
                "symbol": row["symbol"],
                "section_name": row["section_name"],
                "text": row["content_snippet"],
                "word_count": None,
                "vector_similarity": None,
                "fts_score": row["fts_score"],
                "hybrid_score": keyword_weight / (rank + rrf_k),
            }

    # Sort by hybrid score descending, take top_k
    ranked = sorted(scored.values(), key=lambda x: x["hybrid_score"], reverse=True)
    return pl.DataFrame(ranked[:top_k])


@metaxify(key="sec_filing_hybrid_search_ready")
@dg.asset(
    group_name="transformation",
    kinds={"duckdb", "search"},
    deps=[sec_filing_search_index, sec_filing_fts_index],
    description=(
        "Validate hybrid search readiness by checking vector embeddings "
        "and FTS index coverage. Materializes search coverage stats."
    ),
    metadata={"metaxy/feature": "sec/search_index"},
)
def sec_filing_hybrid_search_ready(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    metaxy_store: dg.ResourceParam[MetadataStore],
) -> dg.MaterializeResult:
    """Check that both vector and FTS indexes are populated and queryable."""
    conn = None
    metaxy_stale_count: int | None = None
    metaxy_shadow_error: str | None = None

    # Shadow mode (issue #46): aggregation lineage. No boolean staleness
    # check exists for this validator asset, so we only probe that the
    # aggregation lookup works and record the stale count.
    try:
        with metaxy_store:
            increment = metaxy_store.resolve_update("sec/search_index").to_polars()
        metaxy_stale_count = sum(
            len(frame) for frame in (increment.new, increment.stale)
        )
    except Exception as e:  # noqa: BLE001 — shadow mode must never fail the asset
        metaxy_shadow_error = f"{type(e).__name__}: {e}"
        context.log.warning(f"Metaxy shadow probe failed: {metaxy_shadow_error}")

    try:
        conn = bq.get_connection()

        # Check vector index coverage
        vec_row = bq.fetchone("""
            SELECT
                COUNT(DISTINCT symbol) AS symbols_with_embeddings,
                COUNT(DISTINCT filing_id) AS filings_with_embeddings,
                COUNT(*) AS total_chunks
            FROM sec_filing_chunks
            WHERE embedding IS NOT NULL
        """)
        vec_stats = vec_row if vec_row else (0, 0, 0)

        # Check FTS table coverage
        try:
            fts_row = bq.fetchone(f"""
                SELECT
                    COUNT(DISTINCT symbol) AS symbols_indexed,
                    COUNT(DISTINCT filing_id) AS filings_indexed,
                    COUNT(*) AS total_sections
                FROM {FTS_TABLE}
            """)
            fts_stats = fts_row if fts_row else (0, 0, 0)
        except Exception:
            fts_stats = (0, 0, 0)

        context.log.info(
            f"Vector: {vec_stats[2]} chunks across {vec_stats[1]} filings, "
            f"{vec_stats[0]} symbols | "
            f"FTS: {fts_stats[2]} sections across {fts_stats[1]} filings, "
            f"{fts_stats[0]} symbols"
        )

        return dg.MaterializeResult(
            metadata={
                "vector_chunks": vec_stats[2],
                "vector_filings": vec_stats[1],
                "vector_symbols": vec_stats[0],
                "fts_sections": fts_stats[2],
                "fts_filings": fts_stats[1],
                "fts_symbols": fts_stats[0],
                "search_ready": vec_stats[2] > 0 or fts_stats[2] > 0,
                "metaxy_stale_count": metaxy_stale_count,
                "metaxy_shadow_error": metaxy_shadow_error,
            }
        )

    finally:
        if conn:
            conn.close()
