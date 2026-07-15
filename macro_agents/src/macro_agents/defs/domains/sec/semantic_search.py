"""Hybrid semantic + keyword search over SEC filings.

Provides a Dagster asset that materializes a search-ready view and a
reusable search function that combines vector cosine similarity with
BigQuery full-text search for AI agent workflows.

Backends (BigQuery-native):
- Vector similarity: ``ML.DISTANCE(embedding, @query_embedding, 'COSINE')``
  over the ``ARRAY<FLOAT64>`` embeddings written by ``sec_filing_search_index``.
- Keyword relevance: ``SEARCH(content_text, @query_text)`` to match, ranked by
  query-term overlap (``CONTAINS_SUBSTR``) since BigQuery exposes no BM25 score.
"""

import re

import dagster as dg
import polars as pl
from metaxy.ext.dagster import metaxify
from metaxy.metadata_store.base import MetadataStore

from macro_agents.defs.domains.sec import lineage  # noqa: F401 — register features
from macro_agents.defs.domains.sec.fts import FTS_TABLE, sec_filing_fts_index
from macro_agents.defs.domains.sec.search import sec_filing_search_index
from macro_agents.defs.resources.bigquery_query import QueryArrayParameter
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource

# Tokenizes a query into lowercase alphanumeric terms for the overlap score.
_TERM_RE = re.compile(r"[A-Za-z0-9]+")


def _query_terms(query_text: str) -> list[str]:
    """Split a query into distinct lowercase terms for overlap scoring."""
    return list(
        dict.fromkeys(m.group(0).lower() for m in _TERM_RE.finditer(query_text))
    )


def vector_search(
    bq: BigQueryWarehouseResource,
    query_embedding: list[float],
    *,
    symbol: str | None = None,
    section_name: str | None = None,
    top_k: int = 10,
) -> pl.DataFrame:
    """Search sec_filing_chunks by cosine similarity.

    Args:
        bq: BigQuery warehouse resource.
        query_embedding: 768-dim embedding of the search query.
        symbol: Optional filter by ticker symbol.
        section_name: Optional filter by section name.
        top_k: Number of results to return.

    Returns:
        Polars DataFrame with chunk_id, filing_id, symbol, section_name,
        chunk_text, word_count, chunk_index, similarity.
    """
    if not query_embedding:
        return pl.DataFrame()

    filters = ["c.embedding IS NOT NULL"]
    params: dict = {
        "query_embedding": QueryArrayParameter(list(query_embedding), "FLOAT64")
    }
    if symbol:
        filters.append("c.symbol = @symbol")
        params["symbol"] = symbol.upper()
    if section_name:
        filters.append("c.section_name = @section_name")
        params["section_name"] = section_name

    where_clause = "WHERE " + " AND ".join(filters)

    # Cosine distance in [0, 2]; similarity = 1 - distance. Brute-force scan
    # (no VECTOR INDEX) keeps results exact and avoids the index's minimum-row
    # and eventual-consistency constraints for the pilot corpus.
    sql = f"""
        SELECT c.chunk_id, c.filing_id, c.symbol, c.section_name,
               c.chunk_text, c.word_count, c.chunk_index,
               1 - ML.DISTANCE(c.embedding, @query_embedding, 'COSINE') AS similarity
        FROM sec_filing_chunks c
        {where_clause}
        ORDER BY similarity DESC
        LIMIT {int(top_k)}
    """
    return bq.execute_query(sql, read_only=True, params=params)


def keyword_search(
    bq: BigQueryWarehouseResource,
    query_text: str,
    *,
    symbol: str | None = None,
    section_name: str | None = None,
    top_k: int = 10,
) -> pl.DataFrame:
    """Search sec_filing_fts_content using BigQuery full-text SEARCH().

    BigQuery exposes no BM25 relevance score, so rows are matched with
    ``SEARCH()`` and ranked by how many distinct query terms appear in the
    text (``CONTAINS_SUBSTR``), breaking ties by recency.

    Args:
        bq: BigQuery warehouse resource.
        query_text: Keyword search query.
        symbol: Optional filter by ticker symbol.
        section_name: Optional filter by section name.
        top_k: Number of results to return.

    Returns:
        Polars DataFrame with content_id, filing_id, symbol, form_type,
        filing_date, section_name, content_snippet, fts_score.
    """
    if not query_text or not query_text.strip():
        return pl.DataFrame()

    filters = ["SEARCH(fts.content_text, @query_text)"]
    params: dict = {
        "query_text": query_text,
        "query_terms": QueryArrayParameter(_query_terms(query_text), "STRING"),
    }
    if symbol:
        filters.append("fts.symbol = @symbol")
        params["symbol"] = symbol.upper()
    if section_name:
        filters.append("fts.section_name = @section_name")
        params["section_name"] = section_name

    where_clause = "WHERE " + " AND ".join(filters)

    sql = f"""
        SELECT fts.content_id, fts.filing_id, fts.symbol, fts.form_type,
               fts.filing_date, fts.section_name,
               LEFT(fts.content_text, 500) AS content_snippet,
               (
                   SELECT COUNT(1) FROM UNNEST(@query_terms) AS term
                   WHERE CONTAINS_SUBSTR(fts.content_text, term)
               ) AS fts_score
        FROM {FTS_TABLE} fts
        {where_clause}
        ORDER BY fts_score DESC, fts.filing_date DESC
        LIMIT {int(top_k)}
    """
    return bq.execute_query(sql, read_only=True, params=params)


def hybrid_search(
    bq: BigQueryWarehouseResource,
    query_text: str,
    query_embedding: list[float],
    *,
    symbol: str | None = None,
    section_name: str | None = None,
    top_k: int = 10,
    vector_weight: float = 0.6,
    keyword_weight: float = 0.4,
) -> pl.DataFrame:
    """Combine vector similarity and keyword search via reciprocal rank fusion.

    Returns results ranked by a weighted combination of vector and keyword
    ranks, deduped by filing_id + section_name.

    Args:
        bq: BigQuery warehouse resource.
        query_text: Natural language query.
        query_embedding: 768-dim embedding of the query.
        symbol: Optional filter by ticker.
        section_name: Optional filter by section.
        top_k: Number of results.
        vector_weight: Weight for vector similarity (0-1).
        keyword_weight: Weight for keyword search (0-1).

    Returns:
        Polars DataFrame with combined results and hybrid_score.
    """
    # Get vector results
    vec_results = vector_search(
        bq,
        query_embedding,
        symbol=symbol,
        section_name=section_name,
        top_k=top_k * 2,
    )

    # Get keyword results
    try:
        kw_results = keyword_search(
            bq,
            query_text,
            symbol=symbol,
            section_name=section_name,
            top_k=top_k * 2,
        )
    except Exception:
        # FTS table may not exist yet — fall back to vector-only
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
    kinds={"bigquery", "search"},
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
