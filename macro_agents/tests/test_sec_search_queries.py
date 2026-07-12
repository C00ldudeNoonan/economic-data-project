"""Tests for BigQuery-native SEC search query builders (issue #130).

These assert that vector/keyword/hybrid search emit valid BigQuery Standard
SQL — not DuckDB — by running each generated statement through the same
``prepare_query_parameters`` contract the warehouse resource uses at runtime.
"""

from pathlib import Path
from unittest.mock import Mock

import polars as pl

from macro_agents.defs.domains.sec import semantic_search as ss
from macro_agents.defs.resources.bigquery_query import prepare_query_parameters


def _capture(fn, *args, **kwargs) -> tuple[str, dict]:
    """Invoke a search builder with a mocked warehouse and return (sql, params)."""
    bq = Mock()
    bq.execute_query.return_value = pl.DataFrame()
    fn(bq, *args, **kwargs)
    call = bq.execute_query.call_args
    return call.args[0], call.kwargs["params"]


def test_vector_search_emits_valid_bigquery_cosine_sql() -> None:
    sql, params = _capture(ss.vector_search, [0.1] * 768, top_k=5)
    assert "ML.DISTANCE(c.embedding, @query_embedding, 'COSINE')" in sql
    assert "list_cosine_similarity" not in sql  # DuckDB primitive gone
    assert "::FLOAT" not in sql
    # Contract-parses as a single BigQuery SELECT with matching params.
    prepare_query_parameters(sql, read_only=True, params=params)
    assert set(params) == {"query_embedding"}


def test_vector_search_applies_symbol_and_section_filters() -> None:
    sql, params = _capture(
        ss.vector_search,
        [0.1] * 768,
        symbol="aapl",
        section_name="Risk Factors",
        top_k=5,
    )
    assert "c.symbol = @symbol" in sql
    assert "c.section_name = @section_name" in sql
    assert params["symbol"] == "AAPL"  # upper-cased
    prepare_query_parameters(sql, read_only=True, params=params)


def test_vector_search_empty_embedding_short_circuits() -> None:
    bq = Mock()
    assert ss.vector_search(bq, [], top_k=5).is_empty()
    bq.execute_query.assert_not_called()


def test_keyword_search_emits_valid_bigquery_search_sql() -> None:
    sql, params = _capture(ss.keyword_search, "supply chain risk", top_k=5)
    assert "SEARCH(fts.content_text, @query_text)" in sql
    assert "CONTAINS_SUBSTR(fts.content_text, term)" in sql
    assert "match_bm25" not in sql  # DuckDB FTS primitive gone
    prepare_query_parameters(sql, read_only=True, params=params)
    assert set(params) == {"query_text", "query_terms"}
    assert params["query_terms"].values == ("supply", "chain", "risk")


def test_keyword_search_blank_query_short_circuits() -> None:
    bq = Mock()
    assert ss.keyword_search(bq, "   ", top_k=5).is_empty()
    bq.execute_query.assert_not_called()


def test_hybrid_search_fuses_vector_and_keyword_ranks() -> None:
    vec = pl.DataFrame(
        {
            "chunk_id": ["c1"],
            "filing_id": ["f1"],
            "symbol": ["AAPL"],
            "section_name": ["Risk Factors"],
            "chunk_text": ["vector hit"],
            "word_count": [10],
            "chunk_index": [0],
            "similarity": [0.9],
        }
    )
    kw = pl.DataFrame(
        {
            "content_id": ["k1"],
            "filing_id": ["f2"],
            "symbol": ["MSFT"],
            "form_type": ["10-K"],
            "filing_date": ["2026-01-01"],
            "section_name": ["Business"],
            "content_snippet": ["keyword hit"],
            "fts_score": [3],
        }
    )
    bq = Mock()
    bq.execute_query.side_effect = [vec, kw]

    result = ss.hybrid_search(bq, "supply chain", [0.1] * 768, top_k=10)

    # Both a vector-only and a keyword-only result survive fusion.
    assert set(result["filing_id"].to_list()) == {"f1", "f2"}
    assert "hybrid_score" in result.columns
    assert bq.execute_query.call_count == 2


def test_sec_search_modules_contain_no_duckdb_primitives() -> None:
    """Guard against DuckDB SQL creeping back into the SEC search modules."""
    sec_dir = Path(ss.__file__).parent
    forbidden = [
        "list_cosine_similarity",
        "match_bm25",
        "PRAGMA",
        "INSTALL fts",
        "LOAD fts",
        ".pl()",
        "conn.execute(",
    ]
    for name in ("semantic_search.py", "fts.py", "search.py"):
        text = (sec_dir / name).read_text(encoding="utf-8")
        for token in forbidden:
            assert token not in text, (
                f"{name} still contains DuckDB primitive {token!r}"
            )
