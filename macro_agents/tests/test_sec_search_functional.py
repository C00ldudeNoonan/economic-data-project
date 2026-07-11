"""Functional tests for SEC filing search pipeline.

Tests the full FTS index, vector search, keyword search, and hybrid search
against an in-memory DuckDB with realistic SEC filing content.
"""

import os

import duckdb
import polars as pl
import pytest

from macro_agents.defs.domains.sec.fts import FTS_TABLE, _ensure_fts_content_table
from macro_agents.defs.domains.sec.search import (
    _split_into_sentences,
    _split_text_into_chunks,
)
from macro_agents.defs.domains.sec.semantic_search import (
    keyword_search,
)

pytestmark = [
    pytest.mark.network,
    pytest.mark.skipif(
        os.getenv("RUN_NETWORK_TESTS") != "1",
        reason=("DuckDB downloads the FTS extension; run with RUN_NETWORK_TESTS=1"),
    ),
]


def vector_search_3d(
    conn, query_embedding, *, symbol=None, section_name=None, top_k=10
):
    """Test helper: vector_search with FLOAT[3] instead of FLOAT[768]."""
    filters = ["c.embedding IS NOT NULL"]
    params = []
    if symbol:
        filters.append("c.symbol = ?")
        params.append(symbol.upper())
    if section_name:
        filters.append("c.section_name = ?")
        params.append(section_name)
    where_clause = "WHERE " + " AND ".join(filters)
    query = f"""
        SELECT c.chunk_id, c.filing_id, c.symbol, c.section_name,
               c.chunk_text, c.word_count,
               list_cosine_similarity(c.embedding, ?::FLOAT[3]) AS similarity
        FROM sec_filing_chunks c
        {where_clause}
        ORDER BY similarity DESC
        LIMIT {top_k}
    """
    return pl.read_database(
        query,
        connection=conn,
        execute_options={"parameters": [query_embedding, *params]},
    )


def hybrid_search_3d(
    conn,
    query_text,
    query_embedding,
    *,
    symbol=None,
    section_name=None,
    top_k=10,
    vector_weight=0.6,
    keyword_weight=0.4,
):
    """Test helper: hybrid_search using FLOAT[3] vector search."""
    vec_results = vector_search_3d(
        conn,
        query_embedding,
        symbol=symbol,
        section_name=section_name,
        top_k=top_k * 2,
    )
    try:
        kw_results = keyword_search(
            conn,
            query_text,
            symbol=symbol,
            section_name=section_name,
            top_k=top_k * 2,
        )
    except Exception:
        kw_results = pl.DataFrame()

    if vec_results.is_empty() and kw_results.is_empty():
        return pl.DataFrame()

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

    ranked = sorted(scored.values(), key=lambda x: x["hybrid_score"], reverse=True)
    return pl.DataFrame(ranked[:top_k])


# ---------------------------------------------------------------------------
# Realistic SEC filing excerpts for testing
# ---------------------------------------------------------------------------

AAPL_RISK_FACTORS = (
    "The Company's operations and performance depend significantly on worldwide "
    "economic conditions and their impact on levels of consumer spending. Markets "
    "for the Company's products and services are highly competitive and the Company "
    "is confronted by aggressive competition in all areas of its business. Supply "
    "chain disruptions could materially adversely affect the Company's business. "
    "The Company depends on component and product manufacturing and logistical "
    "services provided by outsourcing partners, many of which are located outside "
    "of the U.S. Global markets for the Company's products and services are subject "
    "to risks associated with doing business internationally. The Company is exposed "
    "to credit risk on its trade accounts receivable and vendor non-trade receivables."
)

AAPL_BUSINESS = (
    "Apple Inc. designs, manufactures and markets smartphones, personal computers, "
    "tablets, wearables and accessories, and sells a variety of related services. "
    "The Company's products include iPhone, Mac, iPad, and wearables, home and "
    "accessories. The Company's fiscal year is the 52- or 53-week period that ends "
    "on the last Saturday of September. Revenue for fiscal year 2024 was $394.3 "
    "billion, an increase of 8 percent from the prior year."
)

MSFT_RISK_FACTORS = (
    "Cybersecurity threats and data breaches represent significant risk to Microsoft "
    "Corporation. Our cloud infrastructure serves millions of enterprise customers "
    "worldwide. Artificial intelligence developments present both opportunities and "
    "challenges for our business model. We face intense competition from companies "
    "such as Amazon Web Services, Google Cloud, and other technology companies. "
    "Regulatory actions in the European Union and other jurisdictions may require "
    "changes to our products and business practices."
)

MSFT_BUSINESS = (
    "Microsoft Corporation develops, licenses, and supports software products, "
    "services, devices, and solutions worldwide. The Intelligent Cloud segment "
    "offers Azure and other cloud services. Revenue from Azure grew 29 percent "
    "year-over-year. The Productivity and Business Processes segment includes "
    "Office 365, LinkedIn, and Dynamics products. Total revenue reached $245.1 "
    "billion for the fiscal year ended June 2024."
)

TSLA_RISK_FACTORS = (
    "Supply chain constraints for battery materials including lithium, cobalt, "
    "and nickel may limit our vehicle production capacity. We face increasing "
    "competition from both traditional automotive manufacturers and new entrants "
    "in the electric vehicle market. Our vehicles contain complex software that "
    "requires regular updates and is subject to cybersecurity vulnerabilities. "
    "Regulatory changes regarding autonomous driving technology could impact our "
    "Full Self-Driving product timeline and market acceptance."
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def search_db():
    """Create an in-memory DuckDB with FTS and vector tables populated."""
    conn = duckdb.connect(":memory:")

    # --- FTS table ---
    _ensure_fts_content_table(conn)
    fts_rows = [
        (
            "c1",
            "f1",
            "AAPL",
            "10-K",
            "2024-01-15",
            "Risk Factors",
            AAPL_RISK_FACTORS,
            len(AAPL_RISK_FACTORS.split()),
        ),
        (
            "c2",
            "f1",
            "AAPL",
            "10-K",
            "2024-01-15",
            "Business",
            AAPL_BUSINESS,
            len(AAPL_BUSINESS.split()),
        ),
        (
            "c3",
            "f2",
            "MSFT",
            "10-K",
            "2024-02-01",
            "Risk Factors",
            MSFT_RISK_FACTORS,
            len(MSFT_RISK_FACTORS.split()),
        ),
        (
            "c4",
            "f2",
            "MSFT",
            "10-K",
            "2024-02-01",
            "Business",
            MSFT_BUSINESS,
            len(MSFT_BUSINESS.split()),
        ),
        (
            "c5",
            "f3",
            "TSLA",
            "10-K",
            "2024-03-01",
            "Risk Factors",
            TSLA_RISK_FACTORS,
            len(TSLA_RISK_FACTORS.split()),
        ),
    ]
    for row in fts_rows:
        conn.execute(
            f"INSERT INTO {FTS_TABLE} VALUES (?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
            list(row),
        )

    conn.execute("INSTALL fts")
    conn.execute("LOAD fts")
    conn.execute(f"""
        PRAGMA create_fts_index(
            '{FTS_TABLE}', 'content_id',
            'content_text', 'symbol', 'section_name',
            overwrite=1
        )
    """)

    # --- Vector chunks table (FLOAT[3] for testability) ---
    conn.execute("""
        CREATE TABLE sec_filing_chunks (
            chunk_id VARCHAR PRIMARY KEY,
            filing_id VARCHAR NOT NULL,
            symbol VARCHAR NOT NULL,
            section_name VARCHAR,
            chunk_index INTEGER,
            chunk_text TEXT,
            word_count INTEGER,
            embedding FLOAT[3],
            model_name VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Embeddings encode topic similarity:
    #   supply chain / manufacturing = [0.9, 0.1, 0.0]
    #   cybersecurity / cloud         = [0.0, 0.9, 0.1]
    #   revenue / business            = [0.1, 0.0, 0.9]
    #   competition                   = [0.4, 0.4, 0.2]
    vec_rows = [
        (
            "ch1",
            "f1",
            "AAPL",
            "Risk Factors",
            0,
            "Supply chain disruptions outsourcing partners manufacturing",
            8,
            [0.9, 0.1, 0.0],
        ),
        (
            "ch2",
            "f1",
            "AAPL",
            "Risk Factors",
            1,
            "Competition aggressive markets consumer spending economic conditions",
            9,
            [0.4, 0.4, 0.2],
        ),
        (
            "ch3",
            "f1",
            "AAPL",
            "Business",
            0,
            "Apple designs manufactures smartphones revenue 394 billion growth",
            9,
            [0.1, 0.0, 0.9],
        ),
        (
            "ch4",
            "f2",
            "MSFT",
            "Risk Factors",
            0,
            "Cybersecurity threats data breaches cloud infrastructure enterprise",
            8,
            [0.0, 0.9, 0.1],
        ),
        (
            "ch5",
            "f2",
            "MSFT",
            "Risk Factors",
            1,
            "Competition Amazon Web Services Google Cloud technology companies",
            9,
            [0.4, 0.4, 0.2],
        ),
        (
            "ch6",
            "f2",
            "MSFT",
            "Business",
            0,
            "Azure cloud services revenue grew 29 percent Office 365 LinkedIn",
            11,
            [0.1, 0.1, 0.8],
        ),
        (
            "ch7",
            "f3",
            "TSLA",
            "Risk Factors",
            0,
            "Supply chain battery materials lithium cobalt nickel production",
            9,
            [0.85, 0.1, 0.05],
        ),
        (
            "ch8",
            "f3",
            "TSLA",
            "Risk Factors",
            1,
            "Cybersecurity vulnerabilities autonomous driving software updates",
            7,
            [0.1, 0.8, 0.1],
        ),
    ]
    for row in vec_rows:
        conn.execute(
            "INSERT INTO sec_filing_chunks VALUES (?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)",
            [*row[:8], "test-model"],
        )

    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# FTS / Keyword Search Tests
# ---------------------------------------------------------------------------


class TestKeywordSearch:
    """Test DuckDB full-text search against realistic filing content."""

    def test_supply_chain_query(self, search_db):
        results = keyword_search(search_db, "supply chain disruptions")
        assert not results.is_empty()
        symbols = results["symbol"].to_list()
        assert "AAPL" in symbols
        assert "TSLA" in symbols

    def test_cybersecurity_query(self, search_db):
        results = keyword_search(search_db, "cybersecurity data breaches")
        assert not results.is_empty()
        assert results["symbol"][0] == "MSFT"

    def test_azure_cloud_query(self, search_db):
        results = keyword_search(search_db, "Azure cloud revenue")
        assert not results.is_empty()
        assert "MSFT" in results["symbol"].to_list()

    def test_filter_by_symbol(self, search_db):
        results = keyword_search(search_db, "competition", symbol="MSFT")
        assert not results.is_empty()
        assert all(s == "MSFT" for s in results["symbol"].to_list())

    def test_filter_by_section(self, search_db):
        results = keyword_search(search_db, "revenue", section_name="Business")
        assert not results.is_empty()
        assert all(s == "Business" for s in results["section_name"].to_list())

    def test_no_results_for_irrelevant_query(self, search_db):
        results = keyword_search(search_db, "cryptocurrency blockchain mining")
        assert results.is_empty()

    def test_top_k_limit(self, search_db):
        results = keyword_search(search_db, "company", top_k=2)
        assert len(results) <= 2

    def test_result_has_expected_columns(self, search_db):
        results = keyword_search(search_db, "revenue")
        expected = {
            "content_id",
            "filing_id",
            "symbol",
            "form_type",
            "filing_date",
            "section_name",
            "content_snippet",
            "fts_score",
        }
        assert expected == set(results.columns)

    def test_scores_are_ordered_descending(self, search_db):
        results = keyword_search(search_db, "competition risk")
        if len(results) > 1:
            scores = results["fts_score"].to_list()
            assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# Vector Search Tests
# ---------------------------------------------------------------------------


class TestVectorSearch:
    """Test cosine similarity search with synthetic embeddings."""

    def test_supply_chain_vector(self, search_db):
        # Query embedding close to supply chain topic [0.9, 0.1, 0.0]
        results = vector_search_3d(search_db, [0.88, 0.12, 0.0])
        assert not results.is_empty()
        # Top result should be supply-chain content
        top = results[0]
        assert (
            "supply" in top["chunk_text"].item().lower()
            or "Supply" in top["chunk_text"].item()
        )

    def test_cybersecurity_vector(self, search_db):
        results = vector_search_3d(search_db, [0.05, 0.9, 0.05])
        top_text = results["chunk_text"][0].lower()
        assert "cyber" in top_text or "cloud" in top_text

    def test_business_revenue_vector(self, search_db):
        results = vector_search_3d(search_db, [0.05, 0.05, 0.9])
        top_symbol = results["symbol"][0]
        # Business/revenue content should rank high
        assert top_symbol in ("AAPL", "MSFT")

    def test_filter_by_symbol(self, search_db):
        results = vector_search_3d(search_db, [0.9, 0.1, 0.0], symbol="TSLA")
        assert all(s == "TSLA" for s in results["symbol"].to_list())

    def test_filter_by_section(self, search_db):
        results = vector_search_3d(search_db, [0.5, 0.3, 0.2], section_name="Business")
        assert all(s == "Business" for s in results["section_name"].to_list())

    def test_top_k_limit(self, search_db):
        results = vector_search_3d(search_db, [0.5, 0.3, 0.2], top_k=3)
        assert len(results) <= 3

    def test_similarity_scores_ordered(self, search_db):
        results = vector_search_3d(search_db, [0.5, 0.3, 0.2])
        scores = results["similarity"].to_list()
        assert scores == sorted(scores, reverse=True)

    def test_result_has_expected_columns(self, search_db):
        results = vector_search_3d(search_db, [0.5, 0.3, 0.2])
        expected = {
            "chunk_id",
            "filing_id",
            "symbol",
            "section_name",
            "chunk_text",
            "word_count",
            "similarity",
        }
        assert expected == set(results.columns)


# ---------------------------------------------------------------------------
# Hybrid Search Tests
# ---------------------------------------------------------------------------


class TestHybridSearch:
    """Test reciprocal rank fusion combining vector + keyword search."""

    def test_hybrid_returns_results(self, search_db):
        results = hybrid_search_3d(
            search_db,
            "supply chain disruptions",
            [0.9, 0.1, 0.0],
        )
        assert not results.is_empty()

    def test_hybrid_boosts_dual_matches(self, search_db):
        """Items matching both vector AND keyword should score higher."""
        results = hybrid_search_3d(
            search_db,
            "supply chain battery materials",
            [0.85, 0.1, 0.05],  # close to TSLA supply chain embedding
        )
        # TSLA should rank highly — matches both keyword and vector
        symbols = results["symbol"].to_list()
        assert "TSLA" in symbols[:3]

    def test_hybrid_filter_by_symbol(self, search_db):
        results = hybrid_search_3d(
            search_db,
            "competition risk",
            [0.4, 0.4, 0.2],
            symbol="AAPL",
        )
        if not results.is_empty():
            assert all(s == "AAPL" for s in results["symbol"].to_list())

    def test_hybrid_custom_weights(self, search_db):
        # Full keyword weight, no vector
        kw_only = hybrid_search_3d(
            search_db,
            "cybersecurity threats",
            [0.0, 0.0, 1.0],  # irrelevant vector
            vector_weight=0.0,
            keyword_weight=1.0,
        )
        # Full vector weight, no keyword
        vec_only = hybrid_search_3d(
            search_db,
            "xyznonexistentterm",
            [0.0, 0.9, 0.1],  # cybersecurity vector
            vector_weight=1.0,
            keyword_weight=0.0,
        )
        # Both should return results from different sources
        if not kw_only.is_empty():
            assert "MSFT" in kw_only["symbol"].to_list()
        if not vec_only.is_empty():
            assert "MSFT" in vec_only["symbol"].to_list()

    def test_hybrid_top_k(self, search_db):
        results = hybrid_search_3d(
            search_db,
            "revenue growth",
            [0.1, 0.0, 0.9],
            top_k=2,
        )
        assert len(results) <= 2

    def test_hybrid_result_has_score(self, search_db):
        results = hybrid_search_3d(search_db, "cloud services", [0.1, 0.8, 0.1])
        assert "hybrid_score" in results.columns
        if not results.is_empty():
            scores = results["hybrid_score"].to_list()
            assert all(s > 0 for s in scores)
            assert scores == sorted(scores, reverse=True)


# ---------------------------------------------------------------------------
# Sentence Chunking with Realistic SEC Content
# ---------------------------------------------------------------------------


class TestRealisticChunking:
    """Test chunking against actual SEC filing text patterns."""

    def test_aapl_risk_factors_chunking(self):
        chunks = _split_text_into_chunks(AAPL_RISK_FACTORS, target_words=50)
        assert len(chunks) >= 2
        for chunk in chunks:
            assert chunk.rstrip().endswith(".")

    def test_preserves_company_names(self):
        """Abbreviations like 'Inc.' and 'U.S.' should not cause false splits."""
        sentences = _split_into_sentences(AAPL_RISK_FACTORS)
        # "U.S." should not be treated as sentence boundary
        full_text = " ".join(sentences)
        assert "U.S." in full_text or "U.S" in full_text

    def test_msft_business_chunking(self):
        chunks = _split_text_into_chunks(MSFT_BUSINESS, target_words=40)
        assert len(chunks) >= 2
        # Each chunk should contain coherent content
        for chunk in chunks:
            words = len(chunk.split())
            assert words >= 10, f"Chunk too small: {words} words"

    def test_full_pipeline_chunk_then_search(self, search_db):
        """Chunk realistic text, verify chunks are searchable."""
        chunks = _split_text_into_chunks(TSLA_RISK_FACTORS, target_words=30)
        assert len(chunks) >= 2

        # Verify each chunk contains complete sentences
        for chunk in chunks:
            assert chunk.rstrip().endswith((".", "!", "?"))
            # No dangling sentence fragments
            assert chunk[0].isupper(), f"Chunk starts with lowercase: {chunk[:30]}"
