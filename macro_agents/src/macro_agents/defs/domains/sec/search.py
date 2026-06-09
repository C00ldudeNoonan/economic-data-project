"""SEC filing chunked embeddings for AI semantic search.

Splits SEC filing content into ~500-word sentence-aware chunks, generates
vector embeddings via Ollama, and stores them in sec_filing_chunks for
granular search retrieval.
"""

import hashlib
import re

import dagster as dg
from metaxy.ext.dagster import metaxify
from metaxy.metadata_store.base import MetadataStore

from macro_agents.defs.domains.sec import lineage  # noqa: F401 — register features
from macro_agents.defs.domains.sec.tables import ensure_sec_filing_chunks_table
from macro_agents.defs.domains.sec.text import sec_filing_text_extracted
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.domains.sec.config import (
    BATCH_SIZE_EMBEDDINGS,
    MAX_ERROR_DETAILS,
    OVERLAP_SENTENCES,
    TARGET_CHUNK_WORDS,
)
from macro_agents.defs.resources.ollama import OllamaResource

# Common abbreviations that should not trigger sentence splits.
_ABBREVIATIONS = {
    "Mr",
    "Mrs",
    "Ms",
    "Dr",
    "Inc",
    "Corp",
    "Ltd",
    "Jr",
    "Sr",
    "vs",
    "etc",
}

# Sentence boundary: period/question/exclamation + whitespace + uppercase letter,
# or double newlines (paragraph breaks).
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+(?=[A-Z])|\n{2,}")


def _generate_chunk_id(filing_id: str, section_name: str, chunk_index: int) -> str:
    raw = f"{filing_id}:{section_name}:{chunk_index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _split_into_sentences(text: str) -> list[str]:
    """Split text into sentences using regex-based boundary detection.

    Rejoins fragments that were split after common abbreviations.
    """
    raw = _SENTENCE_SPLIT_RE.split(text)
    sentences: list[str] = []
    for fragment in raw:
        fragment = fragment.strip()
        if not fragment:
            continue
        # If previous sentence ends with an abbreviation, rejoin
        if sentences:
            last_word = sentences[-1].rsplit(None, 1)[-1].rstrip(".")
            if last_word in _ABBREVIATIONS:
                sentences[-1] = sentences[-1] + " " + fragment
                continue
        sentences.append(fragment)
    return sentences


def _split_text_into_chunks(
    text: str,
    target_words: int = TARGET_CHUNK_WORDS,
    overlap_sentences: int = OVERLAP_SENTENCES,
) -> list[str]:
    """Split text into overlapping chunks respecting sentence boundaries.

    Groups sentences into chunks of ~target_words, overlapping by
    overlap_sentences sentences to preserve cross-chunk context.
    """
    sentences = _split_into_sentences(text)
    if not sentences:
        return []

    total_words = sum(len(s.split()) for s in sentences)
    if total_words <= target_words:
        return [text.strip()]

    chunks: list[str] = []
    current_sentences: list[str] = []
    current_words = 0

    for sentence in sentences:
        word_count = len(sentence.split())
        current_sentences.append(sentence)
        current_words += word_count

        if current_words >= target_words:
            chunks.append(" ".join(current_sentences))
            # Keep last N sentences as overlap for next chunk
            overlap = current_sentences[-overlap_sentences:]
            current_sentences = list(overlap)
            current_words = sum(len(s.split()) for s in current_sentences)

    # Add remaining sentences as final chunk
    if current_sentences:
        remaining = " ".join(current_sentences)
        # Merge into previous chunk if too small (< 25% of target)
        if chunks and current_words < target_words // 4:
            chunks[-1] = chunks[-1] + " " + remaining
        else:
            chunks.append(remaining)

    return chunks


@metaxify(key="sec_filing_search_index")
@dg.asset(
    group_name="transformation",
    kinds={"llm", "duckdb", "ollama"},
    deps=[sec_filing_text_extracted],
    description=(
        "Generate chunked embeddings from SEC filing text for semantic search. "
        "Splits content into ~500-token chunks and embeds via Ollama."
    ),
    metadata={"metaxy/feature": "sec/embeddings"},
)
def sec_filing_search_index(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    gcs: GCSResource,
    ollama: OllamaResource,
    metaxy_store: dg.ResourceParam[MetadataStore],
) -> dg.MaterializeResult:
    """
    Build chunked vector search index from SEC filing content.

    Steps:
    1. Find filing sections not yet fully chunked (content stored in GCS)
    2. Download section content from GCS
    3. Split each section into ~500-word overlapping chunks
    4. Generate embeddings for each chunk via Ollama
    5. Store chunk text + embedding in sec_filing_chunks
    """
    conn = None
    metaxy_stale_count: int | None = None
    metaxy_divergence_count: int | None = None
    metaxy_shadow_error: str | None = None
    try:
        conn = bq.get_connection()
        ensure_sec_filing_chunks_table(conn)

        batch_size = BATCH_SIZE_EMBEDDINGS

        # Content text is stored in GCS, not in the DB column.
        # Query gcs_path and word_count, then download content from GCS.
        # Use LEFT JOIN to detect partially embedded sections: compare
        # expected chunk count (from word_count) against actual chunks stored.
        sections_to_process = bq.execute_query(
            f"""
            SELECT c.content_id, c.filing_id, c.section_name,
                   c.gcs_path, c.word_count, f.symbol,
                   COALESCE(ch_count.chunk_count, 0) AS existing_chunks
            FROM sec_filing_content c
            JOIN sec_filings f ON f.filing_id = c.filing_id
            LEFT JOIN (
                SELECT filing_id, section_name, COUNT(*) AS chunk_count
                FROM sec_filing_chunks
                GROUP BY filing_id, section_name
            ) ch_count ON ch_count.filing_id = c.filing_id
                AND ch_count.section_name = c.section_name
            WHERE c.gcs_path IS NOT NULL
            AND c.word_count > 50
            AND c.section_name IN (
                'Business', 'Risk Factors',
                'Management Discussion and Analysis',
                'full_text'
            )
            ORDER BY f.filing_date DESC
            LIMIT {batch_size * 4}
            """
        )

        # Shadow mode (issue #46 Phase 2): expansion lineage. Roll Metaxy's
        # chunk-level new+stale rows up to filing_ids and compare to the
        # filing_ids surfaced by the section-level boolean query above.
        try:
            boolean_stale_filing_ids = set(
                sections_to_process["filing_id"].unique().to_list()
            )
            with metaxy_store:
                increment = metaxy_store.resolve_update("sec/embeddings").to_polars()
                metaxy_stale_filing_ids: set[str] = set()
                for frame in (increment.new, increment.stale):
                    if "filing_id" in frame.columns:
                        metaxy_stale_filing_ids.update(
                            frame["filing_id"].unique().to_list()
                        )
            metaxy_stale_count = len(metaxy_stale_filing_ids)
            metaxy_divergence_count = len(
                metaxy_stale_filing_ids.symmetric_difference(boolean_stale_filing_ids)
            )
        except Exception as e:  # noqa: BLE001 — shadow mode must never fail the asset
            metaxy_shadow_error = f"{type(e).__name__}: {e}"
            context.log.warning(
                f"Metaxy shadow comparison failed: {metaxy_shadow_error}"
            )

        if sections_to_process.is_empty():
            context.log.debug("No sections pending chunk embedding")
            return dg.MaterializeResult(
                metadata={
                    "status": "up_to_date",
                    "chunks_created": 0,
                    "metaxy_stale_count": metaxy_stale_count,
                    "metaxy_divergence_count": metaxy_divergence_count,
                    "metaxy_shadow_error": metaxy_shadow_error,
                }
            )

        total_chunks = 0
        skipped = 0
        errors = []

        for row in sections_to_process.iter_rows(named=True):
            filing_id = row["filing_id"]
            section_name = row["section_name"]
            gcs_path = row["gcs_path"]
            symbol = row["symbol"]
            word_count = row["word_count"]
            existing_chunks = row["existing_chunks"]

            # With sentence-aware chunking, exact chunk count depends on
            # sentence boundaries. Use a rough estimate: skip only if we have
            # at least the minimum expected chunks for this word count.
            if word_count <= TARGET_CHUNK_WORDS:
                min_expected_chunks = 1
            else:
                min_expected_chunks = max(1, word_count // TARGET_CHUNK_WORDS)

            # Skip if section is already sufficiently embedded
            if existing_chunks >= min_expected_chunks:
                skipped += 1
                continue

            try:
                # Download section content from GCS
                section_data = gcs.download_json(gcs_path)
                content_text = section_data.get("content", "")

                if not content_text:
                    continue

                chunks = _split_text_into_chunks(content_text)
                if not chunks:
                    continue

                # If partially embedded, delete existing chunks and re-embed
                if existing_chunks > 0:
                    conn.query(
                        "DELETE FROM sec_filing_chunks "
                        "WHERE filing_id = ? AND section_name = ?",
                        [filing_id, section_name],  # ty: ignore[invalid-argument-type]
                    ).result()
                    context.log.debug(
                        f"Cleared {existing_chunks} partial chunks for "
                        f"{symbol}/{section_name}, re-embedding {len(chunks)}"
                    )

                for i, chunk_text in enumerate(chunks):
                    chunk_id = _generate_chunk_id(filing_id, section_name, i)
                    chunk_word_count = len(chunk_text.split())

                    # Generate embedding
                    embeddings = ollama.get_embeddings([chunk_text])
                    embedding = embeddings[0] if embeddings else None

                    conn.query(
                        """
                        INSERT INTO sec_filing_chunks
                        (chunk_id, filing_id, symbol, section_name,
                         chunk_index, chunk_text, word_count,
                         embedding, model_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT (chunk_id) DO UPDATE SET
                            chunk_text = EXCLUDED.chunk_text,
                            word_count = EXCLUDED.word_count,
                            embedding = EXCLUDED.embedding,
                            model_name = EXCLUDED.model_name
                        """,
                        [  # ty: ignore[invalid-argument-type]
                            chunk_id,
                            filing_id,
                            symbol,
                            section_name,
                            i,
                            chunk_text,
                            chunk_word_count,
                            embedding,
                            ollama._embedding_model,
                        ],
                    ).result()

                    total_chunks += 1
                context.log.debug(
                    f"Embedded {len(chunks)} chunks for {symbol}/{section_name}"
                )

            except Exception as e:
                error_msg = (
                    f"Error chunking {symbol} filing {filing_id} "
                    f"section {section_name}: {e}"
                )
                context.log.warning(error_msg)
                errors.append(error_msg)
                continue

        context.log.info(f"Created {total_chunks} chunk embeddings, skipped {skipped}")

        return dg.MaterializeResult(
            metadata={
                "status": "completed",
                "sections_processed": len(sections_to_process) - skipped,
                "sections_skipped": skipped,
                "chunks_created": total_chunks,
                "errors": len(errors),
                "error_details": errors[:MAX_ERROR_DETAILS] if errors else [],
                "metaxy_stale_count": metaxy_stale_count,
                "metaxy_divergence_count": metaxy_divergence_count,
                "metaxy_shadow_error": metaxy_shadow_error,
            }
        )

    finally:
        if conn:
            conn.close()
