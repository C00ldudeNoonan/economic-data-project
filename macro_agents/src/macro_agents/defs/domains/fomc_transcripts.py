"""FOMC meeting transcripts ingestion and processing assets."""

import hashlib
import re
from datetime import datetime, timezone

import polars as pl
import dagster as dg

from macro_agents.defs.analysis.fed_sentiment.resource import FedSentimentResource
from macro_agents.defs.resources.federal_reserve import FederalReserveResource
from macro_agents.defs.resources.gcs import GCSResource
from macro_agents.defs.resources.bigquery_warehouse import BigQueryWarehouseResource
from macro_agents.defs.resources.pdf import PDFResource


# Partition by year (transcripts are released 5 years after meeting)
# So 2018 transcripts were released in 2023, etc.
fomc_transcript_years_partition = dg.StaticPartitionsDefinition(
    [str(year) for year in range(2010, 2020)]  # Up to 2019 (released in 2024)
)


@dg.asset(
    group_name="macro_ingestion",
    kinds={"database", "schema"},
    description="Initialize FOMC transcript database tables",
)
def fomc_transcript_schema(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Create database tables for FOMC transcripts, summaries, and related data.

    Tables created:
    - fomc_transcripts: Raw transcript storage
    - transcript_sections: Parsed sections with speakers
    - fomc_meeting_summaries: AI-generated summaries
    - transcript_topics: Topic extraction and sentiment
    - transcript_search_index: Full-text search support
    - member_voting_history: Member votes and positions
    """
    conn = None
    try:
        conn = bq.get_connection()

        # 1. FOMC Transcripts Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS fomc_transcripts (
                transcript_id VARCHAR PRIMARY KEY,
                meeting_date DATE NOT NULL,
                full_text TEXT,
                word_count INTEGER,
                page_count INTEGER,
                source_url VARCHAR,
                source_pdf_path VARCHAR,
                processed_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 2. Transcript Sections Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS transcript_sections (
                section_id VARCHAR PRIMARY KEY,
                transcript_id VARCHAR NOT NULL,
                section_order INTEGER,
                section_type VARCHAR,
                speaker VARCHAR,
                speaker_role VARCHAR,
                content TEXT,
                start_page INTEGER,
                end_page INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 3. FOMC Meeting Summaries Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS fomc_meeting_summaries (
                summary_id VARCHAR PRIMARY KEY,
                meeting_date DATE NOT NULL,
                summary_type VARCHAR,
                summary_text TEXT,
                key_decisions VARCHAR[],
                dissenting_views VARCHAR[],
                economic_outlook_summary TEXT,
                policy_rationale TEXT,
                notable_quotes VARCHAR[],
                generated_by VARCHAR,
                generation_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 4. Transcript Topics Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS transcript_topics (
                topic_id VARCHAR PRIMARY KEY,
                transcript_id VARCHAR NOT NULL,
                section_id VARCHAR,
                topic VARCHAR,
                subtopic VARCHAR,
                relevance_score DECIMAL(3,2),
                mentioned_by VARCHAR[],
                sentiment VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 5. Transcript Search Index Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS transcript_search_index (
                search_id VARCHAR PRIMARY KEY,
                transcript_id VARCHAR NOT NULL,
                section_id VARCHAR,
                searchable_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 6. Member Voting History Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS member_voting_history (
                vote_id VARCHAR PRIMARY KEY,
                meeting_date DATE NOT NULL,
                member_id VARCHAR,
                member_name VARCHAR,
                vote VARCHAR,
                dissent_reason TEXT,
                stance VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 7. Enhanced FOMC Meetings Table (extend existing or create)
        conn.query("""
            CREATE TABLE IF NOT EXISTS fomc_meetings_enhanced (
                meeting_date DATE PRIMARY KEY,
                action VARCHAR,
                rate_change_bps INTEGER,
                target_rate_lower DECIMAL(4,2),
                target_rate_upper DECIMAL(4,2),
                forecast_update BOOLEAN,
                statement_url VARCHAR,
                transcript_available BOOLEAN DEFAULT FALSE,
                transcript_release_date DATE,
                summary_available BOOLEAN DEFAULT FALSE,
                meeting_type VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()

        # 8. Fed Sentiment Scores Table
        conn.query("""
            CREATE TABLE IF NOT EXISTS fomc_sentiment_scores (
                score_id VARCHAR PRIMARY KEY,
                transcript_id VARCHAR NOT NULL,
                section_id VARCHAR,
                meeting_date DATE NOT NULL,
                speaker VARCHAR,
                scoring_method VARCHAR NOT NULL,
                hawkish_score DECIMAL(4,3),
                dovish_score DECIMAL(4,3),
                net_sentiment_score DECIMAL(4,3),
                confidence DECIMAL(3,2),
                keyword_counts JSON,
                total_hawkish_keywords INTEGER,
                total_dovish_keywords INTEGER,
                key_phrases JSON,
                prev_meeting_score DECIMAL(4,3),
                score_delta DECIMAL(4,3),
                reasoning TEXT,
                model_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """).result()
        # Get table counts
        tables = [
            "fomc_transcripts",
            "transcript_sections",
            "fomc_meeting_summaries",
            "transcript_topics",
            "transcript_search_index",
            "member_voting_history",
            "fomc_meetings_enhanced",
            "fomc_sentiment_scores",
        ]

        table_info = {}
        for table in tables:
            try:
                result = bq.fetchone(
                    f"SELECT COUNT(*) as count FROM {table}")
                table_info[table] = result[0] if result else 0
            except Exception:
                table_info[table] = 0

        context.log.info(f"Schema initialized. Table row counts: {table_info}")

        return dg.MaterializeResult(
            metadata={
                "tables_created": len(tables),
                "table_names": ", ".join(tables),
                **{f"{table}_rows": count for table, count in table_info.items()},
            }
        )
    finally:
        if conn:
            conn.close()


@dg.asset(
    group_name="macro_ingestion",
    kinds={"web_scraping", "gcs"},
    partitions_def=fomc_transcript_years_partition,
    deps=[fomc_transcript_schema],
    description="Download FOMC meeting transcript PDFs and store in GCS",
)
def fomc_transcripts_raw(
    context: dg.AssetExecutionContext,
    fed: FederalReserveResource,
    gcs: GCSResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Download FOMC transcript PDFs for a given year and store in GCS.

    Transcripts are released 5 years after the meeting date.

    Storage strategy:
    - PDFs stored in GCS: fomc_transcripts/{year}/{date}.pdf
    - Metadata tracked in DuckDB: fomc_transcripts table
    """
    year = int(context.partition_key)
    now = datetime.now(timezone.utc)

    context.log.info(f"Starting FOMC transcript ingestion for year {year}")

    transcript_links = fed.get_transcript_links(year, context)
    context.log.info(f"Found {len(transcript_links)} transcript PDFs for {year}")

    # Load existing transcript rows so re-runs preserve previously extracted text
    conn = None
    existing_by_id: dict[str, dict] = {}
    try:
        conn = bq.get_connection()
        rows = bq.fetchall(
            "SELECT transcript_id, full_text, word_count, page_count "
            "FROM fomc_transcripts WHERE transcript_id IS NOT NULL")
        for row in rows:
            existing_by_id[row[0]] = {
                "full_text": row[1],
                "word_count": row[2],
                "page_count": row[3],
            }
    except Exception:
        pass
    finally:
        if conn:
            conn.close()

    metadata_records = []

    for entry in transcript_links:
        meeting_date_raw = entry["meeting_date"]
        pdf_url = entry["pdf_url"]
        meeting_date = fed._format_date(meeting_date_raw)

        gcs_path = f"fomc_transcripts/{year}/{meeting_date}.pdf"

        # Skip if already downloaded
        if gcs.file_exists(gcs_path, context):
            context.log.info(
                f"Transcript already exists at {gcs_path}, skipping download"
            )
        else:
            try:
                pdf_bytes = fed.download_transcript_pdf(pdf_url, context)
                gcs.upload_bytes(gcs_path, pdf_bytes, "application/pdf", context)
                context.log.info(f"Uploaded transcript PDF to {gcs_path}")
            except Exception as exc:
                context.log.warning(
                    f"Failed to download transcript for {meeting_date}: {exc}"
                )
                continue

        transcript_id = generate_id("transcript", str(year), meeting_date)

        # Preserve previously extracted text/counts on re-run
        existing = existing_by_id.get(transcript_id, {})
        metadata_records.append(
            {
                "transcript_id": transcript_id,
                "meeting_date": meeting_date,
                "full_text": existing.get("full_text"),
                "word_count": existing.get("word_count"),
                "page_count": existing.get("page_count"),
                "source_url": pdf_url,
                "source_pdf_path": gcs_path,
                "processed_date": now,
                "created_at": now,
            }
        )

    if not metadata_records:
        return dg.MaterializeResult(
            metadata={
                "num_transcripts": 0,
                "year": year,
                "status": "no_transcripts_found",
            }
        )

    df = pl.DataFrame(metadata_records)
    bq.upsert_data("fomc_transcripts", df, ["transcript_id"], context=context)

    return dg.MaterializeResult(
        metadata={
            "num_transcripts": len(metadata_records),
            "year": year,
            "status": "complete",
            "meeting_dates": [r["meeting_date"] for r in metadata_records],
        }
    )


def generate_id(prefix: str, *components: str) -> str:
    """Generate a unique ID by hashing components."""
    hash_input = "_".join(str(c) for c in components)
    hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
    return f"{prefix}_{hash_digest}"


@dg.asset(
    group_name="transformation",
    kinds={"pdf", "processing"},
    deps=[fomc_transcripts_raw],
    description="Extract text from transcript PDFs and parse into sections with speaker attribution",
)
def process_fomc_transcripts(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
    gcs: GCSResource,
    pdf: PDFResource,
) -> dg.MaterializeResult:
    """
    Extract text from transcript PDFs and parse into speaker sections.

    Steps:
    1. Find transcripts without extracted text (full_text IS NULL)
    2. Download PDF from GCS and extract text via pdfplumber
    3. Parse text into speaker-attributed sections
    4. Update fomc_transcripts with full_text, word_count, page_count
    5. Insert parsed sections into transcript_sections table
    """
    context.log.info("Processing FOMC transcripts: extracting text from PDFs")

    conn = None
    try:
        conn = bq.get_connection()

        # Find transcripts that haven't been text-extracted yet
        unprocessed = bq.fetchall("""
            SELECT transcript_id, meeting_date, source_pdf_path
            FROM fomc_transcripts
            WHERE full_text IS NULL AND source_pdf_path IS NOT NULL
        """)

        if not unprocessed:
            context.log.info("No unprocessed transcripts found")
            return dg.MaterializeResult(
                metadata={"sections_processed": 0, "transcripts_processed": 0}
            )

        context.log.info(f"Found {len(unprocessed)} transcripts to process")

        total_sections = 0

        for transcript_id, meeting_date, gcs_path in unprocessed:
            try:
                # Download PDF from GCS
                blob = gcs._bucket.blob(gcs_path)
                if not blob.exists():
                    context.log.warning(f"PDF not found in GCS: {gcs_path}")
                    continue

                pdf_bytes = blob.download_as_bytes()
                context.log.info(
                    f"Processing transcript for {meeting_date} ({len(pdf_bytes)} bytes)"
                )

                # Extract text and metadata
                full_text = pdf.extract_text(pdf_bytes)
                meta = pdf.get_metadata(pdf_bytes)
                word_count = len(full_text.split())
                page_count = meta["page_count"]

                # Update fomc_transcripts with extracted text
                conn.query(
                    """
                    UPDATE fomc_transcripts
                    SET full_text = $1, word_count = $2, page_count = $3,
                        processed_date = CURRENT_TIMESTAMP
                    WHERE transcript_id = $4
                    """,
                    [full_text, word_count, page_count, transcript_id],
                ).result()

                # Parse into speaker sections
                sections = _parse_transcript_sections(full_text, transcript_id)
                for section in sections:
                    conn.query(
                        """
                        INSERT OR REPLACE INTO transcript_sections
                        (section_id, transcript_id, section_order, section_type,
                         speaker, speaker_role, content)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        [
                            section["section_id"],
                            transcript_id,
                            section["section_order"],
                            section["section_type"],
                            section["speaker"],
                            section.get("speaker_role"),
                            section["content"],
                        ],
                    ).result()

                total_sections += len(sections)
                context.log.info(
                    f"Processed {meeting_date}: {word_count} words, "
                    f"{page_count} pages, {len(sections)} sections"
                )

            except Exception as exc:
                context.log.warning(
                    f"Failed to process transcript {transcript_id}: {exc}"
                )
                continue
        context.log.info(
            f"Processed {len(unprocessed)} transcripts, {total_sections} total sections"
        )

        return dg.MaterializeResult(
            metadata={
                "transcripts_processed": len(unprocessed),
                "sections_processed": total_sections,
            }
        )
    finally:
        if conn:
            conn.close()


@dg.asset(
    group_name="transformation",
    kinds={"ai", "llm"},
    deps=[process_fomc_transcripts],
    description="Generate AI summaries of FOMC meetings using Claude",
)
def generate_transcript_summaries(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Generate AI summaries using Claude API.

    Summary types:
    - executive: 2-3 paragraph overview
    - key_decisions: Bullet points of main outcomes
    - economic_assessment: Economic conditions discussed
    - notable_quotes: Significant statements
    """
    context.log.info("Generating transcript summaries")

    # Placeholder for AI summary generation
    summaries_generated = 0

    return dg.MaterializeResult(
        metadata={
            "summaries_generated": summaries_generated,
            "status": "placeholder",
        }
    )


@dg.asset(
    group_name="transformation",
    kinds={"ai", "nlp"},
    deps=[process_fomc_transcripts],
    description="Extract topics and sentiment from transcripts using LLM",
)
def extract_transcript_topics(
    context: dg.AssetExecutionContext,
    fed_sentiment: FedSentimentResource,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """Extract economic topics from transcript sections using LLM.

    For each discussion section, extracts topics (inflation, employment, growth, etc.)
    with relevance scores and hawkish/dovish sentiment labels.
    Writes to the existing transcript_topics table.
    """
    import json as _json

    context.log.info("Extracting topics from transcripts via LLM")
    fed_sentiment.setup_for_execution(context)

    # Find sections without extracted topics
    query = """
        SELECT
            ts.section_id,
            ts.transcript_id,
            ts.speaker,
            ts.content
        FROM transcript_sections ts
        LEFT JOIN transcript_topics tt ON ts.section_id = tt.section_id
        WHERE tt.topic_id IS NULL
            AND ts.content IS NOT NULL
            AND LENGTH(ts.content) > 100
            AND ts.section_type = 'discussion'
        ORDER BY ts.transcript_id, ts.section_order
    """

    sections_df = bq.execute_query(query, read_only=True)

    if sections_df.is_empty():
        context.log.info("No sections need topic extraction")
        return dg.MaterializeResult(metadata={"topics_extracted": 0})

    context.log.info(f"Extracting topics from {len(sections_df)} sections")

    topic_records: list[dict] = []
    errors = 0

    for i, row in enumerate(sections_df.iter_rows(named=True)):
        content = row["content"][:12_000]
        speaker = row["speaker"] or "UNKNOWN"

        try:
            result = fed_sentiment.extract_topics(content=content, speaker=speaker)
            topics = _json.loads(result.topics)

            for topic_entry in topics:
                topic_id = generate_id(
                    "topic",
                    row["section_id"],
                    topic_entry.get("topic", ""),
                    topic_entry.get("subtopic", ""),
                )
                topic_records.append(
                    {
                        "topic_id": topic_id,
                        "transcript_id": row["transcript_id"],
                        "section_id": row["section_id"],
                        "topic": topic_entry.get("topic"),
                        "subtopic": topic_entry.get("subtopic"),
                        "relevance_score": topic_entry.get("relevance_score"),
                        "mentioned_by": [speaker],
                        "sentiment": topic_entry.get("sentiment"),
                    }
                )
        except Exception as exc:
            errors += 1
            context.log.warning(
                f"Topic extraction failed for {row['section_id']}: {exc}"
            )
            continue

        if (i + 1) % 20 == 0:
            context.log.info(f"Processed {i + 1}/{len(sections_df)} sections")

    if topic_records:
        import polars as _pl

        df = _pl.DataFrame(topic_records)
        bq.upsert_data("transcript_topics", df, ["topic_id"], context=context)

    context.log.info(
        f"Extracted {len(topic_records)} topics from {len(sections_df)} sections "
        f"({errors} errors)"
    )

    return dg.MaterializeResult(
        metadata={
            "topics_extracted": len(topic_records),
            "sections_processed": len(sections_df),
            "errors": errors,
        }
    )


@dg.asset(
    group_name="transformation",
    kinds={"search", "indexing"},
    deps=[process_fomc_transcripts],
    description="Build searchable index for transcripts",
)
def build_transcript_search_index(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Create searchable index for full-text search.

    Includes:
    - Cleaned text for search
    - Metadata tagging
    - Optional vector embeddings
    """
    context.log.info("Building transcript search index")

    # Placeholder
    records_indexed = 0

    return dg.MaterializeResult(
        metadata={
            "records_indexed": records_indexed,
            "status": "placeholder",
        }
    )


@dg.asset(
    group_name="analysis",
    kinds={"ai", "analysis"},
    deps=[extract_transcript_topics, fomc_transcripts_raw],
    description="Analyze member voting patterns and correlate with transcript content",
)
def analyze_member_voting_patterns(
    context: dg.AssetExecutionContext,
    bq: BigQueryWarehouseResource,
) -> dg.MaterializeResult:
    """
    Correlate voting with transcript content.

    Analysis:
    - Track member stances over time
    - Predict voting based on statements
    - Identify hawkish/dovish trends
    """
    context.log.info("Analyzing member voting patterns")

    # Placeholder
    members_analyzed = 0

    return dg.MaterializeResult(
        metadata={
            "members_analyzed": members_analyzed,
            "status": "placeholder",
        }
    )


def _parse_transcript_sections(full_text: str, transcript_id: str) -> list[dict]:
    """Parse transcript text into speaker-attributed sections.

    FOMC transcripts follow a pattern where speakers are identified by
    all-caps names (e.g., "CHAIRMAN BERNANKE.", "MR. DUDLEY.") followed
    by their remarks.
    """
    # Match speaker lines: "CHAIRMAN POWELL.", "CHAIR YELLEN.", "MR. CLARIDA.", etc.
    # CHAIR/CHAIRMAN/VICE CHAIR(MAN) have no period after title; MR./MS./MRS./DR. do.
    speaker_pattern = re.compile(
        r"^((?:VICE CHAIR(?:MAN)?|CHAIR(?:MAN)?|(?:MR|MS|MRS|DR)\.)\s+[A-Z]+\.)",
        re.MULTILINE,
    )

    splits = speaker_pattern.split(full_text)
    sections = []
    order = 0

    # First chunk is preamble (before any speaker)
    if splits and splits[0].strip():
        order += 1
        sections.append(
            {
                "section_id": generate_id("section", transcript_id, "preamble"),
                "section_order": order,
                "section_type": "preamble",
                "speaker": None,
                "speaker_role": None,
                "content": splits[0].strip(),
            }
        )

    # Remaining chunks alternate: speaker_name, content, speaker_name, content, ...
    for i in range(1, len(splits) - 1, 2):
        speaker_raw = splits[i].strip().rstrip(".")
        content = splits[i + 1].strip() if i + 1 < len(splits) else ""
        if not content:
            continue

        order += 1
        speaker_role = None
        if "VICE CHAIR" in speaker_raw:
            speaker_role = "Vice Chair"
        elif "CHAIR" in speaker_raw:
            speaker_role = "Chair"

        sections.append(
            {
                "section_id": generate_id("section", transcript_id, str(order)),
                "section_order": order,
                "section_type": "discussion",
                "speaker": speaker_raw,
                "speaker_role": speaker_role,
                "content": content,
            }
        )

    return sections


# --- Jobs and Schedules ---

fomc_transcript_ingestion_job = dg.define_asset_job(
    name="fomc_transcript_ingestion_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 3600},
    selection=dg.AssetSelection.assets("fomc_transcripts_raw"),
    description="FOMC transcript PDF ingestion job",
)

fomc_transcript_processing_job = dg.define_asset_job(
    name="fomc_transcript_processing_job",
    tags={"dagster/priority": "5", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("process_fomc_transcripts"),
    description="Extract text from transcript PDFs and parse into sections",
)


defs = dg.Definitions(
    assets=[
        fomc_transcript_schema,
        fomc_transcripts_raw,
        process_fomc_transcripts,
        generate_transcript_summaries,
        extract_transcript_topics,
        build_transcript_search_index,
        analyze_member_voting_patterns,
    ],
    jobs=[
        fomc_transcript_ingestion_job,
        fomc_transcript_processing_job,
    ],
)
