"""Dagster assets for Fed communications sentiment analysis.

Three assets:
- score_fed_sentiment_dictionary: Fast, deterministic keyword-based scoring
- score_fed_sentiment_llm: LLM-based nuanced scoring via DSPy
- fed_sentiment_rate_correlation: Joins sentiment with actual rate decisions
"""

import hashlib
import json
from datetime import datetime, timezone

import dagster as dg
import polars as pl

from macro_agents.defs.analysis.fed_sentiment.lexicon import (
    extract_key_phrases,
    score_text,
)
from macro_agents.defs.analysis.fed_sentiment.resource import FedSentimentResource
from macro_agents.defs.resources.motherduck import MotherDuckResource


VALID_SCORING_METHODS = {"dictionary", "llm"}


def _generate_id(prefix: str, *components: str) -> str:
    """Generate a deterministic ID from components."""
    hash_input = "_".join(str(c) for c in components)
    hash_digest = hashlib.sha256(hash_input.encode()).hexdigest()[:12]
    return f"{prefix}_{hash_digest}"


# ---------------------------------------------------------------------------
# Asset 1: Dictionary-based sentiment scoring
# ---------------------------------------------------------------------------


@dg.asset(
    group_name="fed_sentiment",
    kinds={"nlp", "duckdb"},
    deps=[dg.AssetKey(["process_fomc_transcripts"])],
    description="Dictionary-based hawkish/dovish scoring of FOMC transcript sections",
)
def score_fed_sentiment_dictionary(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Score transcript sections using monetary policy keyword lexicon.

    Processes all transcript sections that haven't been dictionary-scored yet.
    Produces both section-level and meeting-level aggregate scores.
    """
    context.log.info("Starting dictionary-based Fed sentiment scoring")

    # Find sections not yet scored by dictionary method
    query = """
        SELECT
            ts.section_id,
            ts.transcript_id,
            ts.speaker,
            ts.speaker_role,
            ts.content,
            ft.meeting_date
        FROM transcript_sections ts
        JOIN fomc_transcripts ft ON ts.transcript_id = ft.transcript_id
        LEFT JOIN fomc_sentiment_scores fss
            ON ts.section_id = fss.section_id
            AND fss.scoring_method = 'dictionary'
        WHERE fss.score_id IS NULL
            AND ts.content IS NOT NULL
            AND LENGTH(ts.content) > 50
        ORDER BY ft.meeting_date, ts.section_order
    """

    sections_df = md.execute_query(query, read_only=True)

    if sections_df.is_empty():
        context.log.info("No unscored sections found")
        return dg.MaterializeResult(
            metadata={"sections_scored": 0, "meetings_scored": 0}
        )

    context.log.info(f"Scoring {len(sections_df)} sections")

    now = datetime.now(timezone.utc)
    records: list[dict] = []

    for row in sections_df.iter_rows(named=True):
        scores = score_text(row["content"])
        phrases = extract_key_phrases(row["content"], max_phrases=10)

        meeting_date = row["meeting_date"]
        if hasattr(meeting_date, "strftime"):
            meeting_date = meeting_date.strftime("%Y-%m-%d")

        records.append(
            {
                "score_id": _generate_id("dict", row["section_id"], "dictionary"),
                "transcript_id": row["transcript_id"],
                "section_id": row["section_id"],
                "meeting_date": meeting_date,
                "speaker": row["speaker"],
                "scoring_method": "dictionary",
                "hawkish_score": scores["hawkish_score"],
                "dovish_score": scores["dovish_score"],
                "net_sentiment_score": scores["net_sentiment_score"],
                "confidence": None,
                "keyword_counts": json.dumps(scores["keyword_counts"]),
                "total_hawkish_keywords": scores["total_hawkish_keywords"],
                "total_dovish_keywords": scores["total_dovish_keywords"],
                "key_phrases": json.dumps(phrases),
                "prev_meeting_score": None,
                "score_delta": None,
                "reasoning": None,
                "model_name": None,
                "created_at": now,
            }
        )

    # Upsert section-level scores first
    df = pl.DataFrame(records)
    md.upsert_data("fomc_sentiment_scores", df, ["score_id"], context=context)

    # Rebuild meeting-level aggregates from ALL sections in the DB
    # (not just newly scored ones) to handle partial re-runs correctly
    affected_meetings = {r["meeting_date"] for r in records}
    meeting_aggregates = _rebuild_meeting_aggregates(
        md, affected_meetings, "dictionary"
    )
    _compute_score_deltas(meeting_aggregates, md)

    if meeting_aggregates:
        agg_df = pl.DataFrame(meeting_aggregates)
        md.upsert_data("fomc_sentiment_scores", agg_df, ["score_id"], context=context)

    context.log.info(
        f"Scored {len(sections_df)} sections across {len(affected_meetings)} meetings"
    )

    return dg.MaterializeResult(
        metadata={
            "sections_scored": len(sections_df),
            "meetings_scored": len(affected_meetings),
            "meeting_dates": sorted(affected_meetings),
        }
    )


# ---------------------------------------------------------------------------
# Asset 2: LLM-based sentiment scoring
# ---------------------------------------------------------------------------

MAX_SECTION_CHARS = 12_000  # ~3K tokens — safe for most models


@dg.asset(
    group_name="fed_sentiment",
    kinds={"ai", "duckdb"},
    deps=[dg.AssetKey(["process_fomc_transcripts"])],
    description="LLM-based hawkish/dovish scoring of FOMC transcript sections",
)
def score_fed_sentiment_llm(
    context: dg.AssetExecutionContext,
    fed_sentiment: FedSentimentResource,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Score transcript sections using LLM-based sentiment analysis.

    Processes sections incrementally — only scores sections not yet analyzed by LLM.
    """
    context.log.info("Starting LLM-based Fed sentiment scoring")
    fed_sentiment.setup_for_execution(context)

    query = """
        SELECT
            ts.section_id,
            ts.transcript_id,
            ts.speaker,
            ts.content,
            ft.meeting_date
        FROM transcript_sections ts
        JOIN fomc_transcripts ft ON ts.transcript_id = ft.transcript_id
        LEFT JOIN fomc_sentiment_scores fss
            ON ts.section_id = fss.section_id
            AND fss.scoring_method = 'llm'
        WHERE fss.score_id IS NULL
            AND ts.content IS NOT NULL
            AND LENGTH(ts.content) > 50
            AND ts.section_type = 'discussion'
        ORDER BY ft.meeting_date, ts.section_order
    """

    sections_df = md.execute_query(query, read_only=True)

    if sections_df.is_empty():
        context.log.info("No unscored sections found")
        return dg.MaterializeResult(
            metadata={"sections_scored": 0, "meetings_scored": 0}
        )

    context.log.info(f"LLM scoring {len(sections_df)} sections")

    now = datetime.now(timezone.utc)
    records: list[dict] = []
    errors = 0

    for i, row in enumerate(sections_df.iter_rows(named=True)):
        content = row["content"][:MAX_SECTION_CHARS]
        speaker = row["speaker"] or "UNKNOWN"

        meeting_date = row["meeting_date"]
        if hasattr(meeting_date, "strftime"):
            meeting_date = meeting_date.strftime("%Y-%m-%d")

        try:
            result = fed_sentiment.score_sentiment(
                speaker=speaker,
                content=content,
                meeting_date=meeting_date,
            )

            hawkish = _parse_float(result.hawkish_score, 0.0)
            dovish = _parse_float(result.dovish_score, 0.0)
            net = hawkish - dovish

            try:
                phrases = json.loads(result.key_phrases)
            except (json.JSONDecodeError, TypeError):
                phrases = []

            records.append(
                {
                    "score_id": _generate_id("llm", row["section_id"], "llm"),
                    "transcript_id": row["transcript_id"],
                    "section_id": row["section_id"],
                    "meeting_date": meeting_date,
                    "speaker": row["speaker"],
                    "scoring_method": "llm",
                    "hawkish_score": round(hawkish, 3),
                    "dovish_score": round(dovish, 3),
                    "net_sentiment_score": round(net, 3),
                    "confidence": None,
                    "keyword_counts": None,
                    "total_hawkish_keywords": None,
                    "total_dovish_keywords": None,
                    "key_phrases": json.dumps(phrases),
                    "prev_meeting_score": None,
                    "score_delta": None,
                    "reasoning": getattr(result, "reasoning", None),
                    "model_name": fed_sentiment._model_name,
                    "created_at": now,
                }
            )
        except Exception as exc:
            errors += 1
            context.log.warning(
                f"LLM scoring failed for section {row['section_id']}: {exc}"
            )
            continue

        if (i + 1) % 20 == 0:
            context.log.info(f"Scored {i + 1}/{len(sections_df)} sections")

    if records:
        # Upsert section-level scores first
        df = pl.DataFrame(records)
        md.upsert_data("fomc_sentiment_scores", df, ["score_id"], context=context)

        # Rebuild meeting-level aggregates from ALL sections in the DB
        affected_meetings = {r["meeting_date"] for r in records}
        meeting_aggregates = _rebuild_meeting_aggregates(md, affected_meetings, "llm")
        _compute_score_deltas(meeting_aggregates, md)

        if meeting_aggregates:
            agg_df = pl.DataFrame(meeting_aggregates)
            md.upsert_data(
                "fomc_sentiment_scores", agg_df, ["score_id"], context=context
            )

    meeting_dates = {r["meeting_date"] for r in records}
    context.log.info(
        f"LLM scored {len(records)} sections, {errors} errors, "
        f"{len(meeting_dates)} meetings"
    )

    return dg.MaterializeResult(
        metadata={
            "sections_scored": len(records),
            "errors": errors,
            "meetings_scored": len(meeting_dates),
        }
    )


# ---------------------------------------------------------------------------
# Asset 3: Sentiment vs. rate decision correlation
# ---------------------------------------------------------------------------


@dg.asset(
    group_name="fed_sentiment",
    kinds={"analysis", "duckdb"},
    deps=[
        dg.AssetKey(["score_fed_sentiment_dictionary"]),
        dg.AssetKey(["fomc_transcript_schema"]),
    ],
    description="Correlate Fed sentiment scores with actual rate decisions",
)
def fed_sentiment_rate_correlation(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """Join meeting-level sentiment with rate decisions from fomc_meetings_enhanced.

    Stores a denormalized view useful for trend analysis and correlation queries.
    """
    context.log.info("Computing sentiment vs. rate decision correlation")

    query = """
        SELECT
            fss.meeting_date,
            fss.scoring_method,
            fss.hawkish_score,
            fss.dovish_score,
            fss.net_sentiment_score,
            fss.score_delta,
            fss.total_hawkish_keywords,
            fss.total_dovish_keywords,
            fme.action,
            fme.rate_change_bps,
            fme.target_rate_lower,
            fme.target_rate_upper
        FROM fomc_sentiment_scores fss
        LEFT JOIN fomc_meetings_enhanced fme
            ON fss.meeting_date = fme.meeting_date
        WHERE fss.section_id IS NULL
        ORDER BY fss.meeting_date
    """

    result_df = md.execute_query(query, read_only=True)

    if result_df.is_empty():
        context.log.info("No meeting-level sentiment scores found")
        return dg.MaterializeResult(metadata={"rows": 0})

    context.log.info(f"Correlation data: {len(result_df)} meeting-level rows")

    return dg.MaterializeResult(
        metadata={
            "rows": len(result_df),
            "meetings_with_rate_data": int(
                result_df.filter(pl.col("rate_change_bps").is_not_null()).height
            ),
            "scoring_methods": result_df["scoring_method"].unique().to_list(),
        }
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rebuild_meeting_aggregates(
    md: MotherDuckResource,
    meeting_dates: set[str],
    scoring_method: str,
) -> list[dict]:
    """Rebuild meeting-level aggregates from ALL section rows in the database.

    Queries the full set of section-level scores for each affected meeting,
    ensuring partial re-runs don't corrupt aggregates.
    """
    if not meeting_dates:
        return []

    if scoring_method not in VALID_SCORING_METHODS:
        raise ValueError(f"Invalid scoring_method: {scoring_method!r}")

    date_list = ", ".join(f"'{d}'" for d in sorted(meeting_dates))
    query = f"""
        SELECT
            meeting_date,
            transcript_id,
            model_name,
            AVG(hawkish_score) AS avg_hawkish,
            AVG(dovish_score) AS avg_dovish,
            AVG(net_sentiment_score) AS avg_net,
            SUM(total_hawkish_keywords) AS total_hawk_kw,
            SUM(total_dovish_keywords) AS total_dove_kw,
            COUNT(*) AS section_count
        FROM fomc_sentiment_scores
        WHERE section_id IS NOT NULL
            AND scoring_method = '{scoring_method}'
            AND meeting_date IN ({date_list})
        GROUP BY meeting_date, transcript_id, model_name
        ORDER BY meeting_date
    """

    agg_df = md.execute_query(query, read_only=True)
    if agg_df.is_empty():
        return []

    now = datetime.now(timezone.utc)
    aggregates: list[dict] = []

    for row in agg_df.iter_rows(named=True):
        meeting_date = row["meeting_date"]
        if hasattr(meeting_date, "strftime"):
            meeting_date = meeting_date.strftime("%Y-%m-%d")

        aggregates.append(
            {
                "score_id": _generate_id("agg", meeting_date, scoring_method),
                "transcript_id": row["transcript_id"],
                "section_id": None,
                "meeting_date": meeting_date,
                "speaker": None,
                "scoring_method": scoring_method,
                "hawkish_score": round(row["avg_hawkish"] or 0, 3),
                "dovish_score": round(row["avg_dovish"] or 0, 3),
                "net_sentiment_score": round(row["avg_net"] or 0, 3),
                "confidence": None,
                "keyword_counts": None,
                "total_hawkish_keywords": int(row["total_hawk_kw"] or 0),
                "total_dovish_keywords": int(row["total_dove_kw"] or 0),
                "key_phrases": None,
                "prev_meeting_score": None,
                "score_delta": None,
                "reasoning": None,
                "model_name": row.get("model_name"),
                "created_at": now,
            }
        )

    return aggregates


def _compute_score_deltas(records: list[dict], md: MotherDuckResource) -> None:
    """Fill in prev_meeting_score and score_delta for meeting-level rows.

    Queries existing meeting-level scores to find the previous meeting's score.
    Mutates records in-place.
    """
    meeting_rows = [r for r in records if r["section_id"] is None]
    if not meeting_rows:
        return

    try:
        existing = md.execute_query(
            """
            SELECT meeting_date, scoring_method, net_sentiment_score
            FROM fomc_sentiment_scores
            WHERE section_id IS NULL
            ORDER BY meeting_date
            """,
            read_only=True,
        )
    except Exception:
        return

    prev_scores: dict[tuple[str, str], float] = {}
    for row in existing.iter_rows(named=True):
        md_str = row["meeting_date"]
        if hasattr(md_str, "strftime"):
            md_str = md_str.strftime("%Y-%m-%d")
        key = (md_str, row["scoring_method"])
        prev_scores[key] = row["net_sentiment_score"]

    # Also include the new meeting rows for chaining
    for r in meeting_rows:
        key = (r["meeting_date"], r["scoring_method"])
        prev_scores[key] = r["net_sentiment_score"]

    # Sort all meeting dates per method
    from collections import defaultdict

    by_method: dict[str, list[str]] = defaultdict(list)
    for (md_str, method), _ in prev_scores.items():
        if md_str not in by_method[method]:
            by_method[method].append(md_str)
    for method in by_method:
        by_method[method].sort()

    for r in meeting_rows:
        method = r["scoring_method"]
        dates = by_method.get(method, [])
        idx = dates.index(r["meeting_date"]) if r["meeting_date"] in dates else -1
        if idx > 0:
            prev_date = dates[idx - 1]
            prev = prev_scores.get((prev_date, method))
            if prev is not None:
                r["prev_meeting_score"] = round(prev, 3)
                r["score_delta"] = round(r["net_sentiment_score"] - prev, 3)


def _parse_float(value, default: float = 0.0) -> float:
    """Safely parse a string to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default
