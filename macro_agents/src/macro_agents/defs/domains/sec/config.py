"""Centralized configuration constants for the SEC filing pipeline.

All magic numbers and tunable parameters live here so operators can find
and adjust them in one place. Each constant includes a comment explaining
*why* that value was chosen.
"""

# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------

# Documents, text extraction, BI signals, and markdown all use 25.
# Balances throughput against memory (large filings can be 100+ MB)
# and SEC EDGAR rate limits (~10 req/sec).
BATCH_SIZE_STANDARD = 25

# Search/embedding uses a smaller batch because each filing produces
# multiple sections, each requiring an embedding API call.
BATCH_SIZE_EMBEDDINGS = 10

# ---------------------------------------------------------------------------
# BI signal extraction
# ---------------------------------------------------------------------------

# Characters of surrounding text captured with each extracted signal.
BI_CONTEXT_WINDOW = 250

# Max characters stored for a signal term. Prevents oversized DB rows.
SIGNAL_TERM_MAX_LENGTH = 200

# Max characters stored for signal context text.
SIGNAL_CONTEXT_MAX_LENGTH = 1000

# ---------------------------------------------------------------------------
# Error reporting
# ---------------------------------------------------------------------------

# Number of individual error messages surfaced in Dagster asset metadata.
# Keeps the UI readable without hiding all errors; full logs have everything.
MAX_ERROR_DETAILS = 10

# ---------------------------------------------------------------------------
# Search chunking
# ---------------------------------------------------------------------------

# Target words per chunk for vector embeddings. Balances semantic coherence
# with embedding model context limits (768-dim model).
TARGET_CHUNK_WORDS = 500

# Sentence overlap between adjacent chunks to preserve cross-chunk context.
OVERLAP_SENTENCES = 2

# ---------------------------------------------------------------------------
# Sensor intervals
# ---------------------------------------------------------------------------

# How often (seconds) to check for unprocessed filings. 1 hour is frequent
# enough for prompt processing without excessive DB queries.
UNPROCESSED_FILINGS_CHECK_SECONDS = 3600

# How often (seconds) to check for new S&P 500 companies. Index membership
# changes are infrequent, so 24 hours is sufficient.
NEW_COMPANIES_CHECK_SECONDS = 86400

# ---------------------------------------------------------------------------
# Job configuration
# ---------------------------------------------------------------------------

# Max runtime (seconds) for metadata ingestion. 1 hour covers ~500 companies
# with SEC EDGAR rate-limited API calls.
INGESTION_JOB_MAX_RUNTIME = 3600

# Max runtime (seconds) for document processing. 2 hours covers GCS uploads,
# text extraction, and BI signal generation for a batch of filings.
PROCESSING_JOB_MAX_RUNTIME = 7200

# Higher number = higher priority in the Dagster run queue.
# Ingestion must land before processing can start.
INGESTION_JOB_PRIORITY = 10
PROCESSING_JOB_PRIORITY = 8

# ---------------------------------------------------------------------------
# Metadata ingestion
# ---------------------------------------------------------------------------

# Years of filing history to fetch for companies with no existing filings.
YEARS_BACK_FULL_BACKFILL = 20

# Version string stamped into GCS metadata envelopes. Bump when the
# pipeline output format changes in a breaking way.
PIPELINE_VERSION = "2"

# ---------------------------------------------------------------------------
# Progress logging
# ---------------------------------------------------------------------------

# Log progress every N companies during metadata ingestion.
# With ~500 S&P 500 companies this gives ~10 log lines per run.
METADATA_PROGRESS_LOG_INTERVAL = 50
