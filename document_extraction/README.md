# document_extraction — dbt-ml pilot (issue #92)

A [dbt-ml](https://github.com/C00ldudeNoonan/dbt-ml) project that materializes
raw SEC filing HTML from GCS into analytical BigQuery tables. Phase 1 of the
dbt-ml integration: declarative extraction replacing imperative Python, with
incremental builds keyed on GCS object generations and headings/tables
preserved as JSON structure with char offsets.

## DAG

```
sec_filing_html (gs://econ-project-general-storage/sec_filings, *.htm)
    └── sec_document_registry   one row per filing envelope (json backend:
            │                   the .htm objects are JSON envelopes, not raw HTML)
            └── sec_document_text    body text + domain keys (symbol, form_type,
                    │                filing_date) via transforms/sec_envelope_text.py
                    └── sec_document_chunks   one row per 800-char chunk (RAG grain)

fomc_transcript_pdfs (gs://econ-project-general-storage/fomc_transcripts, *.pdf)
    └── fomc_document_registry  one row per transcript, per-page char offsets
            └── fomc_document_chunks  one row per 800-char chunk (RAG grain)
```

Tables land in `economics_documents_dev` (dev) / `economics_documents` (prod)
on BigQuery. Domain parsing (symbol, form_type, year from `source_path`)
happens downstream in dbt SQL, not here — the extraction layer stays generic.

## Run it

This project keeps its own uv-managed environment (dbt-ml needs
Python >= 3.12; macro_agents pins 3.11):

```bash
cd document_extraction
uv sync
uv run dbt-ml build            # dev target: economics_documents_dev
uv run dbt-ml build --target prod
```

Auth is Application Default Credentials for both GCS listing and BigQuery.
Set `GOOGLE_CLOUD_PROJECT=econ-data-project-478800` when using user ADC —
the GCS client cannot infer a project from user credentials. Incremental
runs skip unchanged GCS objects without downloading them.

## Orchestration

Dagster runs this project via `dbt-ml run --json` (see
`macro_agents/src/macro_agents/defs/transformation/dbt_ml.py`) and
`dbt-ml emit-dbt-sources --dagster-meta` hands the tables to `dbt_project`
as sources with pinned asset keys.
