"""Metaxy FeatureSpecs for the SEC filing pipeline.

Phase 1 (issue #46) declares only the first two stages — `sec/raw_html` as a
source feature, and `sec/extracted_text` as identity lineage downstream of it.
Subsequent phases will add `sec/embeddings`, `sec/fts_index`, `sec/bi_signals`,
and `sec/search_index`.
"""

from metaxy import FeatureDep, FeatureSpec, LineageRelationship

raw_html = FeatureSpec(
    key="sec/raw_html",
    id_columns=("filing_id",),
)

extracted_text = FeatureSpec(
    key="sec/extracted_text",
    id_columns=("filing_id",),
    deps=[
        FeatureDep(
            feature="sec/raw_html",
            lineage=LineageRelationship.identity(),
        ),
    ],
)
