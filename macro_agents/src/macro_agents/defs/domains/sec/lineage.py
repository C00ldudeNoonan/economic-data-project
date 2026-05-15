"""Metaxy FeatureSpecs for the SEC filing pipeline.

Phase 1 (issue #46) declared `sec/raw_html` and `sec/extracted_text`.
Phase 2 adds `sec/bi_signals` (expansion from extracted_text — each filing
produces many term rows).
Remaining phases will add `sec/embeddings`, `sec/fts_index`, `sec/search_index`.

Features subclass `BaseFeature` (rather than being bare `FeatureSpec`
instances) so they register in Metaxy's feature graph at import time. The
`entrypoints` key in `metaxy.toml` points at this module.
"""

import metaxy as mx

# Load metaxy.toml (auto-discovered via parent-dir search) and merge env
# overrides. Must run before BaseFeature subclasses below are evaluated so the
# global config is in place when features register.
mx.MetaxyConfig.set(mx.MetaxyConfig.load())


class RawHtml(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="sec/raw_html",
        id_columns=("filing_id",),
    ),
):
    pass


class ExtractedText(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="sec/extracted_text",
        id_columns=("filing_id",),
        deps=[
            mx.FeatureDep(
                feature="sec/raw_html",
                lineage=mx.LineageRelationship.identity(),
            ),
        ],
    ),
):
    pass


class BiSignals(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="sec/bi_signals",
        id_columns=("term_id",),
        deps=[
            mx.FeatureDep(
                feature="sec/extracted_text",
                lineage=mx.LineageRelationship.expansion(on=["filing_id"]),
            ),
        ],
    ),
):
    pass
