"""Metaxy FeatureSpecs for the SEC filing pipeline.

Covers every stage of the SEC pipeline (issue #46):

| Feature              | Lineage                       | id_columns       |
|----------------------|-------------------------------|------------------|
| sec/raw_html         | source                        | (filing_id,)     |
| sec/extracted_text   | identity ← raw_html           | (filing_id,)     |
| sec/bi_signals       | expansion ← extracted_text    | (term_id,)       |
| sec/embeddings       | expansion ← extracted_text    | (chunk_id,)      |
| sec/fts_index        | expansion ← extracted_text    | (content_id,)    |
| sec/search_index     | aggregation ← embeddings + fts_index | (filing_id,) |

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


class Embeddings(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="sec/embeddings",
        id_columns=("chunk_id",),
        deps=[
            mx.FeatureDep(
                feature="sec/extracted_text",
                lineage=mx.LineageRelationship.expansion(on=["filing_id"]),
            ),
        ],
    ),
):
    pass


class FtsIndex(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="sec/fts_index",
        id_columns=("content_id",),
        deps=[
            mx.FeatureDep(
                feature="sec/extracted_text",
                lineage=mx.LineageRelationship.expansion(on=["filing_id"]),
            ),
        ],
    ),
):
    pass


class SearchIndex(
    mx.BaseFeature,
    spec=mx.FeatureSpec(
        key="sec/search_index",
        id_columns=("filing_id",),
        deps=[
            mx.FeatureDep(
                feature="sec/embeddings",
                lineage=mx.LineageRelationship.aggregation(on=["filing_id"]),
            ),
            mx.FeatureDep(
                feature="sec/fts_index",
                lineage=mx.LineageRelationship.aggregation(on=["filing_id"]),
            ),
        ],
    ),
):
    pass
