"""Tests for Metaxy feature declarations (issue #46, Phase 1).

Pure validation tests on the feature module — they don't touch a metadata
store. The integration test that materializes the wrapped asset twice lives
separately so it can be skipped in CI.
"""

import pytest

pytest.importorskip("metaxy")

from macro_agents.defs.domains.sec import lineage  # noqa: E402


def _spec(cls):
    """Return the FeatureSpec attached to a BaseFeature subclass."""
    return cls._spec


def test_raw_html_is_source_feature():
    spec = _spec(lineage.RawHtml)
    assert str(spec.key) == "sec/raw_html"
    assert spec.id_columns == ("filing_id",)
    assert spec.deps == []


def test_extracted_text_has_identity_lineage_from_raw_html():
    spec = _spec(lineage.ExtractedText)
    assert str(spec.key) == "sec/extracted_text"
    assert spec.id_columns == ("filing_id",)
    assert len(spec.deps) == 1
    parent = spec.deps[0]
    assert str(parent.feature) == "sec/raw_html"
    assert type(parent.lineage.relationship).__name__ == "IdentityRelationship"
