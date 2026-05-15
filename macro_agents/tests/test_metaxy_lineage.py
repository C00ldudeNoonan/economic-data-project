"""Tests for Metaxy FeatureSpec declarations (issue #46, Phase 1).

These are pure validation tests on the FeatureSpec module — they don't touch a
metadata store. The integration test that materializes the wrapped asset twice
lives separately (gated by MotherDuck credentials) so it can be skipped in CI.
"""

import pytest

pytest.importorskip("metaxy")

from metaxy import FeatureSpec  # noqa: E402

from macro_agents.defs.domains.sec import lineage  # noqa: E402


def test_raw_html_is_source_feature():
    spec = lineage.raw_html
    assert isinstance(spec, FeatureSpec)
    assert str(spec.key) == "sec/raw_html"
    assert spec.id_columns == ("filing_id",)
    assert spec.deps == []


def test_extracted_text_has_identity_lineage_from_raw_html():
    spec = lineage.extracted_text
    assert isinstance(spec, FeatureSpec)
    assert str(spec.key) == "sec/extracted_text"
    assert spec.id_columns == ("filing_id",)
    assert len(spec.deps) == 1
    parent = spec.deps[0]
    assert str(parent.feature) == "sec/raw_html"
    # Identity is the default relationship; the type name is the cheapest
    # cross-version check.
    assert type(parent.lineage.relationship).__name__ == "IdentityRelationship"
