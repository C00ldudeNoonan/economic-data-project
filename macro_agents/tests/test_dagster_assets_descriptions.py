"""
Test to ensure all Dagster assets have descriptions.
"""
import pytest
from macro_agents.definitions import defs


def test_all_dagster_assets_have_descriptions():
    """Test that all Dagster assets have descriptions."""
    assets_without_descriptions = []
    
    for asset_key, asset in defs.assets.items():
        if not asset.description or asset.description.strip() == "":
            assets_without_descriptions.append(asset_key.to_user_string())
    
    if assets_without_descriptions:
        pytest.fail(
            f"The following assets are missing descriptions: {', '.join(assets_without_descriptions)}"
        )
    
    # If we get here, all assets have descriptions
    assert len(assets_without_descriptions) == 0
