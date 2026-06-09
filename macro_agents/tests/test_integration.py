"""
Integration tests for the macro_agents project.
"""

from macro_agents.definitions import defs


class TestDagsterDefinitions:
    """Test Dagster definitions integration."""

    def test_definitions_load(self):
        """Test that definitions load without errors."""
        assert defs is not None
        assert len(defs.assets) > 0
        assert len(defs.resources) > 0

    def test_all_resources_are_configurable(self):
        """Test that all resources are configurable."""
        for resource_key, resource in defs.resources.items():
            assert hasattr(resource, "model_config")
            assert hasattr(type(resource), "model_fields")
