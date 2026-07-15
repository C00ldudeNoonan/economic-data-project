"""
Integration tests for the macro_agents project.
"""


class TestDagsterDefinitions:
    """Test Dagster definitions integration."""

    def test_definitions_load(self, dagster_defs):
        """Test that definitions load without errors."""
        assert dagster_defs is not None
        assert len(dagster_defs.assets) > 0
        assert len(dagster_defs.resources) > 0

    def test_all_resources_are_configurable(self, dagster_defs):
        """Test that all resources are configurable."""
        for resource_key, resource in dagster_defs.resources.items():
            assert hasattr(resource, "model_config")
            assert hasattr(type(resource), "model_fields")
