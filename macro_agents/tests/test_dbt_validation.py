"""
Tests for dbt project validation and compilation.
"""

from pathlib import Path


class TestDbtProjectValidation:
    """Test dbt project configuration and compilation."""

    def test_dbt_packages_installed(self, dbt_project_dir: Path) -> None:
        """Test that all locked dbt packages are installed."""
        dbt_packages_dir = dbt_project_dir / "dbt_packages"
        assert dbt_packages_dir.is_dir()
        assert (dbt_packages_dir / "dbt_utils").is_dir()
        assert (dbt_packages_dir / "dbt_project_evaluator").is_dir()

    def test_dbt_parse_succeeds(self, dbt_manifest: dict[str, object]) -> None:
        """Test that the session-level offline parse produced a manifest."""
        assert dbt_manifest["metadata"]

    def test_dbt_manifest_contains_models(
        self, dbt_manifest: dict[str, object]
    ) -> None:
        """Test that the parsed project contains model nodes."""
        nodes = dbt_manifest["nodes"]
        assert isinstance(nodes, dict)
        models = [
            node
            for node in nodes.values()
            if isinstance(node, dict) and node.get("resource_type") == "model"
        ]
        assert models

    def test_dbt_manifest_contains_tests(self, dbt_manifest: dict[str, object]) -> None:
        """Test that dbt test definitions parsed successfully."""
        nodes = dbt_manifest["nodes"]
        assert isinstance(nodes, dict)
        tests = [
            node
            for node in nodes.values()
            if isinstance(node, dict) and node.get("resource_type") == "test"
        ]
        assert tests

    def test_dbt_project_yml_exists(self, dbt_project_dir: Path) -> None:
        """Test that dbt_project.yml exists and is valid."""
        dbt_project_yml = dbt_project_dir / "dbt_project.yml"

        assert dbt_project_yml.exists(), (
            f"dbt_project.yml not found at {dbt_project_yml}"
        )

        dbt_project_yaml = dbt_project_dir / "dbt_project.yaml"
        assert dbt_project_yaml.exists() or dbt_project_yml.exists(), (
            f"Neither dbt_project.yml nor dbt_project.yaml found in {dbt_project_dir}"
        )

    def test_dbt_packages_yml_or_in_project_yml(self, dbt_project_dir: Path) -> None:
        """Test that packages are defined (either in packages.yml or dbt_project.yml)."""
        packages_yml = dbt_project_dir / "packages.yml"
        dbt_project_yml = dbt_project_dir / "dbt_project.yml"

        has_packages_yml = packages_yml.exists()
        has_packages_in_project = False

        if dbt_project_yml.exists():
            content = dbt_project_yml.read_text(encoding="utf-8")
            if "packages:" in content or "package:" in content:
                has_packages_in_project = True

        assert has_packages_yml or has_packages_in_project, (
            "No packages definition found. Either create packages.yml or define "
            "packages in dbt_project.yml"
        )
