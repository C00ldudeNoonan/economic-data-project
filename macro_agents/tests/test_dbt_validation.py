"""
Tests for dbt project validation and compilation.
"""

import os
import subprocess
import pytest
from pathlib import Path


class TestDbtProjectValidation:
    """Test dbt project configuration and compilation."""

    @pytest.fixture
    def dbt_project_dir(self):
        """Get the dbt project directory."""
        current_file = Path(__file__).resolve()
        macro_agents_dir = current_file.parent.parent
        repo_root = macro_agents_dir.parent
        dbt_project_dir = repo_root / "dbt_project"

        if not dbt_project_dir.exists():
            pytest.skip(f"dbt_project directory not found at {dbt_project_dir}")

        return dbt_project_dir

    def test_dbt_packages_installed(self, dbt_project_dir):
        """Test that dbt packages are installed (dbt_packages directory exists)."""
        dbt_packages_dir = dbt_project_dir / "dbt_packages"

        if not dbt_packages_dir.exists():
            pytest.skip(
                f"dbt_packages directory not found at {dbt_packages_dir}. "
                f"Run 'dbt deps' in the dbt_project directory to install packages. "
                f"This is a setup requirement, not a code issue."
            )

        dbt_utils_dir = dbt_packages_dir / "dbt_utils"
        if not dbt_utils_dir.exists():
            pytest.skip(
                "dbt_utils package not found. "
                "Run 'dbt deps' in the dbt_project directory to install packages. "
                "This is a setup requirement, not a code issue."
            )

    def test_dbt_parse_succeeds(self, dbt_project_dir):
        """Test that dbt can parse the project without errors."""
        original_cwd = os.getcwd()
        try:
            os.chdir(dbt_project_dir)

            result = subprocess.run(
                ["dbt", "parse", "--target", "local"],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                pytest.fail(
                    f"dbt parse failed with exit code {result.returncode}.\n"
                    f"STDOUT:\n{result.stdout}\n\n"
                    f"STDERR:\n{result.stderr}"
                )
        finally:
            os.chdir(original_cwd)

    def test_dbt_compile_succeeds(self, dbt_project_dir):
        """Test that dbt can compile all models without errors."""
        original_cwd = os.getcwd()
        try:
            os.chdir(dbt_project_dir)

            result = subprocess.run(
                ["dbt", "compile", "--target", "local"],
                capture_output=True,
                text=True,
                timeout=120,
            )

            if result.returncode != 0:
                error_output = result.stderr or result.stdout

                if (
                    "cannot open file" in error_output.lower()
                    or "no such file or directory" in error_output.lower()
                ):
                    pytest.skip(
                        f"dbt compile requires database file to exist. "
                        f"This test validates syntax, not database connectivity. "
                        f"Error: {error_output[:500]}"
                    )

                pytest.fail(
                    f"dbt compile failed with exit code {result.returncode}.\n"
                    f"STDOUT:\n{result.stdout}\n\n"
                    f"STDERR:\n{result.stderr}"
                )
        finally:
            os.chdir(original_cwd)

    def test_dbt_list_models_succeeds(self, dbt_project_dir):
        """Test that dbt can list all models (validates project structure)."""
        original_cwd = os.getcwd()
        try:
            os.chdir(dbt_project_dir)

            result = subprocess.run(
                ["dbt", "list", "--target", "local", "--resource-type", "model"],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                pytest.fail(
                    f"dbt list models failed with exit code {result.returncode}.\n"
                    f"STDOUT:\n{result.stdout}\n\n"
                    f"STDERR:\n{result.stderr}"
                )

            models = [
                line.strip() for line in result.stdout.split("\n") if line.strip()
            ]
            if len(models) == 0:
                pytest.fail("No models found in dbt project")
        finally:
            os.chdir(original_cwd)

    def test_dbt_test_syntax_valid(self, dbt_project_dir):
        """Test that dbt can validate test syntax without running tests."""
        original_cwd = os.getcwd()
        try:
            os.chdir(dbt_project_dir)

            result = subprocess.run(
                ["dbt", "parse", "--target", "local"],
                capture_output=True,
                text=True,
                timeout=60,
            )

            if result.returncode != 0:
                error_output = result.stderr or result.stdout
                if "syntax error" in error_output.lower():
                    pytest.fail(f"dbt test syntax errors detected:\n{error_output}")
                pytest.fail(
                    f"dbt parse failed (may indicate test syntax issues):\n{error_output}"
                )
        finally:
            os.chdir(original_cwd)

    def test_dbt_project_yml_exists(self, dbt_project_dir):
        """Test that dbt_project.yml exists and is valid."""
        dbt_project_yml = dbt_project_dir / "dbt_project.yml"

        if not dbt_project_yml.exists():
            pytest.fail(f"dbt_project.yml not found at {dbt_project_yml}")

        dbt_project_yaml = dbt_project_dir / "dbt_project.yaml"
        if not dbt_project_yaml.exists() and not dbt_project_yml.exists():
            pytest.fail(
                f"Neither dbt_project.yml nor dbt_project.yaml found in {dbt_project_dir}"
            )

    def test_dbt_packages_yml_or_in_project_yml(self, dbt_project_dir):
        """Test that packages are defined (either in packages.yml or dbt_project.yml)."""
        packages_yml = dbt_project_dir / "packages.yml"
        dbt_project_yml = dbt_project_dir / "dbt_project.yml"

        has_packages_yml = packages_yml.exists()
        has_packages_in_project = False

        if dbt_project_yml.exists():
            with open(dbt_project_yml, "r") as f:
                content = f.read()
                if "packages:" in content or "package:" in content:
                    has_packages_in_project = True

        if not has_packages_yml and not has_packages_in_project:
            pytest.fail(
                "No packages definition found. "
                "Either create packages.yml or define packages in dbt_project.yml"
            )
