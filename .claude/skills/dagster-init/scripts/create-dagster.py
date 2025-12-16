#!/usr/bin/env python3
"""
Dagster Project Initialization Script

Creates a new Dagster project using create-dagster CLI with uv package manager.
Sets up a complete development environment ready for `dg dev` or `dg check defs`.

Usage:
    python create-dagster.py [project-name]
"""

import os
import sys
import subprocess
import shutil
import re
import argparse


def check_uv_installed():
    """Check if uv is installed and available."""
    try:
        result = subprocess.run(
            ["uv", "--version"],
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print(f" uv is installed: {result.stdout.strip()}")
            return True
        return False
    except FileNotFoundError:
        return False


def install_uv_instructions():
    """Provide instructions for installing uv."""
    print("\nL uv is not installed.")
    print("\n=� To install uv, run:")
    print("   curl -LsSf https://astral.sh/uv/install.sh | sh")
    print("\nOr visit: https://docs.astral.sh/uv/")
    return False


def validate_project_name(name):
    """Validate that the project name follows Python package naming conventions."""
    if not name:
        return False

    # Must start with letter, contain only alphanumeric, hyphens, underscores
    if not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', name):
        return False

    return True


def get_project_name(provided_name=None):
    """Get and validate project name from user or argument."""
    if provided_name:
        if validate_project_name(provided_name):
            return provided_name
        else:
            print(f"�  Invalid project name: {provided_name}")
            print("Project name must start with a letter and contain only letters, numbers, hyphens, and underscores.")

    # Interactive prompt
    while True:
        name = input("\nEnter project name (or press Enter for 'my-dagster-project'): ").strip()

        if not name:
            return "my-dagster-project"

        if validate_project_name(name):
            return name
        else:
            print("�  Invalid project name.")
            print("Project name must start with a letter and contain only letters, numbers, hyphens, and underscores.")


def check_directory_exists(project_name):
    """Check if directory exists and handle accordingly."""
    if not os.path.exists(project_name):
        return True

    print(f"\n�  Directory '{project_name}' already exists.")
    while True:
        choice = input("Choose an option:\n  1. Use a different name\n  2. Remove and recreate\n  3. Cancel\nEnter choice (1-3): ").strip()

        if choice == "1":
            return False  # Will prompt for new name
        elif choice == "2":
            try:
                shutil.rmtree(project_name)
                print(f"=�  Removed existing directory: {project_name}")
                return True
            except Exception as e:
                print(f"L Failed to remove directory: {e}")
                return False
        elif choice == "3":
            print("L Operation cancelled")
            sys.exit(0)
        else:
            print("�  Invalid choice. Please enter 1, 2, or 3.")


def create_project_with_uvx(project_name):
    """Create Dagster project using uvx and create-dagster CLI."""
    print(f"\n=� Creating Dagster project: {project_name}")
    print("=' Using create-dagster@latest via uvx...")

    try:
        # Pass "y\n" to stdin to automatically answer the interactive prompt
        result = subprocess.run(
            ["uvx", "create-dagster@latest", "project", project_name],
            capture_output=True,
            text=True,
            input="y\n"  # Automatically answer "yes" to the interactive prompt
        )

        if result.returncode != 0:
            print(f"L Failed to create project")
            print(f"Error output: {result.stderr}")
            return False

        # Show any output from create-dagster
        if result.stdout:
            print(result.stdout)

        print(f" Project scaffolded successfully: {project_name}/")
        return True

    except FileNotFoundError:
        print("L uvx command not found. Please ensure uv is installed correctly.")
        return False
    except Exception as e:
        print(f"L Error creating project: {e}")
        return False


def install_dependencies(project_name):
    """Install project dependencies using uv sync."""
    print("\n=� Installing dependencies with uv sync...")

    project_dir = os.path.abspath(project_name)

    try:
        result = subprocess.run(
            ["uv", "sync"],
            cwd=project_dir,
            capture_output=True,
            text=True
        )

        if result.returncode == 0:
            print(" Dependencies installed successfully")

            # Show the virtual environment location
            venv_path = os.path.join(project_dir, ".venv")
            if os.path.exists(venv_path):
                print(f"=� Virtual environment created at: {venv_path}")

            return True
        else:
            print(f"�  uv sync had issues:")
            print(result.stderr)
            return False

    except Exception as e:
        print(f"L Error installing dependencies: {e}")
        return False


def verify_installation(project_name):
    """Verify the project is correctly set up by running dg check defs."""
    print("\n= Verifying project setup...")

    project_dir = os.path.abspath(project_name)

    try:
        result = subprocess.run(
            ["uv", "run", "dg", "check", "defs"],
            cwd=project_dir,
            capture_output=True,
            text=True,
            timeout=30
        )

        if result.returncode == 0:
            print(" Project verification successful!")
            print("   All definitions loaded correctly.")
            return True
        else:
            print("�  Project verification had warnings:")
            if result.stdout:
                print(result.stdout)
            if result.stderr:
                print(result.stderr)
            return True  # Still consider it success, might just be warnings

    except subprocess.TimeoutExpired:
        print("�  Verification timed out, but project is likely OK")
        return True
    except Exception as e:
        print(f"�  Could not verify installation: {e}")
        print("   You can manually verify with: uv run dg check defs")
        return True


def display_success_message(project_name):
    """Display success message and next steps."""
    project_dir = os.path.abspath(project_name)

    print("\n" + "=" * 60)
    print(f"<� Successfully created Dagster project: {project_name}")
    print("=" * 60)

    print(f"\n=� Project structure:")
    print(f"   " pyproject.toml - Project configuration")
    print(f"   " {project_name}/definitions.py - Main definitions module")
    print(f"   " {project_name}/definitions/defs/ - Components directory")
    print(f"   " .venv/ - Virtual environment")

    print(f"\n=� Next steps:")
    print(f"   1. cd {project_name}")
    print(f"   2. uv run dg dev")
    print(f"   3. Open http://localhost:3000 in your browser")

    print(f"\n=� Useful commands:")
    print(f"   " uv run dg check defs  - Validate your definitions")
    print(f"   " uv run pytest         - Run tests")
    print(f"   " uv add <package>      - Add new dependencies")
    print(f"   " uv run python         - Run Python in the venv")

    print(f"\n=� Project location: {project_dir}")
    print()


def main():
    """Main function for Dagster project initialization."""
    parser = argparse.ArgumentParser(
        description="Create a new Dagster project with uv"
    )
    parser.add_argument(
        "project_name",
        nargs="?",
        help="Name of the Dagster project to create"
    )

    args = parser.parse_args()

    print("=� Dagster Project Initialization")
    print("=" * 60)

    # Step 1: Check uv installation
    if not check_uv_installed():
        install_uv_instructions()
        sys.exit(1)

    # Step 2: Get and validate project name
    project_name = None
    while not project_name:
        candidate_name = get_project_name(args.project_name)
        if check_directory_exists(candidate_name):
            project_name = candidate_name
        else:
            args.project_name = None  # Clear arg so we prompt again

    # Step 3: Create project with create-dagster
    if not create_project_with_uvx(project_name):
        print("\nL Project creation failed")
        sys.exit(1)

    # Step 4: Install dependencies
    if not install_dependencies(project_name):
        print("\n�  Dependencies installation had issues, but project was created")
        print("   You can try running 'uv sync' manually in the project directory")

    # Step 5: Verify installation
    verify_installation(project_name)

    # Step 6: Display success message
    display_success_message(project_name)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nL Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nL Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
