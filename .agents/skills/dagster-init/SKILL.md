---
name: dagster-init
description: Initialize a dagster project using the create-dagster cli. Create a dagster project, uv virtual environment, and everything needed for a user to run dg dev or dg check defs successfully. (project)
license: MIT
---

# Dagster Project Initialization

## Overview

This skill automates the creation of a new Dagster project using the `create-dagster` CLI tool with uv as the package manager. It creates a clean, Components-compatible project structure ready for local development.

## What This Skill Does

When invoked, this skill will:

1. ✅ Create a new Dagster project using `create-dagster@latest`
2. ✅ Set up a uv virtual environment with all dependencies
3. ✅ Initialize project structure with Components architecture
4. ✅ Ensure the project is ready to run `dg dev` or `dg check defs`
5. ✅ Provide clear next steps for development

## Prerequisites

Before running this skill, ensure:
- `uv` is installed (check with `uv --version`)
- You have a project name in mind (or will use the default)
- You're in the directory where you want to create the project

## Skill Workflow

### Step 1: Validate Environment

Check that uv is available:
```bash
uv --version
```

If uv is not installed, provide installation instructions:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Step 2: Get Project Name

Ask the user for a project name, or use a sensible default like `my-dagster-project`. Validate that:
- The name starts with a letter
- Contains only alphanumeric characters, hyphens, or underscores
- The directory doesn't already exist (or ask to overwrite)

### Step 3: Create Project with create-dagster

Use `uvx` to run the latest create-dagster CLI. The CLI requires interactive confirmation, so we pass "y" automatically using `printf`:

```bash
printf "y\n" | uvx create-dagster@latest project <project-name>
```

**Important:** The `printf "y\n"` automatically answers "yes" to the interactive prompt that asks for confirmation to proceed with project creation.

This will:
- Scaffold a new Dagster project with Components structure
- Create `pyproject.toml` with project metadata
- Set up package structure with `definitions.py`
- Create `definitions/defs/` directory for components

### Step 4: Install Dependencies

Navigate into the project directory and run uv sync:

```bash
cd <project-name>
uv sync
```

This creates the virtual environment and installs all dependencies specified in `pyproject.toml`.

### Step 5: Verify Installation

Check that the project is properly set up by running:

```bash
uv run dg check defs
```

This validates that:
- All dependencies are installed correctly
- The Dagster definitions are loadable
- The project structure is correct

### Step 6: Display Success Message

Provide the user with a clear summary and next steps:

```
✅ Successfully created Dagster project: <project-name>

📁 Project structure:
   • pyproject.toml - Project configuration
   • <project-name>/definitions.py - Main definitions module
   • <project-name>/definitions/defs/ - Components directory

🚀 Next steps:
   1. cd <project-name>
   2. uv run dg dev       # Start local development server
   3. Open http://localhost:3000 to view Dagster UI

💡 Additional commands:
   • uv run dg check defs  # Validate definitions
   • uv run pytest         # Run tests (if configured)
   • uv add <package>      # Add new dependencies
```

## Error Handling

Handle common issues gracefully:

1. **uv not installed**: Provide installation instructions
2. **Directory already exists**: Ask user to choose different name or overwrite
3. **create-dagster fails**: Show error details and suggest troubleshooting
   - Note: The CLI requires interactive confirmation - we automatically pass "y" via `printf "y\n"` to avoid hanging
4. **Dependency installation fails**: Check network, suggest clearing cache
5. **dg check defs fails**: Show validation errors and help debug

## Alternative: Using the Python Script

You can also invoke the provided Python script directly:

```bash
python .agents/skills/dagster-init/scripts/create-dagster.py
```

This provides an interactive workflow with the same functionality. The script automatically handles the interactive prompt by passing "y" to stdin, so it won't hang waiting for user input.

## Project Structure

After successful creation, the project will have:

```
<project-name>/
├── pyproject.toml              # Project metadata and dependencies
├── <project-name>/
│   ├── __init__.py
│   ├── definitions.py          # Main Dagster definitions
│   └── definitions/
│       └── defs/               # Components directory
│           ├── __init__.py
│           └── ...             # Your components go here
├── <project-name>_tests/       # Test directory
├── .venv/                      # uv virtual environment
└── uv.lock                     # Locked dependencies
```

## Tips for Success

- Use descriptive project names that reflect the purpose
- Run `dg check defs` regularly during development to catch issues early
- Keep dependencies minimal initially, add as needed
- Follow the Components pattern for scalable project organization
- Use `uv add` to add new dependencies (it updates pyproject.toml automatically)

## Related Skills

- **dg-plus-init**: For setting up Dagster+ Cloud deployments
- Use after creating a project with this skill to deploy to the cloud

## Resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Components Guide](https://docs.dagster.io/guides/build/projects/moving-to-components)
- [uv Documentation](https://docs.astral.sh/uv/)
- [create-dagster CLI](https://github.com/dagster-io/dagster/tree/master/python_modules/dagster/dagster/_cli/create_dagster)
