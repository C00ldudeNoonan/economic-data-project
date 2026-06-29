---
name: context-doc-maintainer
description: Automated agent workflow for reviewing and updating project agent context files when code changes are made.
license: MIT
---

# Context Documentation Maintainer

Use this skill when code changes require updates to agent-facing context files, project maps, skill instructions, PR templates, or repository guidance under `.agents/`.

## Scope

- Keep `.agents/` context aligned with current architecture, commands, and conventions.
- Update skill files only when code changes alter the workflow the skill describes.
- Preserve concise, task-oriented context; avoid duplicating detailed docs already available elsewhere.
- Do not update generated artifacts, secrets, local environment files, or unrelated documentation.

## Workflow

1. **Inspect changes**
   - Run `git status --short`.
   - Compare against the PR base with `git diff <base>...HEAD --stat` and `git diff <base>...HEAD --name-status` when a base is known.
   - Categorize changes by domain: Dagster, dbt, API, frontend, tests, infrastructure, or docs.

2. **Map context impact**
   - Update `.agents/skills/*/SKILL.md` when a workflow, command, path, or convention changed.
   - Update `.agents/skills/*/references/*.md` when detailed domain guidance changed.
   - Update `.agents/AGENTS.md` or repo-level `AGENTS.md` only for broad project conventions or architecture shifts.
   - Update `.agents/skills/context-doc-maintainer/templates/pr_template.md` only when the PR handoff structure changes.

3. **Edit context**
   - Keep each `SKILL.md` lean and directly actionable.
   - Prefer linking to reference files over embedding long examples.
   - Use `.agents/` paths for project agent context.
   - Avoid stale product-specific wording unless the skill is explicitly about that product.

4. **Validate**
   - Check frontmatter includes `name` and `description`.
   - Search for stale product/path references with ripgrep before handoff.
   - Check for prohibited auxiliary docs: `find .agents/skills -name README.md -o -name CHANGELOG.md -o -name INSTALLATION_GUIDE.md -o -name QUICK_REFERENCE.md`.
   - Run `git diff --check`.

5. **Handoff**
   - Summarize which context files changed and why.
   - Call out validation that could not run and the exact blocker.
   - Do not create commits, PRs, or merges unless the user explicitly asks.

## PR Template

Use `templates/pr_template.md` when creating a documentation-only PR for context updates.
