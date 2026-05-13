# Claude Code Instructions

## Git Workflow

**Always follow this workflow for ALL code changes, no matter how small:**

1. Create a feature branch from main
2. Make commits to the feature branch
3. Push the branch to origin
4. Create a Pull Request for review
5. Only merge to main after PR approval

**Never commit directly to main.** This applies to:
- New features
- Bug fixes
- Test fixes
- Documentation updates
- Any other changes

## Branch Naming Convention

- Features: `feat/issue-{number}-{short-description}`
- Bug fixes: `fix/issue-{number}-{short-description}`
- Test fixes: `test/{short-description}`
- Refactoring: `refactor/{short-description}`
- Documentation: `docs/{short-description}`

## Worktrees For Parallel Sessions

- If multiple Claude sessions are active, use a separate git worktree per session to avoid file conflicts.
- Create a worktree from `main` using an issue-based branch name: `git worktree add -b feat/issue-123-signal-charts ../economic-data-project-full-wt-issue-123 main`
- List worktrees: `git worktree list`
- Remove a worktree when done (from the primary repo): `git worktree remove ../economic-data-project-full-wt-issue-123`

# Structuring Your Codebase for LLM Coding Agents

## Why This Matters

LLM coding agents read, re-read, and iterate on your files across multi-step loops. Every file load costs tokens. A single task might load the same file 3-5+ times (read → edit → validate → fix). Your codebase structure directly controls how expensive and effective those loops are.

## File Size and Scoping

**Target 100-400 lines per file.** This balances token cost per read against navigation overhead.

- Each file should have a single, clear responsibility
- If a file requires scrolling through unrelated logic to find what matters, split it
- If splitting creates files that are always loaded together, keep them combined
- Cohesion beats small size — 10 tightly coupled 30-line files cost more than one 300-line file because the agent loads all 10 anyway

## Directory Structure

**Make the structure self-documenting.** Agents spend tokens exploring your repo to orient themselves. Reduce that cost.

- Use descriptive directory and file names that telegraph contents
- Group by domain/feature, not by type (e.g., `billing/` over `models/`, `services/`, `utils/`)
- Keep nesting shallow — 2-3 levels max
- Place related files close together in the tree

## Reduce Agent Navigation Tokens

- Maintain a project map file (this file, `CLAUDE.md`, `AGENTS.md`, `CONVENTIONS.md`) at the repo root
- Document non-obvious architecture decisions inline where they apply
- Use consistent naming conventions so the agent can predict file locations
- Keep imports explicit — barrel files and re-exports obscure dependency chains

## Code Conventions That Help Agents

- Write clear function and class names that describe behavior
- Keep functions short and single-purpose (easier to target edits)
- Use type hints and type annotations — they give agents context without reading implementations
- Place constants and config near where they're used, not in distant shared files
- Prefer explicit over clever — agents parse straightforward code faster and more reliably

## What to Avoid

- Monolith files (1000+ lines) — agents pay full token cost even for one-line changes
- Circular dependencies — agents get stuck in loops trying to resolve context
- Deep inheritance chains — forces loading many files to understand one class
- Magic strings and implicit behavior — agents can't infer what they can't see
- Overly DRY abstractions that scatter logic across many files for a single concept

## Project Map Template

Keep this at your repo root. It gives agents a cheap orientation pass instead of exploratory file reads.

```markdown
# Project: [Name]

## Quick Reference
- Language: [e.g., Python 3.12]
- Framework: [e.g., FastAPI, Next.js]
- Package manager: [e.g., uv, pnpm]
- Test runner: [e.g., pytest, vitest]

## Architecture
[2-3 sentences on how the app is structured]

## Key Directories
- `src/api/` — Route handlers and request validation
- `src/core/` — Business logic and domain models
- `src/db/` — Database models, migrations, queries
- `src/integrations/` — Third-party service clients

## Common Tasks
- Run tests: `[command]`
- Start dev server: `[command]`
- Run migrations: `[command]`

## Conventions
- [List project-specific patterns the agent should follow]
- [e.g., "All API endpoints return Pydantic response models"]
- [e.g., "Use repository pattern for database access"]
```

## The Token Math

| Scenario | File Size | Agent Reads | Tokens Used |
|---|---|---|---|
| Monolith | 2,000 lines | 4x per task | ~8,000 lines |
| Well-scoped | 200 lines | 4x per task | ~800 lines |
| Over-split | 30 lines × 8 files | 4x per task | ~960 lines + navigation overhead |

The well-scoped approach isn't just cheaper — it's more reliable. Smaller, focused context means fewer hallucinations and more accurate edits.
