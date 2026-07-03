---
name: start-issue
description: Automate issue triage, planning, and branch creation for issue-driven development.
version: 1.0.0
tags: [github, workflow, planning]
---

# Start Issue Skill

Use this skill to go from GitHub issue → plan → branch in a consistent, repeatable way.

## Workflow

1. **Load issue context**
   - Use `gh issue view <id> --json title,body,labels,assignees,comments,updatedAt` to avoid project-card GraphQL issues.
   - Summarize requirements, constraints, and acceptance criteria.

2. **Clarify scope**
   - If requirements are ambiguous, ask targeted questions before code changes.

3. **Locate code**
   - Use `rg --files` to identify modules.
   - Use `rg -n "<keyword>"` to locate patterns.

4. **Plan**
   - Draft a short, ordered plan (2-5 steps).
   - Confirm with the user before executing if the plan changes scope.

5. **Create branch**
   - Use `git checkout -b feat/issue-<id>-<short-slug>` (or `fix/issue-<id>-<short-slug>` for bugfixes).

6. **Post plan comment (optional)**
   - If requested, post the plan with `gh issue comment <id> --body "..."`.

7. **Implement and validate**
   - Make focused edits, run relevant tests/lints, and summarize changes.

## Notes

- Never commit directly to `main`.
- Prefer small, focused commits with clear intent.
- If the issue asks for a plan only, stop before code changes.
