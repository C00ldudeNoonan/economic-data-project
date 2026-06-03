"""Check dbt models for known dbt Fusion (Rust runtime) compatibility issues.

Phase 7 of the BigQuery migration (issue #82).

Usage:
    uv run python scripts/check_fusion_compatibility.py
    uv run python scripts/check_fusion_compatibility.py --fix  # auto-fix trivial issues

Fusion is the new Rust-based dbt runtime available on dbt platform. It is
stricter than the Python runtime in a few areas. This script scans the dbt
project for known incompatibilities and reports them.

Known Fusion differences vs Python runtime:
1. `{{ this }}` in non-incremental models — Fusion may behave differently
2. `adapter.dispatch` without a fallback implementation
3. `run_query()` / `statement()` — not supported in Fusion
4. Python models — not currently supported
5. Macro recursion depth limits
6. Certain Jinja filter chains that worked by accident in Python
7. `config()` calls that reference other model outputs at parse time
"""

import argparse
import os
import re
import sys
from pathlib import Path

# Patterns that are known Fusion incompatibilities or warnings
CHECKS = [
    {
        "name": "run_query macro",
        "pattern": re.compile(r"\{\{[\s-]*run_query\s*\("),
        "severity": "error",
        "message": "run_query() is not supported in Fusion. Rewrite as a materialization or source.",
    },
    {
        "name": "statement macro",
        "pattern": re.compile(r"\{%-?\s*call\s+statement\s*\("),
        "severity": "error",
        "message": "statement() is not supported in Fusion.",
    },
    {
        "name": "print macro",
        "pattern": re.compile(r"\{\{[\s-]*print\s*\("),
        "severity": "warning",
        "message": "print() behavior may differ in Fusion.",
    },
    {
        "name": "this outside incremental guard",
        # Only flag {{ this }} in files that don't have is_incremental() or snapshot config
        "pattern": re.compile(r"\{\{[\s-]*this[\s.-]"),
        "severity": "info",
        "message": "{{ this }} reference — verify model is incremental or snapshot.",
        "skip_if_contains": ["is_incremental()", "materialized='snapshot'", 'materialized="snapshot"'],
    },
    {
        "name": "log macro",
        "pattern": re.compile(r"\{\{[\s-]*log\s*\("),
        "severity": "info",
        "message": "log() calls are allowed but output may be suppressed in Fusion.",
    },
    {
        "name": "execute flag",
        "pattern": re.compile(r"\bexecute\b"),
        "severity": "info",
        "message": "execute flag usage — verify behavior in Fusion compile-only mode.",
    },
    {
        "name": "fromjson / tojson without safety",
        "pattern": re.compile(r"\|[\s]*fromjson|\|[\s]*tojson"),
        "severity": "warning",
        "message": "JSON filter — use safe_fromjson or validate input to avoid Fusion parse errors.",
    },
]


def scan_file(path: Path) -> list[dict]:
    """Return list of issues found in a single SQL/Jinja file."""
    content = path.read_text(encoding="utf-8")
    issues = []
    for check in CHECKS:
        # Skip check for this file if it contains any of the skip_if_contains strings
        skip_strings = check.get("skip_if_contains", [])
        if any(s in content for s in skip_strings):
            continue
        for m in check["pattern"].finditer(content):
            line_no = content[: m.start()].count("\n") + 1
            issues.append(
                {
                    "file": str(path),
                    "line": line_no,
                    "severity": check["severity"],
                    "name": check["name"],
                    "message": check["message"],
                    "snippet": content.splitlines()[line_no - 1].strip(),
                }
            )
    return issues


def main():
    parser = argparse.ArgumentParser(description="Check dbt models for Fusion compatibility")
    parser.add_argument("--dbt-project", default=None, help="Path to dbt project root")
    parser.add_argument("--fix", action="store_true", help="Auto-fix trivial issues (no-op for now)")
    args = parser.parse_args()

    project_dir = args.dbt_project
    if not project_dir:
        # Auto-detect
        repo_root = Path(__file__).parent.parent
        project_dir = repo_root / "dbt_project"
        if not project_dir.exists():
            print("Could not find dbt_project/. Set --dbt-project.")
            sys.exit(1)

    project_dir = Path(project_dir)
    # Skip dbt_packages — those are third-party and not our responsibility
    sql_files = [
        f for f in project_dir.rglob("*.sql")
        if "dbt_packages" not in f.parts
    ]

    all_issues = []
    for f in sql_files:
        all_issues.extend(scan_file(f))

    errors = [i for i in all_issues if i["severity"] == "error"]
    warnings = [i for i in all_issues if i["severity"] == "warning"]
    infos = [i for i in all_issues if i["severity"] == "info"]

    print(f"\ndbt Fusion Compatibility Report")
    print(f"Project: {project_dir}")
    print(f"Files scanned: {len(sql_files)}")
    print(f"Issues found: {len(errors)} errors, {len(warnings)} warnings, {len(infos)} info\n")

    for severity, issues, label in [
        ("error", errors, "ERRORS (must fix before Fusion)"),
        ("warning", warnings, "WARNINGS (review before Fusion)"),
        ("info", infos, "INFO (verify behavior)"),
    ]:
        if not issues:
            continue
        print(f"{'=' * 60}")
        print(f"{label}")
        print(f"{'=' * 60}")
        for issue in issues:
            rel_path = Path(issue["file"]).relative_to(project_dir)
            print(f"  {rel_path}:{issue['line']}  [{issue['name']}]")
            print(f"    {issue['message']}")
            print(f"    -> {issue['snippet'][:100]}")
        print()

    if not all_issues:
        print("OK: No Fusion compatibility issues found.")

    if errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
