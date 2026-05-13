# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "google-generativeai>=0.8.0",
# ]
# ///
"""Analyze Claude Code conversation logs to identify patterns and suggest tooling improvements.

Parses JSONL conversation logs, extracts session summaries, and uses Google Gemini to identify
recurring workflows, one-off scripts, and opportunities for hooks/skills/CLAUDE.md additions.

Usage:
    uv run python scripts/analyze_claude_conversations.py
    uv run python scripts/analyze_claude_conversations.py --conversations-dir ~/.claude/projects/other-project/
    uv run python scripts/analyze_claude_conversations.py --output report.md
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path

import google.generativeai as genai

# Default conversation log directory for this project
DEFAULT_CONVERSATIONS_DIR = Path.home() / ".claude" / "projects" / "C--Users-alexn-Documents-github-economic-data-project-full"

# Record types we care about
RELEVANT_TYPES = {"user", "assistant", "summary"}

# Model for analysis
ANALYSIS_MODEL = "gemini-2.5-flash"

# Batch size for LLM calls
SESSION_BATCH_SIZE = 10

# Max chars for tool input summaries
TOOL_INPUT_MAX_CHARS = 200

# Max chars for text block summaries
TEXT_BLOCK_MAX_CHARS = 200


@dataclass
class ToolCall:
    name: str
    input_summary: str


@dataclass
class SessionSummary:
    session_id: str
    slug: str
    git_branch: str
    user_prompts: list[str] = field(default_factory=list)
    tool_calls: list[ToolCall] = field(default_factory=list)
    text_responses: list[str] = field(default_factory=list)
    timestamp_first: str = ""
    timestamp_last: str = ""


def _summarize_tool_input(tool_input: dict) -> str:
    """Create a truncated summary of tool call input."""
    # For common tools, extract the most useful field
    if "command" in tool_input:
        return tool_input["command"][:TOOL_INPUT_MAX_CHARS]
    if "pattern" in tool_input:
        return f"pattern={tool_input['pattern']}"
    if "file_path" in tool_input:
        return f"file={tool_input['file_path']}"
    if "query" in tool_input:
        return f"query={tool_input['query'][:TOOL_INPUT_MAX_CHARS]}"
    if "prompt" in tool_input:
        return tool_input["prompt"][:TOOL_INPUT_MAX_CHARS]

    # Fallback: serialize and truncate
    raw = json.dumps(tool_input)
    if len(raw) > TOOL_INPUT_MAX_CHARS:
        return raw[:TOOL_INPUT_MAX_CHARS] + "..."
    return raw


def extract_tool_calls(content_blocks: list[dict]) -> list[ToolCall]:
    """Extract tool calls from assistant message content blocks."""
    calls = []
    for block in content_blocks:
        if block.get("type") != "tool_use":
            continue
        name = block.get("name", "unknown")
        input_data = block.get("input", {})
        calls.append(ToolCall(name=name, input_summary=_summarize_tool_input(input_data)))
    return calls


def extract_text_responses(content_blocks: list[dict]) -> list[str]:
    """Extract text response summaries from assistant message content blocks."""
    texts = []
    for block in content_blocks:
        if block.get("type") != "text":
            continue
        text = block.get("text", "").strip()
        if text:
            texts.append(text[:TEXT_BLOCK_MAX_CHARS])
    return texts


def extract_user_prompt(content_blocks: list[dict]) -> str | None:
    """Extract plain text from user message, skipping tool_result blocks."""
    parts = []
    for block in content_blocks:
        if block.get("type") == "text":
            text = block.get("text", "").strip()
            if text and not text.startswith("[Request interrupted"):
                parts.append(text)
    return " ".join(parts) if parts else None


def parse_session(filepath: Path) -> SessionSummary | None:
    """Parse a single JSONL session file into a SessionSummary."""
    session_id = filepath.stem
    slug = ""
    git_branch = ""
    user_prompts: list[str] = []
    tool_calls: list[ToolCall] = []
    text_responses: list[str] = []
    timestamp_first = ""
    timestamp_last = ""

    try:
        with filepath.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue

                record_type = record.get("type")
                if record_type not in RELEVANT_TYPES:
                    continue

                # Track timestamps
                ts = record.get("timestamp", "")
                if ts:
                    if not timestamp_first:
                        timestamp_first = ts
                    timestamp_last = ts

                # Extract slug/branch from first record that has them
                if not slug and record.get("slug"):
                    slug = record["slug"]
                if not git_branch and record.get("gitBranch"):
                    git_branch = record["gitBranch"]

                msg = record.get("message", {})
                content = msg.get("content", [])

                if record_type == "user":
                    if isinstance(content, str):
                        text = content.strip()
                        # Skip system-generated local command caveats
                        if text and not text.startswith("<local-command-caveat>"):
                            user_prompts.append(text[:500])
                    elif isinstance(content, list):
                        prompt = extract_user_prompt(content)
                        if prompt:
                            user_prompts.append(prompt)

                elif record_type == "assistant" and isinstance(content, list):
                    tool_calls.extend(extract_tool_calls(content))
                    text_responses.extend(extract_text_responses(content))

    except Exception as e:
        print(f"Warning: Failed to parse {filepath.name}: {e}", file=sys.stderr)
        return None

    # Skip empty sessions
    if not user_prompts and not tool_calls:
        return None

    return SessionSummary(
        session_id=session_id,
        slug=slug,
        git_branch=git_branch,
        user_prompts=user_prompts,
        tool_calls=tool_calls,
        text_responses=text_responses,
        timestamp_first=timestamp_first,
        timestamp_last=timestamp_last,
    )


def session_to_summary_text(session: SessionSummary) -> str:
    """Convert a SessionSummary to a compact text representation for LLM analysis."""
    lines = [
        f"## Session: {session.slug or session.session_id}",
        f"Branch: {session.git_branch or 'unknown'}",
        f"Time: {session.timestamp_first} to {session.timestamp_last}",
        "",
        "### User Prompts:",
    ]
    for i, prompt in enumerate(session.user_prompts[:20], 1):  # Cap at 20 prompts
        lines.append(f"  {i}. {prompt[:300]}")

    lines.append("")
    lines.append("### Tool Calls:")

    # Summarize tool call frequencies + show unique examples
    tool_freq: dict[str, int] = {}
    tool_examples: dict[str, list[str]] = {}
    for tc in session.tool_calls:
        tool_freq[tc.name] = tool_freq.get(tc.name, 0) + 1
        if tc.name not in tool_examples:
            tool_examples[tc.name] = []
        if len(tool_examples[tc.name]) < 3:
            tool_examples[tc.name].append(tc.input_summary)

    for name, count in sorted(tool_freq.items(), key=lambda x: -x[1]):
        lines.append(f"  {name}: {count}x")
        for ex in tool_examples[name]:
            lines.append(f"    - {ex[:150]}")

    if session.text_responses:
        lines.append("")
        lines.append("### Key Responses (first 5):")
        for resp in session.text_responses[:5]:
            lines.append(f"  - {resp}")

    return "\n".join(lines)


def batch_sessions(sessions: list[SessionSummary], batch_size: int) -> list[list[SessionSummary]]:
    """Split sessions into batches."""
    return [sessions[i : i + batch_size] for i in range(0, len(sessions), batch_size)]


def _generate(model: genai.GenerativeModel, prompt: str) -> str:
    """Generate text from Gemini model."""
    response = model.generate_content(prompt)
    return response.text


def analyze_patterns(sessions: list[SessionSummary], model: genai.GenerativeModel) -> str:
    """Phase 2: Use Gemini to identify patterns across sessions."""
    batches = batch_sessions(sessions, SESSION_BATCH_SIZE)
    all_patterns: list[str] = []

    print(f"Analyzing {len(sessions)} sessions in {len(batches)} batches...", file=sys.stderr)

    for i, batch in enumerate(batches, 1):
        print(f"  Batch {i}/{len(batches)}...", file=sys.stderr)
        summaries = "\n\n---\n\n".join(session_to_summary_text(s) for s in batch)

        result = _generate(
            model,
            f"""Analyze these Claude Code conversation session summaries and identify patterns.

For each pattern you find, describe:
1. **Common goals/workflows** — recurring task patterns across sessions (e.g., "fix tests then create PR", "explore issue then implement")
2. **Frequent one-off scripts** — Bash tool calls that create temporary Python/shell scripts, or repeated manual sequences that could be automated
3. **Repeated tool sequences** — common chains of tool calls that indicate a workflow (e.g., Grep -> Read -> Edit -> Bash)
4. **Common branch patterns** — what kinds of work are being done based on branch names

Be specific and cite session slugs/branches. Focus on actionable patterns, not obvious ones.

Sessions:

{summaries}""",
        )
        all_patterns.append(result)

    # If multiple batches, do a consolidation pass
    if len(all_patterns) > 1:
        print("  Consolidating patterns...", file=sys.stderr)
        combined = "\n\n---\n\n".join(
            f"### Batch {i+1} Analysis:\n{p}" for i, p in enumerate(all_patterns)
        )
        result = _generate(
            model,
            f"""Consolidate these pattern analyses from multiple batches of conversation sessions into a single, deduplicated report.

Merge similar patterns, keep the most specific examples, and rank by frequency/importance.

{combined}""",
        )
        return result

    return all_patterns[0] if all_patterns else "No patterns found."


def suggest_improvements(patterns: str, model: genai.GenerativeModel) -> str:
    """Phase 3: Suggest tooling improvements based on identified patterns."""
    print("  Generating improvement suggestions...", file=sys.stderr)

    return _generate(
        model,
        f"""Based on these patterns identified from Claude Code conversation logs, suggest concrete tooling improvements.

Claude Code supports these extensibility mechanisms:
- **Custom skills** (slash commands): User-defined prompts that can be invoked with /<name>. Defined as markdown files in .claude/skills/
- **Hooks**: Shell commands that run before/after specific tool calls (PreToolUse, PostToolUse) or notifications. Configured in .claude/settings.json
- **CLAUDE.md**: Project instructions file that Claude reads at session start. Can contain conventions, common commands, project structure docs
- **Permission rules**: Allow/deny rules for specific tools/patterns in .claude/settings.json (e.g., auto-allow Bash commands matching a pattern)
- **MCP servers**: Custom tool servers that provide additional capabilities

For each suggestion, provide:
1. **What it automates** — the specific recurring pattern it addresses
2. **Implementation** — concrete config/code (not just a description)
3. **Impact** — estimated time savings or quality improvement

Categories to cover:
1. Custom skills that could automate frequent workflows
2. Hooks that could automate repetitive pre/post-tool actions
3. CLAUDE.md additions for patterns Claude keeps rediscovering
4. Permission rules for frequently-approved tool patterns

Patterns found:

{patterns}""",
    )


def format_report(
    sessions: list[SessionSummary],
    patterns: str,
    improvements: str,
) -> str:
    """Format the final markdown report."""
    # Session stats
    total_prompts = sum(len(s.user_prompts) for s in sessions)
    total_tools = sum(len(s.tool_calls) for s in sessions)
    branches = {s.git_branch for s in sessions if s.git_branch}

    tool_freq: dict[str, int] = {}
    for s in sessions:
        for tc in s.tool_calls:
            tool_freq[tc.name] = tool_freq.get(tc.name, 0) + 1

    top_tools = sorted(tool_freq.items(), key=lambda x: -x[1])[:15]

    lines = [
        "# Claude Code Conversation Analysis Report",
        "",
        "## Overview",
        "",
        f"- **Sessions analyzed**: {len(sessions)}",
        f"- **Total user prompts**: {total_prompts}",
        f"- **Total tool calls**: {total_tools}",
        f"- **Unique branches**: {len(branches)}",
        "",
        "### Top Tools Used",
        "",
        "| Tool | Count |",
        "|------|-------|",
    ]
    for name, count in top_tools:
        lines.append(f"| {name} | {count} |")

    lines.extend([
        "",
        "### Branches Worked On",
        "",
    ])
    for b in sorted(branches):
        lines.append(f"- `{b}`")

    lines.extend([
        "",
        "---",
        "",
        "## Identified Patterns",
        "",
        patterns,
        "",
        "---",
        "",
        "## Suggested Improvements",
        "",
        improvements,
    ])

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze Claude Code conversation logs for patterns and improvement opportunities."
    )
    parser.add_argument(
        "--conversations-dir",
        type=Path,
        default=DEFAULT_CONVERSATIONS_DIR,
        help="Directory containing JSONL conversation logs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output file path (default: stdout)",
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Skip LLM analysis, only output parsed session stats",
    )
    args = parser.parse_args()

    conversations_dir: Path = args.conversations_dir
    if not conversations_dir.exists():
        print(f"Error: Directory not found: {conversations_dir}", file=sys.stderr)
        sys.exit(1)

    jsonl_files = sorted(conversations_dir.glob("*.jsonl"))
    if not jsonl_files:
        print(f"Error: No .jsonl files found in {conversations_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(jsonl_files)} conversation files in {conversations_dir}", file=sys.stderr)

    # Phase 1: Parse & Filter
    print("Phase 1: Parsing sessions...", file=sys.stderr)
    sessions: list[SessionSummary] = []
    for fp in jsonl_files:
        session = parse_session(fp)
        if session:
            sessions.append(session)

    print(f"  Parsed {len(sessions)} non-empty sessions", file=sys.stderr)

    if not sessions:
        print("Error: No sessions with content found.", file=sys.stderr)
        sys.exit(1)

    if args.skip_llm:
        # Just output stats without LLM analysis
        report = format_report(sessions, "_Skipped (--skip-llm)_", "_Skipped (--skip-llm)_")
    else:
        # Phase 2 & 3: LLM Analysis
        # Configure Gemini - looks for GEMINI_API_KEY or GOOGLE_API_KEY env var
        import os

        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            print(
                "Error: Set GEMINI_API_KEY or GOOGLE_API_KEY environment variable.",
                file=sys.stderr,
            )
            sys.exit(1)
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(ANALYSIS_MODEL)

        print("Phase 2: Analyzing patterns...", file=sys.stderr)
        patterns = analyze_patterns(sessions, model)
        print("Phase 3: Suggesting improvements...", file=sys.stderr)
        improvements = suggest_improvements(patterns, model)
        report = format_report(sessions, patterns, improvements)

    if args.output:
        args.output.write_text(report, encoding="utf-8")
        print(f"Report written to {args.output}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
