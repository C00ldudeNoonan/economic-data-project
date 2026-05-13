---
name: context-doc-maintainer
description: Automated agent that reviews and updates all Claude context files when code changes are made
license: MIT
---

# Claude Context Documentation Maintainer

## Description

This skill provides an automated workflow for maintaining Claude context files in the `.claude/` directory. When invoked, it analyzes git diffs between branches, uses LLM reasoning to intelligently determine which documentation needs updating, automatically modifies the relevant context files, validates the changes, and creates a pull request for review. This ensures that project documentation stays synchronized with code changes across the full-stack application.

## Usage

Invoke this skill when you need to:
- Update Claude context documentation after making significant code changes
- Ensure `.claude/skills/claude.md` reflects the current project architecture
- Synchronize skill files with new patterns or components added to the codebase
- Update security checklists when new APIs or dependencies are added
- Refresh environment variable documentation after configuration changes
- Maintain consistency between code and documentation before creating pull requests

## Prerequisites

This skill requires the following tools and configuration:

1. **Git Repository**: Must be in a git repository
   - Check: `git rev-parse --is-inside-work-tree`

2. **GitHub CLI (`gh`)**: Version 2.0 or higher
   - Check: `gh --version`
   - Install: https://cli.github.com/
   - Must be authenticated: `gh auth status`

3. **Claude API Access**: Anthropic API key for LLM analysis
   - Required environment variable: `ANTHROPIC_API_KEY`
   - Get key from: https://console.anthropic.com/

4. **JSON Processor (`jq`)**: For parsing API responses
   - Check: `jq --version`
   - Install: https://jqlang.github.io/jq/download/

5. **Optional Validation Tools**:
   - `markdownlint`: Markdown syntax validation
   - `yq`: YAML validation
   - `detect-secrets`: Secret scanning

## Instructions

This skill implements a 6-phase workflow to analyze changes, determine documentation updates, apply modifications, validate results, and create a pull request.

---

### Phase 1: Prerequisites & Setup

Verify all requirements and prepare for documentation analysis.

#### Step 1: Verify Git Repository

```bash
# Ensure we're in a git repository
if ! git rev-parse --is-inside-work-tree > /dev/null 2>&1; then
  echo "❌ Error: Not in a git repository"
  echo "Navigate to the project root or initialize with: git init"
  exit 1
fi

echo "✅ Git repository detected"
```

#### Step 2: Verify .claude/ Directory

```bash
# Check that Claude context directory exists
if [ ! -d ".claude" ]; then
  echo "❌ Error: .claude/ directory not found"
  echo "This directory should contain project context files"
  exit 1
fi

echo "✅ .claude/ directory found"
```

#### Step 3: Verify GitHub CLI Authentication

```bash
# Check GitHub CLI is installed and authenticated
if ! command -v gh &> /dev/null; then
  echo "❌ Error: GitHub CLI (gh) not installed"
  echo "Install from: https://cli.github.com/"
  exit 1
fi

if ! gh auth status &> /dev/null; then
  echo "❌ Error: GitHub CLI not authenticated"
  echo "Run: gh auth login"
  exit 1
fi

echo "✅ GitHub CLI authenticated"
```

#### Step 4: Verify Claude API Access

```bash
# Check for Anthropic API key
if [ -z "$ANTHROPIC_API_KEY" ]; then
  echo "❌ Error: ANTHROPIC_API_KEY environment variable not set"
  echo "Get your API key from: https://console.anthropic.com/"
  echo "Then export it: export ANTHROPIC_API_KEY='your-key-here'"
  exit 1
fi

echo "✅ Claude API key configured"
```

#### Step 5: Get Base Branch

```bash
# Default to 'main' branch, allow override via argument
BASE_BRANCH="${1:-main}"
CURRENT_BRANCH=$(git branch --show-current)

echo "=== Configuration ==="
echo "Base branch: $BASE_BRANCH"
echo "Current branch: $CURRENT_BRANCH"
echo "Analyzing changes from $BASE_BRANCH to $CURRENT_BRANCH"
```

#### Step 6: Verify Base Branch Exists

```bash
# Ensure base branch exists
if ! git rev-parse --verify "$BASE_BRANCH" &> /dev/null; then
  echo "❌ Error: Base branch '$BASE_BRANCH' not found"
  echo "Available branches:"
  git branch -a
  exit 1
fi

echo "✅ All prerequisites met. Starting documentation analysis..."
echo ""
```

---

### Phase 2: Git Diff Analysis

Analyze code changes and categorize them to determine documentation impact.

#### Step 1: Get Diff Statistics

```bash
echo "=== Analyzing Git Diff ==="

# Get overall diff statistics
DIFF_STAT=$(git diff "$BASE_BRANCH"...HEAD --stat)
echo "$DIFF_STAT"
echo ""

# Extract summary metrics
FILES_CHANGED=$(echo "$DIFF_STAT" | tail -1 | awk '{print $1}')
ADDITIONS=$(echo "$DIFF_STAT" | tail -1 | awk '{print $4}')
DELETIONS=$(echo "$DIFF_STAT" | tail -1 | awk '{print $6}')

echo "Summary: $FILES_CHANGED files changed, +$ADDITIONS/-$DELETIONS lines"
echo ""
```

#### Step 2: Get File Changes with Status

```bash
# Get list of changed files with their status (A=added, M=modified, D=deleted)
FILES_CHANGED_LIST=$(git diff "$BASE_BRANCH"...HEAD --name-status)

echo "=== Changed Files ==="
echo "$FILES_CHANGED_LIST"
echo ""
```

#### Step 3: Categorize Changes by File Pattern

```bash
# Initialize associative array for categorized changes
declare -A CATEGORIES

# Categorize each changed file
while IFS=$'\t' read -r status filepath; do
  [ -z "$filepath" ] && continue

  # Dagster agents
  if [[ $filepath =~ ^macro_agents/src/macro_agents/defs/agents/ ]]; then
    CATEGORIES[dagster_agents]+="$status $filepath\n"

  # Dagster ingestion
  elif [[ $filepath =~ ^macro_agents/src/macro_agents/defs/ingestion/ ]]; then
    CATEGORIES[dagster_ingestion]+="$status $filepath\n"

  # Dagster resources
  elif [[ $filepath =~ ^macro_agents/src/macro_agents/defs/resources/ ]]; then
    CATEGORIES[dagster_resources]+="$status $filepath\n"

  # Dagster schedules/sensors
  elif [[ $filepath =~ ^macro_agents/src/macro_agents/defs/schedules ]]; then
    CATEGORIES[dagster_schedules]+="$status $filepath\n"

  # dbt models
  elif [[ $filepath =~ ^dbt_project/models/ ]]; then
    CATEGORIES[dbt_models]+="$status $filepath\n"

  # API endpoints
  elif [[ $filepath =~ ^api/routers/ ]]; then
    CATEGORIES[api_endpoints]+="$status $filepath\n"

  # Frontend pages
  elif [[ $filepath =~ ^frontend/src/pages/ ]]; then
    CATEGORIES[frontend_pages]+="$status $filepath\n"

  # Frontend components
  elif [[ $filepath =~ ^frontend/src/components/ ]]; then
    CATEGORIES[frontend_components]+="$status $filepath\n"

  # GitHub workflows
  elif [[ $filepath =~ ^\.github/workflows/ ]]; then
    CATEGORIES[workflows]+="$status $filepath\n"

  # Python dependencies
  elif [[ $filepath =~ pyproject\.toml$ ]]; then
    CATEGORIES[dependencies_python]+="$status $filepath\n"

  # Node dependencies
  elif [[ $filepath =~ package\.json$ ]]; then
    CATEGORIES[dependencies_node]+="$status $filepath\n"

  # Environment variables
  elif [[ $filepath =~ \.env\.example$ ]]; then
    CATEGORIES[environment]+="$status $filepath\n"

  # Claude context files
  elif [[ $filepath =~ ^\.claude/ ]]; then
    CATEGORIES[claude_context]+="$status $filepath\n"
  fi
done <<< "$FILES_CHANGED_LIST"
```

#### Step 4: Generate Change Summary

```bash
# Display categorized changes
echo "=== Change Summary by Category ==="
for category in "${!CATEGORIES[@]}"; do
  echo ""
  echo "[$category]"
  echo -e "${CATEGORIES[$category]}"
done
echo ""
```

#### Step 5: Extract Detailed Diffs

```bash
# Extract detailed diffs for important files (not .claude/ files)
echo "=== Extracting Detailed Diffs ==="
DETAILED_DIFFS_FILE="/tmp/detailed_diffs_$$.txt"
> "$DETAILED_DIFFS_FILE"  # Clear file

for category in "${!CATEGORIES[@]}"; do
  # Skip Claude context changes (we're updating these)
  [[ "$category" == "claude_context" ]] && continue

  while IFS=$'\t' read -r status filepath; do
    [ -z "$filepath" ] && continue

    echo "### File: $filepath (Status: $status)" >> "$DETAILED_DIFFS_FILE"
    echo "" >> "$DETAILED_DIFFS_FILE"

    # Get the diff for this file
    git diff "$BASE_BRANCH"...HEAD -- "$filepath" >> "$DETAILED_DIFFS_FILE"
    echo "" >> "$DETAILED_DIFFS_FILE"
    echo "---" >> "$DETAILED_DIFFS_FILE"
    echo "" >> "$DETAILED_DIFFS_FILE"
  done <<< "$(echo -e "${CATEGORIES[$category]}")"
done

echo "✅ Detailed diffs extracted to: $DETAILED_DIFFS_FILE"
echo ""
```

---

### Phase 3: LLM Impact Analysis

Use Claude API to intelligently determine which context files need updates.

#### Step 1: Prepare System Prompt

```bash
# Create system prompt explaining context file structure
cat > /tmp/system_prompt_$$.txt << 'EOF'
You are a technical documentation expert analyzing code changes to determine what documentation updates are needed.

# Context Files in This Project

1. **.claude/skills/claude.md** (1,013 lines, main project documentation)
   Sections:
   - Overview
   - Architecture Overview
   - Tech Stack (Core Data Platform, Backend, Frontend, Supporting Technologies)
   - Project Structure (complete directory tree)
   - Data Sources
   - Key Components (Ingestion, Transformation, AI Agents, Resources, API Server, Frontend)
   - Data Flow
   - Environment Variables
   - Quick Start Guide
   - Development Workflow
   - Testing
   - Deployment
   - Code Style Guidelines

2. **.claude/skills/dagster-development/SKILL.md**
   Dagster patterns: asset definitions, resources, schedules, sensors, testing

3. **.claude/skills/dbt-development/*.md**
   dbt patterns: model organization, naming conventions, testing

4. **.claude/agents/security-reviewer.md**
   Security checklist: credentials, SQL injection, API security, dependencies

5. **.claude/settings.local.json**
   Bash command permissions

# Your Task

Analyze the provided code changes and determine which context files need updates.

For each update, provide:
- file: Path to context file
- section_path: Hierarchical path like "Tech Stack > Core Data Platform"
- action: "ADD", "UPDATE", or "REMOVE"
- content: Exact markdown content to add/update/remove
- reasoning: Why this change is needed (1-2 sentences)
- confidence: Float 0.0-1.0 indicating certainty

Return ONLY a valid JSON array of updates. No other text.

Example format:
[
  {
    "file": ".claude/skills/claude.md",
    "section_path": "Tech Stack > Core Data Platform",
    "action": "ADD",
    "content": "- **TextBlob**: Natural language processing library for sentiment analysis",
    "reasoning": "New dependency added in pyproject.toml for NLP capabilities",
    "confidence": 0.95
  }
]
EOF
```

#### Step 2: Prepare Analysis Prompt

```bash
# Create user prompt with change summary and diffs
cat > /tmp/analysis_prompt_$$.txt << EOF
# Code Changes to Analyze

## Summary
- Files changed: $FILES_CHANGED
- Additions: +$ADDITIONS lines
- Deletions: -$DELETIONS lines

## Categorized Changes

$(for category in "${!CATEGORIES[@]}"; do
  echo "### $category"
  echo -e "${CATEGORIES[$category]}"
  echo ""
done)

## Detailed Diffs

$(cat "$DETAILED_DIFFS_FILE" | head -1000)  # Limit to first 1000 lines for token management

# Instructions

Analyze these changes and provide JSON array of documentation updates needed.
Focus on:
1. New files → Add to project structure tree and relevant component sections
2. New dependencies → Add to Tech Stack section
3. New environment variables → Add to Environment Variables section
4. New API endpoints → Add to API Server section
5. Modified components → Update descriptions if functionality changed

Return ONLY the JSON array.
EOF
```

#### Step 3: Call Claude API

```bash
echo "=== Calling Claude API for Impact Analysis ==="

# Call Claude API using curl
ANALYSIS_RESPONSE=$(curl -s https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -H "content-type: application/json" \
  -d "{
    \"model\": \"claude-sonnet-4.5-20250929\",
    \"max_tokens\": 4000,
    \"system\": \"$(cat /tmp/system_prompt_$$.txt | jq -Rsa .)\",
    \"messages\": [
      {
        \"role\": \"user\",
        \"content\": \"$(cat /tmp/analysis_prompt_$$.txt | jq -Rsa .)\"
      }
    ]
  }")

# Check for API errors
if echo "$ANALYSIS_RESPONSE" | jq -e '.error' > /dev/null 2>&1; then
  echo "❌ Error calling Claude API:"
  echo "$ANALYSIS_RESPONSE" | jq '.error'
  exit 1
fi

echo "✅ Claude API call successful"
```

#### Step 4: Parse JSON Response

```bash
# Extract content from Claude's response
UPDATES_JSON=$(echo "$ANALYSIS_RESPONSE" | jq -r '.content[0].text')

# Save to file for processing
echo "$UPDATES_JSON" > /tmp/doc_updates_$$.json

# Validate JSON
if ! jq empty /tmp/doc_updates_$$.json 2>/dev/null; then
  echo "❌ Error: Claude API returned invalid JSON"
  echo "$UPDATES_JSON"
  exit 1
fi

echo "✅ Parsed JSON response"
```

#### Step 5: Filter by Confidence Threshold

```bash
# Filter updates with confidence >= 0.7
HIGH_CONFIDENCE_UPDATES=$(jq '[.[] | select(.confidence >= 0.7)]' /tmp/doc_updates_$$.json)
UPDATE_COUNT=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq 'length')

echo ""
echo "=== Analysis Results ==="
echo "Total updates suggested: $(jq 'length' /tmp/doc_updates_$$.json)"
echo "High-confidence updates (>= 0.7): $UPDATE_COUNT"

if [ "$UPDATE_COUNT" -eq 0 ]; then
  echo ""
  echo "No high-confidence documentation updates needed."
  echo "Code changes do not significantly impact context files."
  echo "Exiting."
  exit 0
fi

echo ""
echo "=== High-Confidence Updates ==="
echo "$HIGH_CONFIDENCE_UPDATES" | jq -r '.[] | "\(.file) - \(.section_path) (\(.action)) - Confidence: \(.confidence)"'
echo ""
```

---

### Phase 4: Documentation Updates

Apply the recommended updates to context files using Claude's Edit tool.

#### Step 1: Create Backup

```bash
echo "=== Creating Backups ==="

# Create backup directory with timestamp
BACKUP_DIR="/tmp/claude_context_backup_$$"
mkdir -p "$BACKUP_DIR"

# Backup all .claude/ files
cp -r .claude/* "$BACKUP_DIR/"

echo "✅ Backup created at: $BACKUP_DIR"
echo ""
```

#### Step 2: Process Each Update

This phase would use Claude's Edit tool in practice. For the bash version, we show the logic:

```bash
echo "=== Applying Documentation Updates ==="

# Save update details for PR description
UPDATE_DETAILS_FILE="/tmp/update_details_$$.txt"
> "$UPDATE_DETAILS_FILE"

# Process each update
for i in $(seq 0 $(($UPDATE_COUNT - 1))); do
  # Extract update details
  FILE=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq -r ".[$i].file")
  SECTION=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq -r ".[$i].section_path")
  ACTION=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq -r ".[$i].action")
  CONTENT=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq -r ".[$i].content")
  REASONING=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq -r ".[$i].reasoning")
  CONFIDENCE=$(echo "$HIGH_CONFIDENCE_UPDATES" | jq -r ".[$i].confidence")

  echo "[$((i+1))/$UPDATE_COUNT] Updating: $FILE"
  echo "  Section: $SECTION"
  echo "  Action: $ACTION"
  echo "  Confidence: $CONFIDENCE"
  echo "  Reason: $REASONING"
  echo ""

  # Record update for PR description
  cat >> "$UPDATE_DETAILS_FILE" << EOF
### Update $((i+1)): $FILE

**Section**: $SECTION
**Action**: $ACTION
**Confidence**: $CONFIDENCE

**Content**:
\`\`\`
$CONTENT
\`\`\`

**Reasoning**: $REASONING

---

EOF

  # Note: In actual implementation, use Claude's Edit tool here
  # Edit tool would:
  # 1. Read the file
  # 2. Find the section using hierarchical path matching
  # 3. Apply the ADD/UPDATE/REMOVE action
  # 4. Preserve formatting (lists, indentation, etc.)

  echo "  Note: Use Edit tool to apply this change"
  echo "  Edit(file_path='$FILE', section='$SECTION', action='$ACTION', content='''$CONTENT''')"
  echo ""
done

echo "✅ All updates processed"
echo "Update details saved to: $UPDATE_DETAILS_FILE"
echo ""
```

---

### Phase 5: Validation

Validate changes before committing to ensure quality and safety.

#### Step 1: Check Markdown Syntax

```bash
echo "=== Running Validation Checks ==="
VALIDATION_SUMMARY="/tmp/validation_summary_$$.txt"
> "$VALIDATION_SUMMARY"

# Markdown syntax validation (if markdownlint is available)
MARKDOWN_VALID=true
if command -v markdownlint &> /dev/null; then
  echo "Checking markdown syntax..."
  for file in .claude/**/*.md; do
    if [ -f "$file" ]; then
      if ! markdownlint "$file" 2>/dev/null; then
        echo "  ⚠️  Markdown issues in $file"
        MARKDOWN_VALID=false
      fi
    fi
  done

  if $MARKDOWN_VALID; then
    echo "✅ Markdown syntax: PASSED" | tee -a "$VALIDATION_SUMMARY"
  else
    echo "⚠️  Markdown syntax: WARNINGS" | tee -a "$VALIDATION_SUMMARY"
  fi
else
  echo "⏭️  Markdown syntax: SKIPPED (markdownlint not installed)" | tee -a "$VALIDATION_SUMMARY"
fi
echo ""
```

#### Step 2: Validate YAML Frontmatter

```bash
# YAML frontmatter validation (if yq is available)
YAML_VALID=true
if command -v yq &> /dev/null; then
  echo "Checking YAML frontmatter..."
  for file in .claude/skills/*/SKILL.md .claude/agents/*.md; do
    if [ -f "$file" ]; then
      # Extract frontmatter (between --- markers)
      FRONTMATTER=$(sed -n '/^---$/,/^---$/p' "$file" | sed '1d;$d')
      if [ -n "$FRONTMATTER" ]; then
        if ! echo "$FRONTMATTER" | yq eval . > /dev/null 2>&1; then
          echo "  ⚠️  Invalid YAML in $file"
          YAML_VALID=false
        fi
      fi
    fi
  done

  if $YAML_VALID; then
    echo "✅ YAML frontmatter: PASSED" | tee -a "$VALIDATION_SUMMARY"
  else
    echo "⚠️  YAML frontmatter: WARNINGS" | tee -a "$VALIDATION_SUMMARY"
  fi
else
  echo "⏭️  YAML frontmatter: SKIPPED (yq not installed)" | tee -a "$VALIDATION_SUMMARY"
fi
echo ""
```

#### Step 3: Check Diff Sizes

```bash
# Check if changes are reasonable (< 30% of file)
echo "Checking diff sizes..."
DIFF_SIZE_OK=true

for file in $(git diff --name-only .claude/); do
  if [ -f "$file" ]; then
    ORIGINAL_LINES=$(git show "$BASE_BRANCH:$file" 2>/dev/null | wc -l)
    CURRENT_LINES=$(wc -l < "$file")

    if [ "$ORIGINAL_LINES" -gt 0 ]; then
      DIFF_ABS=$((CURRENT_LINES > ORIGINAL_LINES ? CURRENT_LINES - ORIGINAL_LINES : ORIGINAL_LINES - CURRENT_LINES))
      DIFF_PERCENT=$(( DIFF_ABS * 100 / ORIGINAL_LINES ))

      if [ "$DIFF_PERCENT" -gt 30 ]; then
        echo "  ⚠️  $file changed ${DIFF_PERCENT}% (flagged for review)"
        DIFF_SIZE_OK=false
      fi
    fi
  fi
done

if $DIFF_SIZE_OK; then
  echo "✅ Diff sizes reasonable: PASSED" | tee -a "$VALIDATION_SUMMARY"
else
  echo "⚠️  Diff sizes: FLAGGED FOR REVIEW" | tee -a "$VALIDATION_SUMMARY"
fi
echo ""
```

#### Step 4: Scan for Secrets

```bash
# Secret detection (if detect-secrets is available)
if command -v detect-secrets &> /dev/null; then
  echo "Scanning for secrets..."
  if detect-secrets scan .claude/ 2>/dev/null 1>&2; then
    echo "✅ No secrets detected: PASSED" | tee -a "$VALIDATION_SUMMARY"
  else
    echo "⚠️  Potential secrets found: REVIEW NEEDED" | tee -a "$VALIDATION_SUMMARY"
  fi
else
  echo "⏭️  Secret scanning: SKIPPED (detect-secrets not installed)" | tee -a "$VALIDATION_SUMMARY"
fi
echo ""
```

#### Step 5: Display Validation Summary

```bash
echo "=== Validation Summary ==="
cat "$VALIDATION_SUMMARY"
echo ""
```

---

### Phase 6: PR Creation

Create a pull request with all updates and validation results.

#### Step 1: Check for Changes

```bash
echo "=== Preparing Pull Request ==="

# Stage .claude/ changes
git add .claude/

# Check if there are changes to commit
if git diff --cached --quiet; then
  echo "No changes to commit. All context files are already up to date."
  echo "Exiting."

  # Cleanup
  rm -rf "$BACKUP_DIR"
  exit 0
fi

echo "✅ Changes detected in .claude/ directory"
```

#### Step 2: Create Feature Branch

```bash
# Create timestamped branch
TIMESTAMP=$(date +%s)
BRANCH_NAME="docs/update-context-$TIMESTAMP"

git checkout -b "$BRANCH_NAME"
echo "✅ Created branch: $BRANCH_NAME"
```

#### Step 3: Generate Commit Message

```bash
# Create detailed commit message
COMMIT_MSG_FILE="/tmp/commit_msg_$$.txt"
cat > "$COMMIT_MSG_FILE" << EOF
docs: Update Claude context documentation

Automated documentation updates based on code changes from $BASE_BRANCH.

Changes analyzed:
- $FILES_CHANGED files changed
- +$ADDITIONS/-$DELETIONS lines

Updates applied:
$(git diff --cached --stat .claude/)

Validation:
$(cat "$VALIDATION_SUMMARY")

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>
EOF
```

#### Step 4: Commit Changes

```bash
# Commit with detailed message
git commit -F "$COMMIT_MSG_FILE"
echo "✅ Changes committed"
```

#### Step 5: Push Branch

```bash
# Push to remote
git push -u origin "$BRANCH_NAME"
echo "✅ Branch pushed to origin"
```

#### Step 6: Generate PR Body

```bash
# Generate PR description from template
PR_BODY_FILE="/tmp/pr_body_$$.txt"

# Read template
TEMPLATE=$(cat .claude/skills/context-doc-maintainer/templates/pr_template.md)

# Replace placeholders
echo "$TEMPLATE" | \
  sed "s|{{BASE_BRANCH}}|$BASE_BRANCH|g" | \
  sed "s|{{CURRENT_BRANCH}}|$CURRENT_BRANCH|g" | \
  sed "s|{{FILES_CHANGED}}|$FILES_CHANGED|g" | \
  sed "s|{{ADDITIONS}}|$ADDITIONS|g" | \
  sed "s|{{DELETIONS}}|$DELETIONS|g" | \
  sed "/{{CHANGE_SUMMARY}}/r /dev/stdin" <<< "$(for category in "${!CATEGORIES[@]}"; do echo "- **$category**: $(echo -e "${CATEGORIES[$category]}" | wc -l) files"; done)" | \
  sed "/{{CHANGE_SUMMARY}}/d" | \
  sed "/{{UPDATE_DETAILS}}/r $UPDATE_DETAILS_FILE" | \
  sed "/{{UPDATE_DETAILS}}/d" | \
  sed "/{{VALIDATION_SUMMARY}}/r $VALIDATION_SUMMARY" | \
  sed "/{{VALIDATION_SUMMARY}}/d" \
  > "$PR_BODY_FILE"
```

#### Step 7: Create Pull Request

```bash
# Create PR using GitHub CLI
PR_URL=$(gh pr create \
  --title "docs: Update Claude context documentation" \
  --body-file "$PR_BODY_FILE" \
  --base "$BASE_BRANCH" \
  --head "$BRANCH_NAME")

echo ""
echo "=========================================="
echo "✅ Pull Request Created Successfully!"
echo "=========================================="
echo ""
echo "PR URL: $PR_URL"
echo ""
echo "Next steps:"
echo "1. Review the changes in the PR"
echo "2. Verify documentation accurately reflects code changes"
echo "3. Check validation results"
echo "4. Merge when ready"
echo ""
```

#### Step 8: Return to Original Branch

```bash
# Switch back to original branch
git checkout "$CURRENT_BRANCH"

# Cleanup temporary files
rm -f /tmp/*_$$.txt /tmp/*_$$.json
rm -rf "$BACKUP_DIR"

echo "✅ Workflow complete!"
echo "Returned to branch: $CURRENT_BRANCH"
```

---

## Error Handling

### Edge Case 1: No Changes Detected

If no code changes are detected or no documentation updates are needed:

```bash
if [ "$UPDATE_COUNT" -eq 0 ]; then
  echo "No documentation updates needed."
  echo "Code changes do not significantly impact context files."
  exit 0
fi
```

### Edge Case 2: API Rate Limits

If Claude API rate limits are hit:

```bash
if echo "$ANALYSIS_RESPONSE" | jq -e '.error.type == "rate_limit_error"' > /dev/null 2>&1; then
  echo "⚠️  Rate limit hit. Waiting 60 seconds before retry..."
  sleep 60
  # Retry API call
fi
```

### Edge Case 3: Validation Failures

Validation warnings don't block PR creation but are flagged:

```bash
if ! $MARKDOWN_VALID || ! $YAML_VALID || ! $DIFF_SIZE_OK; then
  echo "⚠️  Some validation checks failed"
  echo "PR will be created with warnings for manual review"
fi
```

### Edge Case 4: Git Conflicts

If base branch has moved ahead:

```bash
# Pull latest changes before creating PR
git fetch origin "$BASE_BRANCH"
if ! git merge origin/"$BASE_BRANCH" --no-commit --no-ff; then
  echo "⚠️  Merge conflicts detected"
  echo "Resolve conflicts manually before creating PR"
  git merge --abort
  exit 1
fi
```

## Success Criteria

This skill succeeds when:

1. All prerequisites are met (git repo, gh auth, API key, tools)
2. Git diff is successfully analyzed and categorized
3. Claude API returns valid JSON with update recommendations
4. High-confidence updates (>= 0.7) are identified
5. Context files are modified correctly (in practice, using Edit tool)
6. Validation checks run without critical errors
7. Git commit is created with detailed message
8. Branch is pushed to remote successfully
9. Pull request is created with comprehensive description
10. User is returned to original branch

## Usage Examples

### Example 1: After Adding New Dagster Agent

```bash
# Added new sentiment_analyzer.py agent
git add macro_agents/src/macro_agents/defs/agents/sentiment_analyzer.py
git commit -m "Add sentiment analyzer agent"

# Run context maintainer
# (In practice: invoke via Claude Code)
# Expected updates:
# - claude.md: Key Components > AI Analysis Agents (ADD agent description)
# - claude.md: Project Structure (ADD file to tree)
```

### Example 2: After Adding New Dependency

```bash
# Added textblob to pyproject.toml
git add macro_agents/pyproject.toml
git commit -m "Add textblob dependency"

# Run context maintainer
# Expected updates:
# - claude.md: Tech Stack > Core Data Platform (ADD dependency)
# - security-reviewer.md: Dependencies section (ADD to checklist)
```

### Example 3: After Adding Environment Variable

```bash
# Added SENTIMENT_API_KEY to .env.example
git add .env.example
git commit -m "Add sentiment API key config"

# Run context maintainer
# Expected updates:
# - claude.md: Environment Variables (ADD variable documentation)
# - security-reviewer.md: Credentials checklist (ADD API key check)
```

## Notes

### LLM-Powered Analysis

The skill uses Claude API to analyze changes intelligently. This is more sophisticated than rule-based matching because:
- Understands semantic meaning of code changes
- Identifies indirect impacts (e.g., new agent → update security checklist)
- Provides confidence scores for each recommendation
- Explains reasoning for each update

### Section Path Matching

Section paths use hierarchical notation:
- "Tech Stack > Core Data Platform"
- "Key Components > 3. AI Analysis Agents"
- "Environment Variables"

In practice, Claude's Edit tool would:
1. Parse markdown structure (h1, h2, h3 headings)
2. Find section matching the hierarchical path
3. Apply the change while preserving formatting

### Validation-First Approach

Running validation before PR creation prevents:
- Broken markdown syntax
- Invalid YAML frontmatter
- Accidentally committed secrets
- Overly large changes that need manual review

### Manual Trigger Benefits

On-demand execution (not automatic) gives developers:
- Control over when documentation is updated
- Opportunity to batch multiple changes
- Ability to review before committing
- Flexibility to skip for minor changes

## Resources

- **Claude API Documentation**: https://docs.anthropic.com/claude/reference/
- **GitHub CLI Manual**: https://cli.github.com/manual/
- **jq Tutorial**: https://jqlang.github.io/jq/tutorial/
- **Git Diff Documentation**: https://git-scm.com/docs/git-diff
- **Markdown Guide**: https://www.markdownguide.org/
