---
name: github-explorer
description: General-purpose GitHub exploration and analysis tool for searching issues, creating PRs, analyzing git history, and reviewing PR comments
license: MIT
---

# GitHub Explorer Skill

## Description

This skill provides interactive GitHub exploration and analysis capabilities using the GitHub CLI (`gh`) and git commands. It enables searching and filtering issues, creating pull requests with custom templates, analyzing file history with git blame, and reviewing PR comments and feedback. Use this skill when you need flexible, exploratory interactions with GitHub repositories.

## Usage

Invoke this skill when you need to:
- Search for issues with specific labels, assignees, or text queries
- List and filter issues across repositories
- Create pull requests with customized content and templates
- Analyze who authored specific lines of code and when
- Review pull request comments and identify actionable feedback
- Investigate code history and track changes to specific functions
- Parse and analyze PR review threads

## Prerequisites

This skill requires the following tools to be installed and configured:

1. **GitHub CLI (`gh`)**: Version 2.0 or higher
   - Check installation: `gh --version`
   - Install: https://cli.github.com/

2. **Git**: Any recent version
   - Check installation: `git --version`

3. **jq**: JSON processor for parsing API responses
   - Check installation: `jq --version`
   - Install: https://jqlang.github.io/jq/download/

4. **GitHub Authentication**: Must be logged in to `gh` CLI
   - Check status: `gh auth status`
   - Login: `gh auth login`

## Instructions

### Feature 1: Search and List Issues

Use this feature to find and explore GitHub issues across repositories.

#### Step 1: List Issues in Current Repository

```bash
# List all open issues in current repository
gh issue list --state open

# List all issues (open and closed)
gh issue list --state all

# Limit results
gh issue list --state open --limit 20
```

#### Step 2: Filter Issues by Labels

```bash
# List issues with a specific label
gh issue list --label "bug" --state open

# List issues with multiple labels (AND logic)
gh issue list --label "bug,high-priority" --state open

# Combine label filters with limits
gh issue list --label "enhancement" --state open --limit 10
```

#### Step 3: Filter Issues by Assignee

```bash
# List issues assigned to you
gh issue list --assignee @me --state open

# List issues assigned to a specific user
gh issue list --assignee username --state open

# Combine assignee and label filters
gh issue list --assignee @me --label "bug,high-priority" --state open
```

#### Step 4: Search Issues with Text Queries

```bash
# Search issues in current repository
gh search issues "authentication error"

# Search issues in a specific repository
gh search issues "docker configuration" --repo owner/repo

# Search across an organization
gh search issues "api timeout" --owner orgname

# Combine search with filters
gh search issues "memory leak" --state open --label "bug" --limit 10
```

#### Step 5: View Issue Details

```bash
# View a specific issue
gh issue view 123

# View issue with comments
gh issue view 123 --comments

# Get issue JSON for parsing
gh issue view 123 --json title,body,state,labels,assignees,createdAt
```

#### Step 6: Parse and Format Issue Data

```bash
# Get issues as JSON and parse with jq
gh issue list --json number,title,state,labels,assignees,createdAt --limit 10 | \
  jq -r '.[] | "\(.number): \(.title) [\(.state)]"'

# Create a table of issues
gh issue list --json number,title,assignees --limit 20 | \
  jq -r '.[] | "\(.number)\t\(.title)\t\(.assignees[0].login // "unassigned")"'

# Count issues by label
gh issue list --state all --json labels --jq '[.[].labels[].name] | group_by(.) | map({label: .[0], count: length}) | sort_by(.count) | reverse'
```

### Feature 2: Create Pull Requests

Use this feature to create pull requests with custom content and templates.

#### Step 1: Review Current Branch Changes

Before creating a PR, review what changes will be included:

```bash
# Show files changed compared to main
git diff main...HEAD --stat

# Show detailed diff
git diff main...HEAD

# Show commit messages that will be in the PR
git log main...HEAD --oneline

# Show all commits with details
git log main...HEAD --pretty=fuller
```

#### Step 2: Create Basic Pull Request

```bash
# Create PR with auto-filled title and body from commits
gh pr create --fill

# Create PR with custom title and body
gh pr create --title "Fix authentication bug" --body "This PR fixes the token validation issue"

# Create PR targeting a specific base branch
gh pr create --base develop --head feature-branch
```

#### Step 3: Create PR with Custom Template

```bash
# Create PR with formatted body using heredoc
gh pr create --title "Add user profile feature" --body "$(cat <<'EOF'
## Summary
- Implemented user profile page
- Added avatar upload functionality
- Created profile editing form

## Changes Made
- New UserProfile component
- Profile API endpoints
- Avatar storage in GCS

## Related Issues
Fixes #42
Relates to #38

## Testing
- [ ] Manual testing completed
- [ ] Unit tests added
- [ ] Integration tests passing

🤖 Generated with an agent-assisted workflow.
EOF
)"
```

#### Step 4: Create Draft or WIP Pull Requests

```bash
# Create draft PR (not ready for review)
gh pr create --draft --title "WIP: Refactor authentication"

# Create normal PR that can be reviewed immediately
gh pr create --title "Fix login bug" --body "Ready for review"
```

#### Step 5: Link PR to Issues

```bash
# Link PR to issue with "Fixes" keyword
gh pr create --title "Fix memory leak" --body "Fixes #123"

# Link to multiple issues
gh pr create --title "Bug fixes" --body "$(cat <<'EOF'
## Summary
Multiple bug fixes

## Issues
Fixes #123
Fixes #124
Relates to #125
EOF
)"
```

#### Step 6: Advanced PR Creation

```bash
# Get repository info for PR context
REPO_URL=$(gh repo view --json url --jq .url)
CURRENT_BRANCH=$(git branch --show-current)

# Auto-generate PR body from commits
COMMITS=$(git log main...HEAD --pretty=format:"- %s")

gh pr create --title "Feature: $(git log -1 --pretty=%s)" --body "$(cat <<EOF
## Changes in this PR

$COMMITS

## Branch
\`$CURRENT_BRANCH\`

## Repository
$REPO_URL

🤖 Generated with an agent-assisted workflow.
EOF
)"
```

### Feature 3: Git Blame Analysis

Use this feature to analyze file history and identify code authors.

#### Step 1: Basic Git Blame

```bash
# Blame entire file
git blame path/to/file.py

# Blame specific line range
git blame -L 50,100 path/to/file.py

# Blame with date information
git blame -L 50,100 path/to/file.py --date=short
```

#### Step 2: Enhanced Blame Output

```bash
# Show email addresses instead of names
git blame --show-email path/to/file.py

# Show full commit hash
git blame -L 10,20 path/to/file.py --abbrev=40

# Ignore whitespace changes
git blame -w path/to/file.py

# Follow file renames
git blame --follow path/to/file.py
```

#### Step 3: Analyze File History

```bash
# Show detailed history of specific line range
git log -L 50,100:path/to/file.py --pretty=fuller --patch

# Find when a function was added or modified
git log -S "function_name" --source --all --pretty=fuller path/to/file.py

# Show commits that modified a file
git log --follow --oneline path/to/file.py

# Show who contributed most to a file
git blame path/to/file.py | awk '{print $2}' | sort | uniq -c | sort -nr
```

#### Step 4: Link Blame to GitHub Commits

```bash
# Get commit hash from specific line
COMMIT_HASH=$(git blame -L 50,50 path/to/file.py | awk '{print $1}')

# Get repository URL
REPO_URL=$(gh repo view --json url --jq .url)

# Create GitHub commit URL
echo "$REPO_URL/commit/$COMMIT_HASH"

# Open commit in browser (if on macOS/Linux with `open` or `xdg-open`)
# open "$REPO_URL/commit/$COMMIT_HASH"
```

#### Step 5: Format Blame Output

```bash
# Clean blame output with just author and line
git blame -L 10,20 path/to/file.py | \
  awk '{printf "Line %s: %s %s %s\n", substr($0, index($0, ")") + 1, 4), $2, $3, $4}'

# Create CSV of blame data
echo "Line,Author,Date,Commit" > blame_output.csv
git blame -L 1,100 path/to/file.py --date=short | \
  awk '{print NR "," $2 "," $3 "," $1}' >> blame_output.csv
```

#### Step 6: Advanced History Analysis

```bash
# Find when specific text was added
git log -S "critical_function" --source --all -p path/to/file.py

# Show file changes over time
git log --stat path/to/file.py

# Show who last modified each line (concise format)
git blame path/to/file.py | awk '{print $2, $3}' | sort | uniq -c | sort -nr

# Compare current version with specific commit
COMMIT=$(git blame -L 50,50 path/to/file.py | awk '{print $1}')
git diff $COMMIT path/to/file.py
```

### Feature 4: Analyze PR Comments

Use this feature to analyze pull request review comments and feedback.

#### Step 1: View PR with Comments

```bash
# View PR details with all comments
gh pr view 123 --comments

# View specific PR sections
gh pr view 123 --json title,body,state,comments
```

#### Step 2: Check PR Review Status

```bash
# View PR checks (CI/CD status)
gh pr checks 123

# View PR reviews
gh pr reviews 123

# Get review decision
gh pr view 123 --json reviewDecision
```

#### Step 3: Get Detailed Review Comments

```bash
# Get all review comments as JSON
gh api repos/:owner/:repo/pulls/123/comments | jq '.'

# Parse comments with jq
gh api repos/:owner/:repo/pulls/123/comments --jq '.[] | {
  author: .user.login,
  body: .body,
  path: .path,
  line: .line,
  created: .created_at
}'

# Filter comments by file
gh api repos/:owner/:repo/pulls/123/comments --jq '.[] | select(.path == "src/main.py") | {author: .user.login, line: .line, body: .body}'
```

#### Step 4: Analyze Conversation Threads

```bash
# Get all review threads
gh api repos/:owner/:repo/pulls/123/reviews --jq '.[] | {
  author: .user.login,
  state: .state,
  body: .body,
  submitted: .submitted_at
}'

# Find unresolved conversations
gh pr view 123 --json reviews --jq '.reviews[] | select(.state == "CHANGES_REQUESTED")'

# Count reviews by state
gh pr view 123 --json reviews --jq '.reviews | group_by(.state) | map({state: .[0].state, count: length})'
```

#### Step 5: Track Comment Authors

```bash
# Count comments by author
gh api repos/:owner/:repo/pulls/123/comments --jq 'group_by(.user.login) | map({author: .[0].user.login, count: length})'

# List unique commenters
gh api repos/:owner/:repo/pulls/123/comments --jq '[.[].user.login] | unique'

# Find most active reviewer
gh api repos/:owner/:repo/pulls/123/comments --jq 'group_by(.user.login) | map({author: .[0].user.login, count: length}) | sort_by(.count) | reverse | .[0]'
```

#### Step 6: Identify Actionable Feedback

```bash
# Find all "CHANGES_REQUESTED" reviews
gh pr reviews 123 --json author,state,body | \
  jq '.[] | select(.state == "CHANGES_REQUESTED")'

# Get list of files with comments
gh api repos/:owner/:repo/pulls/123/comments --jq '[.[].path] | unique'

# Create summary of comments by file
gh api repos/:owner/:repo/pulls/123/comments --jq 'group_by(.path) | map({file: .[0].path, comment_count: length})'

# View PR diff for context
gh pr diff 123

# View PR diff for specific file
gh pr diff 123 -- path/to/file.py
```

## Error Handling

### Authentication Errors

If you encounter authentication issues:

```bash
# Check authentication status
gh auth status

# Re-authenticate if needed
gh auth login

# Test API access
gh api user

# Verify you have access to the repository
gh repo view
```

### Rate Limiting

GitHub API has rate limits. If you encounter rate limit errors:

```bash
# Check current rate limit status
gh api rate_limit

# View rate limit details
gh api rate_limit --jq '.resources.core | {
  limit: .limit,
  remaining: .remaining,
  reset: .reset,
  reset_time: (.reset | strftime("%Y-%m-%d %H:%M:%S"))
}'

# Wait if rate limit is reached (reset time shown in output above)
```

Rate limits:
- Authenticated requests: 5,000 per hour
- Unauthenticated requests: 60 per hour
- GraphQL API: 5,000 points per hour

### Repository Context Errors

If commands fail because you're not in a git repository:

```bash
# Check if in a git repository
if ! git rev-parse --is-inside-work-tree 2>/dev/null; then
  echo "Error: Not in a git repository"
  echo "Navigate to a git repository or initialize one with: git init"
  exit 1
fi

# Get repository information
gh repo view --json nameWithOwner,url

# Clone repository if needed
gh repo clone owner/repo
```

### Missing Issues or PRs

If an issue or PR doesn't exist:

```bash
# Check if issue exists before viewing
if gh issue view 123 2>/dev/null; then
  gh issue view 123
else
  echo "Issue #123 not found"
  echo "Available issues:"
  gh issue list --limit 10
fi

# List available PRs
gh pr list --state all --limit 20
```

### Permission Errors

If you lack permissions for certain operations:

1. **For PR creation**: Ensure you have write access to the repository
2. **For issue access**: Check repository visibility (private vs public)
3. **For API operations**: Verify your authentication scope

```bash
# Check repository permissions
gh api repos/:owner/:repo | jq '.permissions'

# Re-authenticate with additional scopes if needed
gh auth refresh -s repo,read:org
```

## Success Criteria

This skill succeeds when:

1. GitHub CLI is properly installed and authenticated (`gh auth status` shows active login)
2. Issue searches return relevant, filtered results matching the specified criteria
3. PR creation completes successfully with properly formatted title and body
4. Git blame analysis identifies correct authors and timestamps for code sections
5. PR comment analysis retrieves and parses all review threads and feedback
6. All bash commands execute without errors
7. JSON parsing with `jq` produces readable, formatted output
8. Rate limits are monitored and respected
9. Repository context is verified before operations that require it
10. Error messages are clear and provide actionable next steps

## Notes

### Best Practices

1. **Always authenticate first**: Run `gh auth login` and `gh auth status` before using this skill
2. **Use JSON output for parsing**: Add `--json` flags to `gh` commands when you need structured data
3. **Parse with jq**: Use `jq` for filtering and formatting JSON API responses
4. **Verify repository context**: Check you're in the correct repository with `gh repo view`
5. **Monitor rate limits**: Check `gh api rate_limit` if you're making many requests
6. **Test commands incrementally**: Start with simple commands before building complex queries

### GitHub API Rate Limits

Be mindful of GitHub's API rate limits:
- **5,000 requests/hour** for authenticated users
- **60 requests/hour** for unauthenticated requests
- Use `gh api rate_limit` to check current usage
- The `gh` CLI automatically handles authentication

### Complementary with git-pr-workflow Skill

This skill complements the existing `git-pr-workflow` skill:

- **Use github-explorer when**: You need to explore issues, analyze code history, review PR feedback, or manually craft custom PRs
- **Use git-pr-workflow when**: You want fully automated PR creation, CI monitoring, and auto-merge

Example combined workflow:
1. Use `github-explorer` to find bugs: `gh issue list --label "bug"`
2. Fix the bugs in your code
3. Use `git-pr-workflow` to automatically create a PR, monitor CI, and merge

### Working with Multiple Repositories

To work across different repositories:

```bash
# Specify repository explicitly
gh issue list --repo owner/repo
gh pr list --repo owner/other-repo

# Clone and switch to different repo
gh repo clone owner/repo
cd repo

# Search across organization
gh search issues "bug" --owner orgname
```

### Output Formatting Tips

For better readability:
- Use `--json` with `jq` for structured data
- Pipe to `grep` for text filtering
- Use `column -t` for table formatting
- Export to CSV for spreadsheet analysis

## Resources

- **GitHub CLI Manual**: https://cli.github.com/manual/
- **GitHub REST API Documentation**: https://docs.github.com/en/rest
- **GitHub CLI Authentication**: https://cli.github.com/manual/gh_auth_login
- **jq Tutorial**: https://jqlang.github.io/jq/tutorial/
- **jq Manual**: https://jqlang.github.io/jq/manual/
- **Git Blame Documentation**: https://git-scm.com/docs/git-blame
- **Git Log Documentation**: https://git-scm.com/docs/git-log
- **GitHub API Rate Limiting**: https://docs.github.com/en/rest/overview/rate-limits-for-the-rest-api
