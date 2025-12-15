# Git PR Workflow Skill

## Description
Automates the complete git and GitHub workflow: initializes a git repository, creates a private GitHub repository if needed, commits changes, creates a pull request, monitors GitHub Actions for completion, and merges the PR if all checks pass.

## Usage
Invoke this skill when you need to:
- Initialize a new git repository and push to GitHub
- Create a pull request with automatic CI/CD verification
- Wait for GitHub Actions to complete before merging
- Automatically merge PRs that pass all checks

## Instructions

You are an expert at git and GitHub workflows. Your task is to automate the complete process of initializing a repository, creating a PR, and merging it after CI passes.

### Step 1: Initialize Git Repository (if needed)

First, check if the current directory is already a git repository:

```bash
git rev-parse --is-inside-work-tree 2>/dev/null
```

If not a git repository, initialize it:

```bash
git init
```

### Step 2: Check for GitHub Remote

Check if a GitHub remote already exists:

```bash
git remote -v
```

If no remote exists, check if a GitHub repository exists for this project:

```bash
gh repo view --json name,owner 2>&1
```

### Step 3: Create GitHub Repository (if needed)

If the repository doesn't exist on GitHub, create a new private repository:

```bash
gh repo create $(basename "$PWD") --private --source=. --remote=origin --push
```

This command:
- Creates a private repository with the current directory name
- Sets the current directory as the source
- Adds the remote as 'origin'
- Pushes the initial commit

If the repository already exists but no remote is configured:

```bash
gh repo view --json nameWithOwner --jq .nameWithOwner | xargs -I {} git remote add origin "https://github.com/{}.git"
```

### Step 4: Ensure Main Branch Exists

Make sure we have an initial commit on main:

```bash
git add .
git commit -m "Initial commit" || echo "Already has commits"
git branch -M main
git push -u origin main --force
```

### Step 5: Determine Base Commit

Before creating a feature branch, determine the appropriate base commit:

```bash
# If changes are already committed on main, find the commit before them
LAST_COMMIT=$(git log --oneline -1 --format=%H)
BASE_COMMIT=$(git log --oneline -2 --format=%H | tail -1)

# Otherwise use the current HEAD
if [ -z "$BASE_COMMIT" ]; then
  BASE_COMMIT="HEAD"
fi
```

### Step 6: Create Feature Branch

Create a new feature branch from the base commit:

```bash
BRANCH_NAME="feature/automated-changes-$(date +%s)"
git checkout -b "$BRANCH_NAME" "$BASE_COMMIT"
```

### Step 7: Apply Changes

If changes were already committed on main, cherry-pick them:

```bash
# Check if we need to cherry-pick
if git rev-parse --verify "$LAST_COMMIT" >/dev/null 2>&1 && [ "$LAST_COMMIT" != "$BASE_COMMIT" ]; then
  echo "Cherry-picking changes from main..."
  git cherry-pick "$LAST_COMMIT"
else
  # Otherwise stage and commit new changes
  git add .
  git commit -m "Add automated changes

🤖 Generated with [Claude Code](https://claude.com/claude-code)

Co-Authored-By: Claude <noreply@anthropic.com>"
fi
```

### Step 8: Push Branch

Push the feature branch to GitHub:

```bash
git push -u origin "$BRANCH_NAME"
```

### Step 9: Create Pull Request

Create a pull request using the gh CLI:

```bash
PR_URL=$(gh pr create \
  --title "Automated changes" \
  --body "$(cat <<'EOF'
## Summary
Automated changes created via Claude Code skill

## Changes
- Repository initialized and configured
- All current changes committed

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)" \
  --base main \
  --head "$BRANCH_NAME" | grep -o 'https://github.com[^[:space:]]*')

echo "PR created: $PR_URL"
```

### Step 10: Extract PR Number

Extract the PR number from the URL:

```bash
PR_NUMBER=$(echo "$PR_URL" | grep -oE '[0-9]+$')
echo "PR Number: $PR_NUMBER"
```

### Step 11: Monitor GitHub Actions

Wait for GitHub Actions to complete. First, wait a few seconds for checks to start:

```bash
echo "Waiting for checks to start..."
sleep 10
```

Then monitor the checks status:

```bash
MAX_WAIT=600  # 10 minutes
ELAPSED=0
CHECK_INTERVAL=15

while [ $ELAPSED -lt $MAX_WAIT ]; do
  echo "Checking PR status... (${ELAPSED}s elapsed)"

  # Get the checks status
  CHECKS_STATUS=$(gh pr checks "$PR_NUMBER" --json state,conclusion,name)

  # Check if all checks have completed
  PENDING=$(echo "$CHECKS_STATUS" | jq '[.[] | select(.state == "PENDING" or .state == "IN_PROGRESS")] | length')

  if [ "$PENDING" -eq 0 ]; then
    echo "All checks completed!"

    # Check if any checks failed
    FAILED=$(echo "$CHECKS_STATUS" | jq '[.[] | select(.conclusion == "FAILURE" or .conclusion == "CANCELLED")] | length')

    if [ "$FAILED" -gt 0 ]; then
      echo "❌ Some checks failed:"
      echo "$CHECKS_STATUS" | jq -r '.[] | select(.conclusion == "FAILURE" or .conclusion == "CANCELLED") | "  - \(.name): \(.conclusion)"'
      exit 1
    fi

    echo "✅ All checks passed!"
    break
  fi

  echo "Checks still running: $PENDING pending"
  sleep $CHECK_INTERVAL
  ELAPSED=$((ELAPSED + CHECK_INTERVAL))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
  echo "⏱️  Timeout waiting for checks to complete"
  exit 1
fi
```

### Step 12: Merge Pull Request

If all checks pass, merge the PR:

```bash
gh pr merge "$PR_NUMBER" --auto --squash --delete-branch
echo "✅ PR merged successfully!"
```

### Step 13: Switch Back to Main

Finally, switch back to the main branch and pull the merged changes:

```bash
git checkout main
git pull origin main
echo "✅ Workflow complete! All changes merged to main."
```

## Error Handling

Throughout the process:
- Check return codes after each command
- Provide clear error messages if something fails
- If GitHub Actions fail, display which checks failed and why
- If the merge fails, explain what went wrong

## Success Criteria

The skill succeeds when:
1. Git repository is initialized (or already exists)
2. GitHub repository exists and is accessible
3. Changes are committed to a feature branch
4. Pull request is created
5. All GitHub Actions checks pass
6. PR is successfully merged to main
7. Local main branch is updated

## Notes

- The repository created will be private by default
- The skill waits up to 10 minutes for GitHub Actions to complete
- If checks don't start within the timeout period, the skill will fail
- The feature branch is automatically deleted after merge
