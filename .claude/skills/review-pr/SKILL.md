---
name: review-pr
description: >-
  Review a PR on cloudserver (S3-compatible object
  storage server in Node.js)
argument-hint: <pr-number-or-url>
disable-model-invocation: true
allowed-tools: >-
  Bash(gh repo view *), Bash(gh pr view *),
  Bash(gh pr diff *), Bash(gh pr comment *),
  Bash(gh api *), Bash(git diff *),
  Bash(git log *), Bash(git show *)
---

# Review GitHub PR

You are an expert code reviewer. Review this PR: $ARGUMENTS

## Determine PR target

Parse `$ARGUMENTS` to extract the repo and PR number:

- If arguments contain `REPO:` and `PR_NUMBER:` (CI mode), use those
  values directly.
- If the argument is a GitHub URL (starts with `https://github.com/`),
  extract `owner/repo` and the PR number from it.
- If the argument is just a number, use the current repo from
  `gh repo view --json nameWithOwner -q .nameWithOwner`.

## Output mode

- **CI mode** (arguments contain `REPO:` and `PR_NUMBER:`): post inline
  comments and summary to GitHub.
- **Local mode** (all other cases): output the review as text directly.
  Do NOT post anything to GitHub.

## Steps

1. **Fetch PR details:**

    ```bash
    gh pr view <number> --repo <owner/repo> \
      --json title,body,headRefOid,author,files
    gh pr diff <number> --repo <owner/repo>
    ```

2. **Read changed files** to understand the full context around each
   change (not just the diff hunks).

3. **Analyze the changes** against these criteria:

    - **Async error handling** — uncaught promise rejections, missing
      error callbacks, swallowed errors in streams. Double callbacks
      in try/catch blocks (callback called in try then again in catch).
    - **Async/await migration** — when code is migrated from callbacks
      to async/await, verify: no leftover `callback` or `next` params,
      no mixed callback + promise patterns, proper try/catch around
      awaited calls, errors are re-thrown or handled (not silently
      swallowed). Watch for the anti-pattern:
      `try { cb(); } catch(err) { cb(err); }` where an exception after
      the first `cb()` triggers a second call.
    - **S3/API contract** — breaking changes to request/response
      formats, new error codes, header handling, missing XML response
      fields.
    - **Dependency pinning** — git-based deps (arsenal, vaultclient,
      bucketclient, werelogs, utapi, scubaclient) must pin to a tag,
      not a branch.
    - **Logging** — proper use of werelogs logger, no `console.log` in
      production code, log levels match severity.
    - **Stream handling** — backpressure, proper cleanup on error
      (`.destroy()`), no leaked file descriptors, missing error event
      handlers.
    - **Config changes** — backward compatibility with existing env
      vars and `config.json`, default values.
    - **Security** — command injection, header injection, XML external
      entity attacks, path traversal, SSRF in multi-backend requests.
    - **Breaking changes** — changes to public S3 API behavior,
      metadata schema changes, env var renames without backward compat.
    - **Test quality** — no `.only()` tests (eslint enforces this),
      assertions match the behavior being tested, `require()`/`import`
      at top of file (never inside `describe` or functions).

4. **Deliver your review:**

### If CI mode: post to GitHub

#### Part A: Inline file comments

For each specific issue, post a comment on the exact file and line:

```bash
gh api -X POST \
  -H "Accept: application/vnd.github+json" \
  "repos/<owner/repo>/pulls/<number>/comments" \
  -f body="Your comment<br><br>— Claude Code" \
  -f path="path/to/file" \
  -F line=<line_number> \
  -f side="RIGHT" \
  -f commit_id="<headRefOid>"
```

**The command must stay on a single bash line.** Never use newlines in
bash commands — use `<br>` for line breaks in comment bodies. Never put
`<br>` inside code blocks or suggestion blocks.

Each inline comment must:

- Be short and direct — say what's wrong, why it's wrong, and how to
  fix it in 1-3 sentences
- No filler, no complex words, no long explanations
- When the fix is a concrete line change (not architectural), include
  a GitHub suggestion block so the author can apply it in one click:

  ````text
  ```suggestion
  corrected-line-here
  ```
  ````

  Only suggest when you can show the exact replacement. For
  architectural or design issues, just describe the problem.
  Example with a suggestion block:

  ```bash
  gh api ... -f body=$'Missing the update.\
  <br><br>\n```suggestion\n\
  /plugin update shared-guidelines@hub\n\
  /plugin update scality-skills@hub\n\
  ```\n<br><br>— Claude Code' ...
  ```

- When the comment contains a suggestion block, use `$'...'` quoting
  with `\n` for code fence boundaries. Escape single quotes as `\'`
  (e.g., `don\'t`)
- End with: `— Claude Code`

Use the line number from the **new version** of the file (the line
number you'd see after the PR is merged), which corresponds to the
`line` parameter in the GitHub API.

#### Part B: Summary comment

```bash
gh pr comment <number> --repo <owner/repo> \
  --body "LGTM<br><br>Review by Claude Code"
```

**The command must stay on a single bash line.** Never use newlines in
bash commands — use `<br>` for line breaks in comment bodies.

Do not describe or summarize the PR. For each issue, state the problem
on one line, then list one or more suggestions below it:

```text
- <issue>
  - <suggestion>
  - <suggestion>
```

If no issues: just say "LGTM". End with: `Review by Claude Code`

### If local mode: output the review as text

Do NOT post anything to GitHub. Instead, output the review directly
as text.

For each issue found, output:

```text
**<file_path>:<line_number>** — <what's wrong and how to fix it>
```

When the fix is a concrete line change, include a fenced code block
showing the suggested replacement.

At the end, output a summary section listing all issues. If no issues:
just say "LGTM".

End with: `Review by Claude Code`

## What NOT to do

- Do not comment on markdown formatting preferences
- Do not suggest refactors unrelated to the PR's purpose
- Do not praise code — only flag problems or stay silent
- If no issues are found, post only a summary saying "LGTM"
- Do not flag style issues already covered by the project's linter
  (eslint, biome, pylint, golangci-lint)
