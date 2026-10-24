---
name: greptile-round
description: Use when a pull request is being prepared for review, when a Greptile review lands, before commenting @greptileai, or when deciding whether a PR is ready to merge. Checks the PR against the Greptile review-round rules (draft until ready, one request per round, 5/5 on the head commit) and says what to do next.
---

# Greptile review rounds

The rules are in the "Greptile review rounds" section of this repository's `AGENTS.md` / `CLAUDE.md`. This skill applies them to one PR.

## 1. Check where the PR stands

```bash
node .claude/hooks/greptile-round-guard.mjs status                       # this branch's PR
node .claude/hooks/greptile-round-guard.mjs status torixtv/<repo> <number>
```

It prints CI on the head commit, Greptile's latest score and review count, whether the last review covered the head commit, any Greptile thread with no reply, and a verdict.

## 2. Act on the verdict

| Verdict | Meaning | Do |
|---|---|---|
| `blocked: the PR is a draft` | Greptile has not been asked yet | Finish the change and get CI green, then mark the PR ready. That starts round 1. A docs-only, lockfile-only, back-merge or revert PR gets the `skip-greptile` label first. |
| `blocked: a Greptile review is running` | A round is in progress | Wait. Do not push and do not comment. |
| `blocked: N Greptile thread(s) have no reply` | Findings are open | Read every finding in the summary, fix the whole set, run the checks, push, then reply on every thread (fixed in `<sha>`, or why not). |
| `blocked: CI is red` / `CI is still running` | The head is not green | Fix or wait. Never ask for a round on a red head. |
| `blocked: nothing changed since the last review` | The last review already covers the head | Push the fixes first. |
| `blocked: this PR has had N reviews` | Round 3 is used up | Stop and ask a person. They can post `@greptileai` themselves or add the `greptile-extra-round` label. |
| `request-ok` | The round is fixed, pushed, answered and green | Comment `@greptileai` once, plain: never `plus`, `apex` or `/greploop`. |
| `done` | 5/5 on the head commit, every thread answered | Stop. Do not push unless the change needs it: any push means another round. |
| `skipped` | Labelled `skip-greptile` | It merges on green CI and the usual human review. |

## 3. The hook

`.claude/settings.json` runs the same script as a PreToolUse hook. It blocks an `@greptileai` comment, made through `gh` or the GitHub MCP tools, whenever the verdict is not `request-ok`, and its message gives the reason. It only reads GitHub. If GitHub cannot be read it lets the comment through and says the PR was not checked, so the rules above still apply.

Codex and other agents do not run the hook. They follow the same rules from `AGENTS.md` and can run the status command themselves.
