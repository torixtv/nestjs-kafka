# Agent instructions

<!-- greptile-rounds:start. The same block lives in AGENTS.md (Codex and other agents) and CLAUDE.md (Claude Code) in every torixtv repository; change both together. -->
## Greptile review rounds

Greptile reviews a pull request once, when it is opened ready for review or marked ready. It does
not review pushes. Every further review costs $1 and starts only when someone comments
`@greptileai` on the PR. A reviewed PR is ready to merge when Greptile's summary comment shows
5/5, no Greptile thread is left without a reply, and the summary's "Last reviewed commit" is the
PR's head commit. A PR labelled `skip-greptile` gets no summary and waits for none: it merges on
green CI and the usual human review. Measured over the month to 2026-10-23: 91% of repeat reviews
came from pushes to PRs that were already open for review, and a quarter of PRs took three or more
reviews. These rules replace that with rounds you start on purpose.

1. Open the PR as a draft and keep it a draft while you work. Mark it ready only when the change is
   complete and CI is green. That starts round 1.
2. A PR that needs no review (docs only, lockfiles only, a back-merge, a revert) gets the
   `skip-greptile` label before it is marked ready.
3. While a review is running (the "Greptile Review" check is in progress), do not push and do not
   comment `@greptileai`.
4. When a review lands, read every finding in the summary before changing anything. Fix the whole
   set, run the repository's checks, and push. Reply on every Greptile thread, including findings
   you decline, saying what changed or why not.
5. Then comment `@greptileai` once to start the next round. Never request a round while CI is red,
   while a Greptile thread has no reply, or when nothing changed since the last review.
6. Stop when the summary shows 5/5 with no open threads on the head commit. Do not push after that
   unless the change needs it: any push means another round before merge.
7. If round 3 still has findings, stop and ask a person before requesting a fourth.
8. Never write `at plus` or `at apex` in a request (3 and 10 credits), never run `/greploop`, and
   never close and reopen a PR to get a review.
9. Write the mention in no comment but the one request: not in a thread reply, a review or a PR
   description, not even quoted in backticks. Greptile reads any comment that carries it as a
   request, tier words included. On 2026-10-24 six replies that quoted the tier wording to explain
   a fix each bought an unplanned apex review.

`node .claude/hooks/greptile-round-guard.mjs status [torixtv/<repo> <number>]` checks a PR against
these rules and prints what to do next; any agent can run it. In Claude Code the `greptile-round`
skill wraps it, and a PreToolUse hook in `.claude/settings.json` blocks an `@greptileai` comment
that breaks them.
<!-- greptile-rounds:end -->
