// node --test .claude/hooks/greptile-round-guard.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { bodyFiles, decide, evaluateHook, forbiddenRequest, gather, parseSummary, targetOf, unansweredThreads, repoFromRemote } from './greptile-round-guard.mjs';

const HEAD = '1b549eef4b87c2aea143eed843be568d4846809a';
const OLD = 'd18c44ff9ad8177d6bcc4c6131749e4e58e6a14c';
const SUMMARY = (score, reviews, sha) =>
  `<!-- greptile_summary -->\n<h2>…Confidence Score: ${score}/5</h2>\n…\n<sub>Reviews (${reviews}) · Last reviewed commit: ["docs: …"](https://github.com/torixtv/catalog-service/commit/${sha}) · [Reviewed by Greptile](https://www.greptile.com/)</sub>`;
const green = [{ name: 'test', status: 'completed', conclusion: 'success' }];
const pr = (extra = {}) => ({ state: 'open', draft: false, headSha: HEAD, labels: [], ...extra });
const state = (extra = {}) => ({ pr: pr(), checks: green, summary: parseSummary(SUMMARY(4, 2, OLD)), unanswered: 0, ...extra });

test('parseSummary reads score, review count and last reviewed commit', () => {
  assert.deepEqual(parseSummary(SUMMARY(5, 12, OLD)), { score: 5, reviews: 12, lastReviewedSha: OLD });
  assert.equal(parseSummary(''), null);
});

test('a fixed, pushed, answered round may be requested', () => {
  assert.equal(decide(state()).verdict, 'request-ok');
  // A first review that finished without posting a summary does not hold the PR forever.
  const done = { name: 'Greptile Review', status: 'completed', conclusion: 'failure' };
  assert.equal(decide(state({ summary: null, checks: [...green, done] })).verdict, 'request-ok');
});

test('each rule blocks on its own', () => {
  const cases = {
    draft: { pr: pr({ draft: true }) },
    closed: { pr: pr({ state: 'closed' }) },
    'red CI': { checks: [{ name: 'test', status: 'completed', conclusion: 'failure' }] },
    'CI running': { checks: [{ name: 'test', status: 'in_progress', conclusion: null }] },
    'review running': { checks: [...green, { name: 'Greptile Review', status: 'in_progress', conclusion: null }] },
    'unanswered thread': { unanswered: 1 },
    'nothing changed': { summary: parseSummary(SUMMARY(4, 2, HEAD)) },
    'round limit': { summary: parseSummary(SUMMARY(4, 3, OLD)) },
    'first review not posted yet': { summary: null },
  };
  for (const [name, extra] of Object.entries(cases)) {
    assert.equal(decide(state(extra)).verdict, 'blocked', name);
  }
});

test('5/5 on the head with no open thread is done, skip-greptile is skipped, the extra-round label lifts the limit', () => {
  assert.equal(decide(state({ summary: parseSummary(SUMMARY(5, 4, HEAD)) })).verdict, 'done');
  assert.equal(decide(state({ pr: pr({ labels: ['skip-greptile'] }) })).verdict, 'skipped');
  assert.equal(decide(state({ pr: pr({ labels: ['greptile-extra-round'] }), summary: parseSummary(SUMMARY(4, 5, OLD)) })).verdict, 'request-ok');
});

test('a bot reply does not answer a thread, a person reply does', () => {
  const root = { id: 1, user: { login: 'greptile-apps[bot]', type: 'Bot' } };
  assert.equal(unansweredThreads([root, { id: 2, in_reply_to_id: 1, user: { login: 'coderabbitai[bot]', type: 'Bot' } }]).length, 1);
  assert.equal(unansweredThreads([root, { id: 3, in_reply_to_id: 1, user: { login: 'danshapir', type: 'User' } }]).length, 0);
});

test('targetOf finds the PR in MCP and gh calls and ignores everything else', () => {
  const mcp = { tool_name: 'mcp__github__add_issue_comment', tool_input: { owner: 'torixtv', repo: 'x', issue_number: 7, body: '@greptileai' } };
  assert.deepEqual(targetOf(mcp, null), { repo: 'torixtv/x', number: 7 });
  assert.deepEqual(targetOf({ tool_name: 'Bash', tool_input: { command: 'gh pr comment 12 --body "@greptileai"' } }, 'torixtv/y'), { repo: 'torixtv/y', number: 12 });
  assert.deepEqual(targetOf({ tool_name: 'Bash', tool_input: { command: 'gh api repos/torixtv/z/issues/3/comments -f body=@greptileai' } }, null), { repo: 'torixtv/z', number: 3 });
  assert.equal(targetOf({ tool_name: 'Bash', tool_input: { command: "grep -n '@greptileai' AGENTS.md" } }, null), null);
  assert.equal(targetOf({ tool_name: 'mcp__github__create_pull_request', tool_input: { body: '@greptileai' } }, null), null);
  assert.equal(repoFromRemote('git@github.com:torixtv/backoffice.git'), 'torixtv/backoffice');
});

test('evaluateHook blocks plus/apex and unresolvable targets, and fails open when GitHub cannot be read', () => {
  const call = (command) => ({ tool_name: 'Bash', tool_input: { command } });
  assert.equal(evaluateHook(call('gh pr comment 1 --repo torixtv/x --body "@greptileai plus"')).block, true);
  assert.equal(evaluateHook(call('gh pr comment --body "@greptileai"')).block, true);
  assert.equal(evaluateHook(call('gh pr comment 1 --repo torixtv/x --body /greploop')).block, true);
  assert.equal(evaluateHook(call("grep -n '/greploop' AGENTS.md")).block, false);
  const failing = () => { throw new Error('gh: not found'); };
  const r = evaluateHook(call('gh pr comment 1 --repo torixtv/x --body "@greptileai"'), { api: failing });
  assert.equal(r.block, false);
  assert.match(r.message, /not checked/);
  assert.equal(evaluateHook(call('echo hello')).block, false);
});

test('evaluateHook runs the rules against GitHub data', () => {
  const api = (path) => {
    if (path === 'repos/torixtv/x/pulls/9') return { state: 'open', draft: false, head: { sha: HEAD }, labels: [] };
    if (path.startsWith(`repos/torixtv/x/commits/${HEAD}/check-runs`)) return { check_runs: green };
    if (path === `repos/torixtv/x/commits/${HEAD}/status`) return { statuses: [] };
    if (path.startsWith('repos/torixtv/x/issues/9/comments')) return [{ user: { login: 'greptile-apps[bot]' }, body: SUMMARY(4, 1, OLD) }];
    if (path.startsWith('repos/torixtv/x/pulls/9/comments')) return [{ id: 5, user: { login: 'greptile-apps[bot]', type: 'Bot' } }];
    throw new Error(`unexpected ${path}`);
  };
  const r = evaluateHook({ tool_name: 'mcp__github__add_issue_comment', tool_input: { owner: 'torixtv', repo: 'x', issue_number: 9, body: '@greptileai' } }, { api });
  assert.equal(r.block, true);
  assert.match(r.message, /1 Greptile thread\(s\) have no reply/);
});

test('the mention is refused anywhere but a top-level PR comment, whatever the PR state', () => {
  // Greptile bills a mention in a thread reply as a request: the reply that explains a fix by
  // quoting the mention must not go out. No GitHub read is needed to refuse it.
  const noApi = () => { throw new Error('should not read GitHub'); };
  const reply = { tool_name: 'mcp__github__add_reply_to_pull_request_comment', tool_input: { owner: 'torixtv', repo: 'x', pullNumber: 9, commentId: 1, body: 'Fixed: a plain `@greptileai` now waits for CI.' } };
  assert.equal(evaluateHook(reply, { api: noApi }).block, true);
  assert.match(evaluateHook(reply, { api: noApi }).message, /outside a top-level PR comment/);
  const review = { tool_name: 'mcp__github__pull_request_review_write', tool_input: { owner: 'torixtv', repo: 'x', pullNumber: 9, body: 'see @greptileai' } };
  assert.equal(evaluateHook(review, { api: noApi }).block, true);
  const threadApi = { tool_name: 'Bash', tool_input: { command: 'gh api repos/torixtv/x/pulls/9/comments -f body="@greptileai"' } };
  assert.equal(evaluateHook(threadApi, { api: noApi }).block, true);
  const edit = { tool_name: 'Bash', tool_input: { command: 'gh pr comment 9 --repo torixtv/x --edit-last --body "@greptileai"' } };
  assert.equal(evaluateHook(edit, { api: noApi }).block, true);
  // A top-level comment goes on to the round rules (here: GitHub cannot be read, so not checked).
  const top = { tool_name: 'mcp__github__add_issue_comment', tool_input: { owner: 'torixtv', repo: 'x', issue_number: 9, body: '@greptileai' } };
  assert.match(evaluateHook(top, { api: noApi }).message, /not checked/);
});

test("gh's own flags before the subcommand do not hide a comment", () => {
  const cmd = (c) => ({ tool_name: 'Bash', tool_input: { command: c } });
  assert.deepEqual(targetOf(cmd("gh --repo torixtv/gitops pr comment 1050 --body 'hi'"), null), { repo: 'torixtv/gitops', number: 1050 });
  assert.deepEqual(targetOf(cmd('gh -R torixtv/gitops pr comment 7 --body hi'), null), { repo: 'torixtv/gitops', number: 7 });
  // the round rules run for it (GitHub unreadable here, so it is reported as not checked)
  const failing = () => { throw new Error('offline'); };
  assert.match(evaluateHook(cmd("gh --repo torixtv/gitops pr comment 1050 --body '@greptileai'"), { api: failing }).message, /not checked/);
});

test('every paid-tier wording is forbidden, a plain request is not', () => {
  for (const t of ['@greptileai plus', '@greptileai apex', '@greptileai review this at plus', '@greptileai please review at APEX']) {
    assert.equal(forbiddenRequest(t), true, t);
  }
  assert.equal(forbiddenRequest('@greptileai'), false);
  assert.equal(forbiddenRequest('@greptileai\nthe plus button moved'), false);
});

test('a commit status counts as CI', () => {
  const api = (status) => (path) => {
    if (path === 'repos/torixtv/t/pulls/2') return { state: 'open', draft: false, head: { sha: HEAD }, labels: [] };
    if (path.includes('/check-runs')) return { check_runs: green };
    if (path.endsWith('/status')) return { statuses: [{ context: 'Terraform Cloud/torix', state: status }] };
    if (path.includes('/issues/2/comments')) return [{ user: { login: 'greptile-apps[bot]' }, body: SUMMARY(4, 1, OLD) }];
    return [];
  };
  assert.equal(decide(gather('torixtv/t', 2, api('pending'))).verdict, 'blocked');
  assert.equal(decide(gather('torixtv/t', 2, api('failure'))).verdict, 'blocked');
  assert.equal(decide(gather('torixtv/t', 2, api('success'))).verdict, 'request-ok');
});

test('a first review that ran on an earlier commit and posted nothing does not hold the PR forever', () => {
  const api = (earlier) => (path) => {
    if (path === 'repos/torixtv/r/pulls/3') return { state: 'open', draft: false, head: { sha: HEAD }, labels: [] };
    if (path.startsWith(`repos/torixtv/r/commits/${HEAD}/check-runs`)) return { check_runs: green };
    if (path.endsWith('/status')) return { statuses: [] };
    if (path.startsWith('repos/torixtv/r/pulls/3/commits')) return [{ sha: OLD }, { sha: HEAD }];
    if (path.startsWith(`repos/torixtv/r/commits/${OLD}/check-runs`)) return { check_runs: earlier };
    return [];
  };
  const done = [{ name: 'Greptile Review', status: 'completed', conclusion: 'success' }];
  assert.equal(decide(gather('torixtv/r', 3, api(done))).verdict, 'request-ok');
  assert.equal(decide(gather('torixtv/r', 3, api([]))).verdict, 'blocked');
});

test('a body read from a file is checked, and one that cannot be read is refused', () => {
  assert.deepEqual(bodyFiles('gh pr comment 4 --body-file note.md'), ['note.md']);
  assert.deepEqual(bodyFiles('gh pr comment 4 -F "note.md"'), ['note.md']);
  assert.deepEqual(bodyFiles('gh api repos/o/r/issues/4/comments -F body=@note.md'), ['note.md']);
  assert.deepEqual(bodyFiles('gh api repos/o/r/issues/4/comments -F per_page=5'), []);
  const call = { tool_name: 'Bash', cwd: '/w', tool_input: { command: 'gh pr comment 4 --repo torixtv/x --body-file note.md' } };
  const paid = evaluateHook(call, { readFile: () => '@greptileai review this at apex' });
  assert.equal(paid.block, true);
  assert.match(paid.message, /plus|apex/);
  const unreadable = evaluateHook(call, { readFile: () => { throw new Error('ENOENT'); } });
  assert.equal(unreadable.block, true);
  assert.match(unreadable.message, /could not read/);
  assert.equal(evaluateHook(call, { readFile: () => 'thanks, fixed' }).block, false);
});

test('a request already waiting for Greptile, or a head CI has not reached yet, holds the next one', () => {
  assert.equal(decide(state({ pendingRequest: true })).verdict, 'blocked');
  assert.equal(decide(state({ checks: [], headAgeMs: 60 * 1000 })).verdict, 'blocked');
  assert.equal(decide(state({ checks: [], headAgeMs: 60 * 60 * 1000 })).verdict, 'request-ok'); // a repo with no CI at all
  const now = Date.parse('2026-10-24T23:00:00Z');
  const api = (requestAt, checkStartedAt) => (path) => {
    if (path === 'repos/torixtv/q/pulls/5') return { state: 'open', draft: false, head: { sha: HEAD }, labels: [] };
    if (path.startsWith(`repos/torixtv/q/commits/${HEAD}/check-runs`)) {
      return { check_runs: [...green, ...(checkStartedAt ? [{ name: 'Greptile Review', status: 'completed', conclusion: 'success', started_at: checkStartedAt }] : [])] };
    }
    if (path.endsWith('/status')) return { statuses: [] };
    if (path === `repos/torixtv/q/commits/${HEAD}`) return { commit: { committer: { date: '2026-10-24T20:00:00Z' } } };
    if (path.startsWith('repos/torixtv/q/issues/5/comments')) {
      return [
        { user: { login: 'greptile-apps[bot]', type: 'Bot' }, body: SUMMARY(4, 1, OLD), created_at: '2026-10-24T21:00:00Z', updated_at: '2026-10-24T21:00:00Z' },
        { user: { login: 'dan', type: 'User' }, body: '@greptileai', created_at: requestAt },
      ];
    }
    return [];
  };
  assert.equal(gather('torixtv/q', 5, api('2026-10-24T22:55:00Z'), now).pendingRequest, true);
  assert.equal(gather('torixtv/q', 5, api('2026-10-24T22:55:00Z', '2026-10-24T22:56:00Z'), now).pendingRequest, false); // picked up
  assert.equal(gather('torixtv/q', 5, api('2026-10-24T21:30:00Z'), now).pendingRequest, false); // stale: Greptile never came
});

test('only comment writes are checked, and a cd hides the target repository', () => {
  const cmd = (c) => ({ tool_name: 'Bash', tool_input: { command: c } });
  assert.equal(targetOf(cmd("gh api repos/torixtv/x/issues/3/comments --jq '.[] | select(.body | test(\"@greptileai\"))'"), null), null);
  assert.equal(targetOf(cmd('gh api -X GET repos/torixtv/x/issues/3/comments -f per_page=100'), null), null);
  assert.deepEqual(targetOf(cmd('gh api repos/torixtv/x/issues/3/comments -f body=hi'), null), { repo: 'torixtv/x', number: 3 });
  assert.deepEqual(targetOf(cmd('gh api --method POST repos/torixtv/x/issues/3/comments --input b.json'), null), { repo: 'torixtv/x', number: 3 });
  assert.deepEqual(targetOf(cmd('cd ../other && gh pr comment 12 --body hi'), 'torixtv/here'), { repo: null, number: 12 });
  assert.deepEqual(targetOf(cmd('cd ../other && gh pr comment 12 --repo torixtv/other --body hi'), 'torixtv/here'), { repo: 'torixtv/other', number: 12 });
});

test('a quoted repository is read, and a named one that cannot be read is not swapped for the checkout', () => {
  const cmd = (c) => ({ tool_name: 'Bash', tool_input: { command: c } });
  assert.deepEqual(targetOf(cmd('gh pr comment 4 --repo "torixtv/catalog-service" --body hi'), 'torixtv/here'), { repo: 'torixtv/catalog-service', number: 4 });
  assert.deepEqual(targetOf(cmd("gh pr comment 4 -R 'torixtv/catalog-service' --body hi"), 'torixtv/here'), { repo: 'torixtv/catalog-service', number: 4 });
  assert.deepEqual(targetOf(cmd('gh pr comment 4 --repo "$REPO" --body hi'), 'torixtv/here'), { repo: null, number: 4 });
});

test('an MCP body is checked line by line, as written', () => {
  const failing = () => { throw new Error('offline'); };
  const call = (body) => ({ tool_name: 'mcp__github__add_issue_comment', tool_input: { owner: 'torixtv', repo: 'x', issue_number: 9, body } });
  // a plain request with the word on a later line is not a paid one
  assert.match(evaluateHook(call('@greptileai\nThe plus button is fixed.'), { api: failing }).message, /not checked/);
  assert.equal(evaluateHook(call('@greptileai review this at plus'), { api: failing }).block, true);
});
