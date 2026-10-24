// node --test .claude/hooks/greptile-round-guard.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { decide, evaluateHook, forbiddenRequest, gather, parseSummary, targetOf, unansweredThreads, repoFromRemote } from './greptile-round-guard.mjs';

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
