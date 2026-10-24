#!/usr/bin/env node
// Greptile review-round guard. The same file lives in .claude/hooks/ of every torixtv repository;
// change them together. Zero dependencies: it needs node and an authenticated `gh` on PATH.
//
//   node .claude/hooks/greptile-round-guard.mjs status [owner/repo] [pr]
//       Prints where the PR stands in its review rounds and whether a round may be requested.
//       Without arguments it uses this checkout's origin and current branch.
//
//   (no arguments, JSON on stdin)
//       Claude Code PreToolUse hook. Blocks a comment that asks @greptileai for a round the
//       "Greptile review rounds" rules say not to start. Exit 2 blocks the call; every other
//       path allows it. When GitHub cannot be read it allows the call and says so: the written
//       rules still apply.
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export const GREPTILE_LOGIN = 'greptile-apps[bot]';
export const GREPTILE_CHECK = 'Greptile Review';
export const SKIP_LABEL = 'skip-greptile';
export const EXTRA_ROUND_LABEL = 'greptile-extra-round';
export const ROUND_LIMIT = 3;

const FAILED = new Set(['failure', 'timed_out', 'cancelled', 'action_required', 'startup_failure', 'stale']);
const COMMENT_TOOLS = /(add_issue_comment|add_reply_to_pull_request_comment|update_issue_comment|add_comment_to_pending_review|pull_request_review_write)$/;

export const mentionsGreptile = (text) => /@greptileai\b/i.test(text ?? '');
export const forbiddenRequest = (text) =>
  /@greptileai\s+(plus|apex)\b/i.test(text ?? '') || /(^|\s)\/greploop\b/i.test(text ?? '');

/** Reads score, review count and last reviewed commit out of Greptile's summary comment. */
export function parseSummary(body) {
  if (!body) return null;
  const score = body.match(/Confidence Score:\s*(\d)\s*\/\s*5/i);
  const reviews = body.match(/Reviews \((\d+)\)/);
  const last = body.match(/Last reviewed commit:[\s\S]*?\/commit\/([0-9a-f]{7,40})/i);
  return {
    score: score ? Number(score[1]) : null,
    reviews: reviews ? Number(reviews[1]) : null,
    lastReviewedSha: last ? last[1] : null,
  };
}

/** Greptile review threads where nobody but a bot has replied. */
export function unansweredThreads(reviewComments, botLogin = GREPTILE_LOGIN) {
  const answered = new Set(
    reviewComments
      .filter((c) => c.in_reply_to_id && c.user?.type !== 'Bot' && !c.user?.login?.endsWith('[bot]'))
      .map((c) => c.in_reply_to_id),
  );
  return reviewComments.filter((c) => !c.in_reply_to_id && c.user?.login === botLogin && !answered.has(c.id));
}

const sameCommit = (a, b) => Boolean(a && b) && (a.startsWith(b) || b.startsWith(a));

/**
 * Decides whether a new round may be requested. Pure: `state` holds the PR, its head check runs,
 * the parsed summary (or null) and the unanswered thread count.
 */
export function decide({ pr, checks, summary, unanswered }) {
  const reasons = [];
  const labels = pr.labels ?? [];
  if (pr.state !== 'open') return { verdict: 'blocked', reasons: ['the PR is not open'] };
  if (pr.draft) {
    return { verdict: 'blocked', reasons: ['the PR is a draft; marking it ready starts round 1 on its own'] };
  }
  if (labels.includes(SKIP_LABEL)) {
    return { verdict: 'skipped', reasons: [`the PR is labelled ${SKIP_LABEL}: it merges on green CI without a review`] };
  }

  const greptile = checks.filter((c) => c.name === GREPTILE_CHECK);
  const ci = checks.filter((c) => c.name !== GREPTILE_CHECK);
  if (greptile.some((c) => c.status !== 'completed')) reasons.push('a Greptile review is running on the head commit');
  const red = ci.filter((c) => c.status === 'completed' && FAILED.has(c.conclusion)).map((c) => c.name);
  const running = ci.filter((c) => c.status !== 'completed').map((c) => c.name);
  if (red.length) reasons.push(`CI is red: ${[...new Set(red)].join(', ')}`);
  if (running.length) reasons.push(`CI is still running: ${[...new Set(running)].join(', ')}`);
  if (unanswered > 0) reasons.push(`${unanswered} Greptile thread(s) have no reply`);

  const reviewedHead = summary && sameCommit(summary.lastReviewedSha, pr.headSha);
  if (summary && reviewedHead) {
    if (summary.score === 5 && unanswered === 0) return { verdict: 'done', reasons: ['5/5 on the head commit with every thread answered'] };
    reasons.push('nothing changed since the last review: fix the findings and push first');
  }
  if (summary?.reviews != null && summary.reviews >= ROUND_LIMIT && !labels.includes(EXTRA_ROUND_LABEL)) {
    reasons.push(
      `this PR has had ${summary.reviews} reviews; ask a person before another round ` +
        `(they can post @greptileai themselves or add the ${EXTRA_ROUND_LABEL} label)`,
    );
  }
  return reasons.length ? { verdict: 'blocked', reasons } : { verdict: 'request-ok', reasons: [] };
}

// --- GitHub access (impure) -------------------------------------------------------------------

export function ghApi(path) {
  return JSON.parse(execFileSync('gh', ['api', path], { encoding: 'utf8', timeout: 20000, stdio: ['ignore', 'pipe', 'pipe'] }));
}

function pages(api, path, max = 5) {
  const out = [];
  for (let page = 1; page <= max; page++) {
    const batch = api(`${path}${path.includes('?') ? '&' : '?'}per_page=100&page=${page}`);
    out.push(...batch);
    if (batch.length < 100) break;
  }
  return out;
}

export function gather(repo, number, api = ghApi) {
  const raw = api(`repos/${repo}/pulls/${number}`);
  const pr = {
    state: raw.state,
    draft: raw.draft,
    headSha: raw.head.sha,
    labels: (raw.labels ?? []).map((l) => l.name),
  };
  const checks = api(`repos/${repo}/commits/${pr.headSha}/check-runs?per_page=100`).check_runs.map((c) => ({
    name: c.name,
    status: c.status,
    conclusion: c.conclusion,
  }));
  const comments = pages(api, `repos/${repo}/issues/${number}/comments`);
  const summaryComment = comments
    .filter((c) => c.user?.login === GREPTILE_LOGIN && /Confidence Score/i.test(c.body ?? ''))
    .at(-1);
  const threads = unansweredThreads(pages(api, `repos/${repo}/pulls/${number}/comments`));
  return { pr, checks, summary: parseSummary(summaryComment?.body), unanswered: threads.length, threads };
}

// --- Locating the PR a tool call targets -------------------------------------------------------

export function repoFromRemote(url) {
  const m = (url ?? '').trim().match(/github\.com[:/]([^/\s]+)\/([^/\s]+?)(?:\.git)?$/);
  return m ? `${m[1]}/${m[2]}` : null;
}

function originRepo(cwd) {
  try {
    return repoFromRemote(execFileSync('git', ['-C', cwd, 'remote', 'get-url', 'origin'], { encoding: 'utf8' }));
  } catch {
    return null;
  }
}

/** Returns { repo, number } for a comment-posting tool call, null for anything else. */
export function targetOf(input, cwdRepo) {
  const name = input.tool_name ?? '';
  const ti = input.tool_input ?? {};
  if (name === 'Bash') {
    const cmd = ti.command ?? '';
    const api = cmd.match(/repos\/([\w.-]+\/[\w.-]+)\/(?:issues|pulls)\/(\d+)\/(?:comments|reviews)/);
    if (api) return { repo: api[1], number: Number(api[2]) };
    const cli = cmd.match(/gh\s+(?:pr|issue)\s+(?:comment|review)\s+(\d+)/);
    if (cli) {
      const r = cmd.match(/(?:-R|--repo)[\s=]+([\w.-]+\/[\w.-]+)/);
      return { repo: r ? r[1] : cwdRepo, number: Number(cli[1]) };
    }
    if (/gh\s+(?:pr|issue)\s+(?:comment|review)\b/.test(cmd) || /gh\s+api\b[\s\S]*\/(comments|reviews)\b/.test(cmd)) {
      return { repo: null, number: null };
    }
    return null;
  }
  if (!COMMENT_TOOLS.test(name)) return null;
  const number = ti.issue_number ?? ti.pullNumber ?? ti.pull_number ?? ti.issueNumber;
  const repo = ti.owner && ti.repo ? `${ti.owner}/${ti.repo}` : null;
  return { repo, number: number != null ? Number(number) : null };
}

/** Hook entry point. Returns { block: boolean, message: string }. */
export function evaluateHook(input, { api = ghApi, cwdRepo = null } = {}) {
  const text = input.tool_name === 'Bash' ? input.tool_input?.command : JSON.stringify(input.tool_input ?? {});
  if (!mentionsGreptile(text)) return { block: false, message: '' };
  const target = targetOf(input, cwdRepo);
  if (!target) return { block: false, message: '' };
  if (forbiddenRequest(text)) {
    return { block: true, message: 'Greptile review rounds: never ask for "plus"/"apex" reviews (3 and 10 credits) or run /greploop. Post a plain @greptileai.' };
  }
  if (!target.repo || !target.number) {
    return {
      block: true,
      message:
        'Greptile review rounds: could not tell which PR this @greptileai comment is for, so it was not checked. ' +
        'Post it with `gh pr comment <number> --repo <owner/repo>` or the GitHub MCP comment tool.',
    };
  }
  let state;
  try {
    state = gather(target.repo, target.number, api);
  } catch (err) {
    return { block: false, message: `Greptile round guard could not read ${target.repo}#${target.number} (${String(err.message ?? err).split('\n')[0]}); not checked. The written rules still apply.` };
  }
  const { verdict, reasons } = decide(state);
  if (verdict === 'request-ok') return { block: false, message: '' };
  return {
    block: true,
    message:
      `Greptile review rounds: do not request a round on ${target.repo}#${target.number} now (${verdict}).\n- ${reasons.join('\n- ')}\n` +
      'Run `node .claude/hooks/greptile-round-guard.mjs status` for the full picture, or see the "Greptile review rounds" section of AGENTS.md / CLAUDE.md.',
  };
}

// --- Status report -----------------------------------------------------------------------------

export function formatStatus(repo, number, state, result) {
  const { pr, checks, summary, threads } = state;
  const ci = checks.filter((c) => c.name !== GREPTILE_CHECK);
  const ciLine = ci.length
    ? ci.map((c) => `${c.name}=${c.status === 'completed' ? c.conclusion : c.status}`).join(', ')
    : 'no check runs';
  const lines = [
    `${repo}#${number} head ${pr.headSha.slice(0, 7)}${pr.draft ? ' (draft)' : ''}${pr.labels.length ? ` labels: ${pr.labels.join(', ')}` : ''}`,
    `CI: ${ciLine}`,
    summary
      ? `Greptile: ${summary.score ?? '?'}/5 after ${summary.reviews ?? '?'} review(s); last reviewed ${summary.lastReviewedSha?.slice(0, 7) ?? 'unknown'}${sameCommit(summary.lastReviewedSha, pr.headSha) ? ' (head)' : ' (behind head)'}`
      : 'Greptile: no summary yet',
    `Unanswered Greptile threads: ${threads.length}${threads.length ? ` (${threads.map((t) => t.html_url ?? t.id).join(', ')})` : ''}`,
    `Verdict: ${result.verdict}${result.reasons.length ? `\n- ${result.reasons.join('\n- ')}` : ''}`,
  ];
  return lines.join('\n');
}

function currentPr(repo, cwd) {
  const branch = execFileSync('git', ['-C', cwd, 'rev-parse', '--abbrev-ref', 'HEAD'], { encoding: 'utf8' }).trim();
  const owner = repo.split('/')[0];
  const open = ghApi(`repos/${repo}/pulls?state=open&head=${owner}:${encodeURIComponent(branch)}`);
  if (!open.length) throw new Error(`no open PR for branch ${branch} in ${repo}`);
  return open[0].number;
}

async function readStdin() {
  let data = '';
  for await (const chunk of process.stdin) data += chunk;
  return data;
}

async function main(argv) {
  if (argv[0] === 'status') {
    const cwd = process.cwd();
    const repo = argv[1] ?? originRepo(cwd);
    if (!repo) throw new Error('pass owner/repo: no GitHub origin in this checkout');
    const number = argv[2] ? Number(argv[2]) : currentPr(repo, cwd);
    const state = gather(repo, number);
    console.log(formatStatus(repo, number, state, decide(state)));
    return 0;
  }
  const raw = await readStdin();
  let input;
  try {
    input = JSON.parse(raw || '{}');
  } catch {
    return 0;
  }
  const result = evaluateHook(input, { cwdRepo: originRepo(input.cwd ?? process.cwd()) });
  if (result.block) {
    process.stderr.write(`${result.message}\n`);
    return 2;
  }
  if (result.message) process.stdout.write(`${JSON.stringify({ systemMessage: result.message })}\n`);
  return 0;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  main(process.argv.slice(2)).then(
    (code) => process.exit(code),
    (err) => {
      process.stderr.write(`greptile-round-guard: ${err.message ?? err}\n`);
      process.exit(process.argv[2] === 'status' ? 1 : 0);
    },
  );
}
