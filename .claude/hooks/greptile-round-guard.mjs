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
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export const GREPTILE_LOGIN = 'greptile-apps[bot]';
export const GREPTILE_CHECK = 'Greptile Review';
export const EXTRA_ROUND_LABEL = 'greptile-extra-round';
export const ROUND_LIMIT = 3;
const CI_GRACE_MS = 15 * 60 * 1000; // how long a fresh head may show no CI before none is assumed
const REQUEST_WAIT_MS = 30 * 60 * 1000; // how long an unstarted request counts as pending

const FAILED = new Set(['failure', 'timed_out', 'cancelled', 'action_required', 'startup_failure', 'stale']);
const COMMENT_TOOLS = /(add_issue_comment|add_reply_to_pull_request_comment|update_issue_comment|add_comment_to_pending_review|pull_request_review_write)$/;

export const mentionsGreptile = (text) => /@greptileai\b/i.test(text ?? '');
// A paid tier, in any wording that names it after the mention ("@greptileai plus",
// "@greptileai review this at apex"), and /greploop.
export const forbiddenRequest = (text) =>
  /@greptileai\b[^\n]*?\b(plus|apex)\b/i.test(text ?? '') || /(^|\s)\/greploop\b/i.test(text ?? '');

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
export function decide({ pr, checks, summary, unanswered, pendingRequest = false, headAgeMs = null }) {
  const reasons = [];
  const labels = pr.labels ?? [];
  if (pr.state !== 'open') return { verdict: 'blocked', reasons: ['the PR is not open'] };
  if (pr.draft) {
    return { verdict: 'blocked', reasons: ['the PR is a draft: finish the change, get CI green and mark it ready first'] };
  }

  const greptile = checks.filter((c) => c.name === GREPTILE_CHECK);
  const ci = checks.filter((c) => c.name !== GREPTILE_CHECK);
  if (greptile.some((c) => c.status !== 'completed')) reasons.push('a Greptile review is running on this PR');
  const red = ci.filter((c) => c.status === 'completed' && FAILED.has(c.conclusion)).map((c) => c.name);
  const running = ci.filter((c) => c.status !== 'completed').map((c) => c.name);
  if (red.length) reasons.push(`CI is red: ${[...new Set(red)].join(', ')}`);
  if (running.length) reasons.push(`CI is still running: ${[...new Set(running)].join(', ')}`);
  if (unanswered > 0) reasons.push(`${unanswered} Greptile thread(s) have no reply`);
  if (pendingRequest) reasons.push('a request is already waiting for Greptile to start: give it a few minutes');
  // Right after a push no check run exists yet, and an empty list is not a green one.
  if (ci.length === 0 && headAgeMs !== null && headAgeMs < CI_GRACE_MS) {
    reasons.push('no CI has reported on the head commit yet: wait for it');
  }

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

export function gather(repo, number, api = ghApi, now = Date.now()) {
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
    startedAt: c.started_at,
  }));
  // Some CI reports through commit statuses instead (Terraform Cloud, for one): count them too.
  for (const st of api(`repos/${repo}/commits/${pr.headSha}/status`).statuses ?? []) {
    checks.push({
      name: st.context,
      status: st.state === 'pending' ? 'in_progress' : 'completed',
      conclusion: st.state === 'success' ? 'success' : 'failure',
    });
  }
  const comments = pages(api, `repos/${repo}/issues/${number}/comments`);
  const summaryComment = comments
    .filter((c) => c.user?.login === GREPTILE_LOGIN && /Confidence Score/i.test(c.body ?? ''))
    .at(-1);
  // Greptile reviews the commit that was head when the round started, so after a push its check
  // sits on an earlier commit. Look back while the head shows none: a review still running there
  // must hold the next request even when an older summary exists, and with no summary at all a
  // first review that ran there and posted nothing must not be mistaken for one never started.
  if (!checks.some((c) => c.name === GREPTILE_CHECK)) {
    const shas = api(`repos/${repo}/pulls/${number}/commits?per_page=100`).map((c) => c.sha).filter((sha) => sha !== pr.headSha);
    for (const sha of shas.slice(-10).reverse()) {
      const earlier = api(`repos/${repo}/commits/${sha}/check-runs?check_name=${encodeURIComponent(GREPTILE_CHECK)}`).check_runs ?? [];
      if (earlier.length) {
        const found = earlier.map((c) => ({ name: c.name, status: c.status, conclusion: c.conclusion, startedAt: c.started_at }));
        // With a summary, only a review still running matters; a finished one is what the summary reports.
        checks.push(...(summaryComment ? found.filter((c) => c.status !== 'completed') : found));
        break;
      }
    }
  }
  const threads = unansweredThreads(pages(api, `repos/${repo}/pulls/${number}/comments`));
  // A request someone already posted, newer than the last review and not yet picked up by a
  // Greptile check, is pending: asking again would only queue a second paid round.
  const lastReview = Date.parse(summaryComment?.updated_at ?? summaryComment?.created_at ?? 0) || 0;
  const started = checks.filter((c) => c.name === GREPTILE_CHECK).map((c) => Date.parse(c.startedAt ?? 0) || 0);
  const pendingRequest = comments.some((c) => {
    const at = Date.parse(c.created_at ?? 0) || 0;
    return c.user?.type !== 'Bot' && mentionsGreptile(c.body) && at > lastReview && now - at < REQUEST_WAIT_MS && !started.some((t) => t >= at);
  });
  // How long the head has been on the PR. A commit's own date can be long before its push, so the
  // PR's last update counts too (a push updates it); the later of the two is the safe one.
  let headAgeMs = null;
  try {
    const date = api(`repos/${repo}/commits/${pr.headSha}`).commit?.committer?.date;
    const newest = Math.max(Date.parse(date ?? 0) || 0, Date.parse(raw.updated_at ?? 0) || 0);
    if (newest) headAgeMs = now - newest;
  } catch {
    // Unknown age: fall back to the check list alone.
  }
  return { pr, checks, summary: parseSummary(summaryComment?.body), unanswered: threads.length, threads, pendingRequest, headAgeMs };
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
// `gh pr comment`, `gh issue review` and the like, with gh's own flags allowed before the
// subcommand (`gh --repo owner/name pr comment 12`).
const GH_COMMENT = /\bgh(?:\s+(?:-R|--repo)[\s=]+\S+)*\s+(?:pr|issue)\s+(?:comment|review)\b/;

export function targetOf(input, cwdRepo) {
  const name = input.tool_name ?? '';
  const ti = input.tool_input ?? {};
  if (name === 'Bash') {
    const cmd = ti.command ?? '';
    const api = cmd.match(/repos\/([\w.-]+\/[\w.-]+)\/(?:issues|pulls)\/(\d+)\/(?:comments|reviews)/);
    if (api) {
      // gh api reads unless told to write: a GET that merely mentions Greptile is not a request.
      const method = cmd.match(/(?:-X|--method)[\s=]+(\w+)/i);
      const writes = method ? !/^GET$/i.test(method[1]) : /\s(?:-f|-F|--field|--raw-field|--input)[\s=]/.test(cmd);
      return writes ? { repo: api[1], number: Number(api[2]) } : null;
    }
    const cli = cmd.match(new RegExp(`${GH_COMMENT.source}\\s+(\\d+)`));
    if (cli) {
      // An explicit repository wins, quoted or not; one that is named but cannot be read is not
      // replaced by the starting checkout's.
      const named = /(?:^|\s)(?:-R|--repo)[\s=]/.test(cmd);
      const r = cmd.match(/(?:-R|--repo)[\s=]+["']?([\w.-]+\/[\w.-]+)["']?(?=\s|$)/);
      // After a cd the starting checkout says nothing about which repository gh will post to.
      const moved = /(?:^|[;&|(]\s*)(?:cd|pushd)\s/.test(cmd);
      return { repo: r ? r[1] : named || moved ? null : cwdRepo, number: Number(cli[1]) };
    }
    // A comment command this cannot place is still a comment: refuse it rather than wave it through.
    if (GH_COMMENT.test(cmd) || /gh\s+api\b[\s\S]*\/(comments|reviews)\b/.test(cmd)) {
      return { repo: null, number: null };
    }
    return null;
  }
  if (!COMMENT_TOOLS.test(name)) return null;
  const number = ti.issue_number ?? ti.pullNumber ?? ti.pull_number ?? ti.issueNumber;
  const repo = ti.owner && ti.repo ? `${ti.owner}/${ti.repo}` : null;
  return { repo, number: number != null ? Number(number) : null };
}

/** Files a posting command takes its body from: gh --body-file / -F <file>, gh api -F body=@<file> or --input <file>. */
export function bodyFiles(cmd) {
  const out = [];
  // One shell argument: double-quoted, single-quoted (spaces kept) or bare.
  const ARG = String.raw`(?:"([^"]*)"|'([^']*)'|(\S+))`;
  const value = (m, i) => m[i] ?? m[i + 1] ?? m[i + 2];
  for (const m of cmd.matchAll(new RegExp(`--body-file[=\\s]+${ARG}`, 'g'))) out.push(value(m, 1));
  for (const m of cmd.matchAll(new RegExp(`--input[=\\s]+${ARG}`, 'g'))) out.push(value(m, 1));
  for (const m of cmd.matchAll(new RegExp(`\\s-F\\s+${ARG}`, 'g'))) {
    const v = value(m, 1);
    if (!v.includes('=')) out.push(v);
    else if (v.includes('=@')) out.push(v.slice(v.indexOf('=@') + 2));
  }
  for (const m of cmd.matchAll(new RegExp(`\\s-f\\s+\\w+=@${ARG}`, 'g'))) out.push(value(m, 1));
  return out;
}

/** The one place a round request belongs: a new top-level comment on the PR. */
export function isTopLevelComment(input) {
  const name = input.tool_name ?? '';
  if (name === 'Bash') {
    const cmd = input.tool_input?.command ?? '';
    if (/\bgh(?:\s+(?:-R|--repo)[\s=]+\S+)*\s+(?:pr|issue)\s+comment\b/.test(cmd) && !/--edit-last\b/.test(cmd)) return true;
    return /repos\/[\w.-]+\/[\w.-]+\/issues\/\d+\/comments\b/.test(cmd);
  }
  return /add_issue_comment$/.test(name);
}

/** Hook entry point. Returns { block: boolean, message: string }. */
export function evaluateHook(input, { api = ghApi, cwdRepo = null, readFile = (p) => readFileSync(p, 'utf8') } = {}) {
  // The raw strings, not their JSON: escaping turns line breaks into text and joins the lines.
  let text = input.tool_name === 'Bash'
    ? input.tool_input?.command ?? ''
    : Object.values(input.tool_input ?? {}).filter((v) => typeof v === 'string').join('\n');
  // A body read from a file is invisible in the command: read it, and refuse what cannot be read.
  if (input.tool_name === 'Bash' && targetOf(input, cwdRepo)) {
    for (const file of bodyFiles(text)) {
      try {
        text += `\n${readFile(resolve(input.cwd ?? process.cwd(), file))}`;
      } catch {
        return {
          block: true,
          message: `Greptile review rounds: could not read the comment body from ${file}, so it was not checked. Pass the body inline with --body, or post it with the GitHub MCP comment tool.`,
        };
      }
    }
  }
  if (!mentionsGreptile(text) && !forbiddenRequest(text)) return { block: false, message: '' };
  const target = targetOf(input, cwdRepo);
  if (!target) return { block: false, message: '' };
  if (forbiddenRequest(text)) {
    return { block: true, message: 'Greptile review rounds: never ask for "plus"/"apex" reviews (3 and 10 credits) or run /greploop. Post a plain @greptileai.' };
  }
  // The request is one top-level PR comment. Anywhere else (a thread reply, a review, an edited
  // comment) the mention is a quote, and Greptile still reads it as a request and bills it.
  if (!isTopLevelComment(input)) {
    return {
      block: true,
      message:
        'Greptile review rounds: this text mentions @greptileai outside a top-level PR comment. Greptile treats any ' +
        'comment carrying the mention as a review request, quoted or not. Reword it without the mention.',
    };
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
