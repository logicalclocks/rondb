/*
 * GSN signal-scope audit — fan-out classification workflow.
 *
 * Classifies NDB kernel signals (GSNs) into their SignalScope
 * (Local | Remote | Management | External) by fanning out subagents that trace
 * each signal's send sites, then adversarially verifying every restrictive
 * proposal. It emits PROPOSALS ONLY — landing entries into SignalScopes.hpp
 * (Phase 6) is human-gated and outside this script.
 *
 * Design: gsn_scope_audit_plan.md + gsn_scope_workflow_plan.md (same dir).
 * Ground truth for calibration: signal_scope_calibration_set.md (same dir).
 *
 * ── How to run (via the Workflow tool, scriptPath form) ──────────────────────
 *
 * 1. VALIDATE THE METHOD FIRST (small, ~5 agents over the 60 known GSNs):
 *      Workflow scriptPath=".../gsn_scope_audit_workflow.js"
 *               args={ mode: "calibrate", batchSize: 12 }
 *    Require `pass: true` (zero tighteningErrors, zero missing) and confirm the
 *    known traps come out right (DUMP_STATE_ORD => External; EVENT_SUBSCRIBE_REQ
 *    => Management vs SUB_START_REQ => External). If it fails, tune the METHOD
 *    string / prompts and rerun. Do NOT run audit mode until calibrate passes.
 *
 * 2. BUILD THE WORKLIST (the 831 unclassified GSNs). One way:
 *      comm -23 \
 *        <(grep -oE '^#define (GSN_[A-Z0-9_]+)' storage/ndb/include/kernel/GlobalSignalNumbers.h \
 *            | sed -E 's/^#define //' | sort -u) \
 *        <(grep -oE 'X\((GSN_[A-Z0-9_]+),' storage/ndb/include/kernel/signaldata/SignalScopes.hpp \
 *            | sed -E 's/X\((GSN_[A-Z0-9_]+),/\1/' | sort -u)
 *    Pass that list as args.gsns (a JSON array of strings). Anchor the first
 *    grep to `^#define` (not a bare `GSN_[A-Z0-9_]+` sweep) — GlobalSignalNumbers.h
 *    has several deprecated/phantom names that appear only in comments (a
 *    commented-out define, and a few "N not unused - formerly GSN_x" notes),
 *    which inflate the list to 837 if matched unanchored. Same phantom-GSN
 *    trap as gsn_scope_audit_plan.md finding #5.
 *
 * 3. RUN THE AUDIT:
 *      Workflow scriptPath=".../gsn_scope_audit_workflow.js"
 *               args={ mode: "audit", gsns: ["GSN_...", ...], batchSize: 12 }
 *    Returns { needsReview, autoPass, missing }. Humans review `needsReview`
 *    (every restrictive/low-confidence/refuted/version-variant proposal); the
 *    approved entries are appended by hand to SignalScopes.hpp.
 *
 * NOTE: this script deliberately does NOT read SignalScopes.hpp for answers —
 * agents derive scope from send sites only (see METHOD), so it stays honest
 * even though the 60 verified entries are already committed in the tree.
 */

export const meta = {
  name: 'gsn-scope-audit',
  description:
    'Classify NDB GSNs into SignalScope with adversarial verification; proposals only, human-gated integration',
  phases: [
    { title: 'Extract', detail: 'per-batch send-site extraction + first-pass scope' },
    { title: 'Verify', detail: 'adversarially refute each restrictive proposal' },
    { title: 'Coverage', detail: 'coverage check + bucket for human review' },
  ],
}

// ── Classification method (inlined so the workflow is self-contained; mirrors
//    .claude/skills/gsn-signal-scope-audit/SKILL.md) ───────────────────────────
const METHOD = `
You classify one or more NDB kernel signals (GSNs) into a SignalScope by tracing,
in the RonDB source tree, every place each signal is SENT and resolving the node
type of each sender. Determine the scope from evidence in code — never from the
signal's name, and NEVER from existing DECLARE_SIGNAL_SCOPE / signal_property /
SignalScopes.hpp entries (ignore those entirely; they may be wrong or absent, and
reading them defeats the audit). A raw grep for DECLARE_SIGNAL_SCOPE can also match
commented-out text — trust only real, active declarations, and here you ignore
declarations regardless.

The four scopes (a signal gets exactly one; take the UNION over all legitimate senders):
  - Local:      only ever sent within the same node (never crosses the network:
                EXECUTE_DIRECT, self-send, or a fixed local block ref like NDBFS_REF).
  - Remote:     crosses the wire but ONLY from a data (DB) node.
  - Management: may also legitimately come from an MGM node, but NEVER from API.
  - External:   an API node (mysqld / NdbApi client / REST) legitimately sends it — anyone.
  - Unclassified: use ONLY when you cannot fully trace the send sites. Fail open.

Decisive discriminators (apply in this priority):
  1. A literal send from storage/ndb/src/ndbapi/  => External (an API node sends it).
  2. A send from storage/ndb/src/mgmsrv/ (or mgmapi/) with NO ndbapi/ sender and a
     DB-node receiver => Management.
  3. A send whose destination ref resolves to a REMOTE node (e.g.
     numberToRef(block, ..., remoteNodeId), a NodeReceiverGroup spanning nodes,
     calcXxxBlockRef(remoteNode)) with only DB senders and no API/MGM sender => Remote.
  4. Sent only to the local node (node 0 / getOwnNodeId() / reference() /
     EXECUTE_DIRECT) and never over the wire => Local.

Enumerate EVERY send site: sendSignal, sendSignalNoRelease, sendFragmentedSignal,
sendDelayed, EXECUTE_DIRECT, the GSN passed as a variable, and routed/relayed sends
(LOCAL_ROUTE_ORD, TRPMAN forwarding). A single missed sender makes the answer wrong.

Overriding bias: errors toward strictness are the dangerous ones — mislabelling a
user-reachable signal as Local/Remote/Management would disconnect a healthy node
(worst during a rolling upgrade). When uncertain, choose the LOOSER scope and mark
low confidence. Never emit a restrictive scope from incomplete evidence.

Known traps (name-based intuition is wrong): DUMP_STATE_ORD is External (NdbApi
sends it via ndb_internal.cpp), not Management. EVENT_SUBSCRIBE_REQ is Management
(cluster log -> CMVMI) but the similarly named SUB_START_REQ is External (NdbApi
event -> SUMA). Decide by the actual sender/receiver, not the word.
`.trim()

// ── Verified ground truth for calibrate mode (mirrors signal_scope_calibration_set.md;
//    keep in sync if the calibration set changes). NOT shown to agents. ─────────
const CALIBRATION = {
  GSN_CONTINUEB: 'Local', GSN_FSSUSPENDORD: 'Local', GSN_MALICIOUS_SIGNAL_REPORT: 'Local',
  GSN_FSOPENREQ: 'Local', GSN_FSCLOSEREQ: 'Local', GSN_FSREADREQ: 'Local',
  GSN_FSWRITEREQ: 'Local', GSN_FSSYNCREQ: 'Local', GSN_FSREMOVEREQ: 'Local',
  GSN_FSAPPENDREQ: 'Local', GSN_FSOPENCONF: 'Local', GSN_FSCLOSECONF: 'Local',
  GSN_FSREADCONF: 'Local', GSN_FSWRITECONF: 'Local', GSN_FSSYNCCONF: 'Local',
  GSN_FSREMOVECONF: 'Local', GSN_FSAPPENDCONF: 'Local', GSN_FSOPENREF: 'Local',
  GSN_FSCLOSEREF: 'Local', GSN_FSREADREF: 'Local', GSN_FSWRITEREF: 'Local',
  GSN_FSSYNCREF: 'Local', GSN_FSREMOVEREF: 'Local', GSN_FSAPPENDREF: 'Local',
  GSN_STTOR: 'Local', GSN_NDB_STTOR: 'Local', GSN_STTORRY: 'Local', GSN_NDB_STTORRY: 'Local',
  GSN_READ_CONFIG_REQ: 'Local', GSN_READ_CONFIG_CONF: 'Local',
  GSN_BUILDINDXREQ: 'Local', GSN_BUILDINDXCONF: 'Local', GSN_BUILDINDXREF: 'Local',
  GSN_CREATE_FRAGMENTATION_REQ: 'Local', GSN_CREATE_FRAGMENTATION_REF: 'Local',
  GSN_CREATE_FRAGMENTATION_CONF: 'Local',
  GSN_ALLOC_MEM_REQ: 'Local', GSN_ALLOC_MEM_REF: 'Local', GSN_ALLOC_MEM_CONF: 'Local',
  GSN_FAIL_REP: 'Remote', GSN_LQHKEYREQ: 'Remote', GSN_SCAN_FRAGREQ: 'Remote',
  GSN_GCP_PREPARE: 'Remote', GSN_LCP_FRAG_ORD: 'Remote', GSN_COPY_FRAGREQ: 'Remote',
  GSN_START_ORD: 'Management', GSN_STOP_REQ: 'Management', GSN_RESUME_REQ: 'Management',
  GSN_CREATE_NODEGROUP_REQ: 'Management', GSN_DROP_NODEGROUP_REQ: 'Management',
  GSN_ALLOC_NODEID_REQ: 'Management', GSN_EVENT_SUBSCRIBE_REQ: 'Management',
  GSN_BACKUP_REQ: 'Management', GSN_ABORT_BACKUP_ORD: 'Management',
  GSN_TCKEYREQ: 'External', GSN_SCAN_TABREQ: 'External', GSN_CREATE_INDX_REQ: 'External',
  GSN_API_REGREQ: 'External', GSN_SUB_START_REQ: 'External', GSN_DUMP_STATE_ORD: 'External',
}

// ── Structured output schemas ────────────────────────────────────────────────
const SCOPE_ENUM = ['Local', 'Remote', 'Management', 'External', 'Unclassified']

const EXTRACT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['classifications'],
  properties: {
    classifications: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['gsn', 'proposedScope', 'confidence', 'versionVariance', 'sendSites', 'reasoning'],
        properties: {
          gsn: { type: 'string' },
          proposedScope: { type: 'string', enum: SCOPE_ENUM },
          confidence: { type: 'string', enum: ['high', 'medium', 'low'] },
          versionVariance: { type: 'string' },
          sendSites: {
            type: 'array',
            items: {
              type: 'object',
              additionalProperties: false,
              required: ['file', 'line', 'senderNodeType', 'crossNode'],
              properties: {
                file: { type: 'string' },
                line: { type: 'integer' },
                senderNodeType: { type: 'string', enum: ['DB', 'API', 'MGM', 'local'] },
                crossNode: { type: 'boolean' },
              },
            },
          },
          reasoning: { type: 'string' },
        },
      },
    },
  },
}

const VERIFY_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['gsn', 'refuted', 'note'],
  properties: {
    gsn: { type: 'string' },
    refuted: { type: 'boolean' },
    counterExampleSendSite: { type: ['string', 'null'] },
    looserScope: { type: ['string', 'null'] },
    note: { type: 'string' },
  },
}

// ── Helpers ──────────────────────────────────────────────────────────────────
const RESTRICTIVE = ['Local', 'Remote', 'Management']
function isRestrictiveScope(s) { return RESTRICTIVE.includes(s) }
function isRestrictive(r) { return r && isRestrictiveScope(r.proposedScope) }
function chunk(arr, n) {
  const out = []
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n))
  return out
}

function extractPrompt(gsns) {
  return `${METHOD}

Classify EACH of these GSNs and return one entry per GSN in "classifications"
(same spelling as given). Cite the decisive send sites (file + line). If you
cannot fully trace a GSN's senders, set proposedScope="Unclassified" with
confidence="low" rather than guessing. Set versionVariance to "none" unless the
legitimate sender set differs in a still-supported older version (then describe it).

GSNs to classify:
${gsns.map((g) => `  - ${g}`).join('\n')}`
}

function verifyPrompt(r) {
  return `${METHOD}

A first pass proposed that ${r.gsn} has scope "${r.proposedScope}", citing:
${(r.sendSites || []).map((s) => `  - ${s.file}:${s.line} sender=${s.senderNodeType} crossNode=${s.crossNode}`).join('\n') || '  (no sites cited)'}

Your job is to REFUTE this. Independently search for a legitimate sender that
contradicts scope "${r.proposedScope}" — e.g. an ndbapi/ send (=> should be
External) for a Local/Remote/Management proposal, an MGM send for a Remote
proposal, or any cross-node send for a Local proposal. If you find one, set
refuted=true, give the counterExampleSendSite, and the looserScope it implies.
Default refuted=true if you are not confident the restrictive scope is correct —
a wrong restrictive scope disconnects healthy nodes.`
}

// ── Run ──────────────────────────────────────────────────────────────────────
// args may arrive as a parsed object or as a JSON string, depending on how the
// workflow is invoked — accept both.
let ARGS = args
if (typeof ARGS === 'string') {
  try { ARGS = JSON.parse(ARGS) } catch (e) { ARGS = {} }
}
ARGS = ARGS || {}
const mode = ARGS.mode || 'audit'
const batchSize = ARGS.batchSize || 12
const worklist = mode === 'calibrate' ? Object.keys(CALIBRATION) : (ARGS.gsns || [])

if (!worklist.length) {
  log('empty worklist — pass args.gsns (audit mode) or use mode:"calibrate"')
  return { error: 'empty worklist', mode }
}
log(`${mode}: ${worklist.length} GSNs in ${Math.ceil(worklist.length / batchSize)} batch(es)`)

// Extract per batch, then adversarially verify only the restrictive proposals.
// No barrier between batches: a batch's restrictive findings verify while other
// batches are still extracting.
const results = await pipeline(
  chunk(worklist, batchSize),
  (batch) =>
    agent(extractPrompt(batch), { phase: 'Extract', label: `extract:${batch[0]}+${batch.length - 1}`, schema: EXTRACT_SCHEMA })
      .then((o) => (o && o.classifications) || []),
  async (extracted) => {
    // Calibrate validates extraction accuracy against embedded ground truth, so
    // the adversarial verify (a safety net for audit mode) is skipped — keeps
    // calibrate to ~1 agent per batch instead of one per restrictive proposal.
    if (mode === 'calibrate') return extracted
    const restrictive = extracted.filter(isRestrictive)
    const rest = extracted.filter((r) => !isRestrictive(r))
    const verified = (
      await parallel(
        restrictive.map((r) => () =>
          agent(verifyPrompt(r), { phase: 'Verify', label: `verify:${r.gsn}`, schema: VERIFY_SCHEMA })
            .then((v) => ({ ...r, verify: v })),
        ),
      )
    ).filter(Boolean)
    return [...verified, ...rest]
  },
)

phase('Coverage')
const flat = results.flat().filter(Boolean)
const seen = new Set(flat.map((r) => r.gsn))
const missing = worklist.filter((g) => !seen.has(g)) // dropped/crashed agents — reported, never silently dropped

if (mode === 'calibrate') {
  const mismatches = flat
    .filter((r) => r.proposedScope !== CALIBRATION[r.gsn])
    .map((r) => ({ gsn: r.gsn, expected: CALIBRATION[r.gsn], got: r.proposedScope, confidence: r.confidence }))
  // The blocking failures: proposing a restrictive scope where the truth is looser
  // (a would-be false-positive disconnect).
  const tighteningErrors = mismatches.filter(
    (m) => isRestrictiveScope(m.got) && !isRestrictiveScope(m.expected),
  )
  const pass = tighteningErrors.length === 0 && missing.length === 0
  log(`calibrate: ${flat.length}/${worklist.length} classified, ${mismatches.length} mismatch(es), ${tighteningErrors.length} tightening error(s) => pass=${pass}`)
  return { mode, total: flat.length, missing, mismatches, tighteningErrors, pass }
}

// audit mode: bucket for human review.
const needsReview = flat.filter(
  (r) =>
    isRestrictive(r) ||
    r.confidence !== 'high' ||
    (r.verify && r.verify.refuted) ||
    (r.versionVariance && r.versionVariance !== 'none'),
)
const autoPass = flat.filter((r) => !needsReview.includes(r))
log(`audit: ${flat.length} classified, ${needsReview.length} need review, ${autoPass.length} auto-pass, ${missing.length} missing`)
return { mode, total: flat.length, missing, needsReviewCount: needsReview.length, needsReview, autoPass }
