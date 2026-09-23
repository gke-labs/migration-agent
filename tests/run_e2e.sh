#!/usr/bin/env bash
# Copyright 2026 Google LLC
# Licensed under the Apache License, Version 2.0.
#
# Local E2E test runner for the GKE Agentic Migration plugin.
#
# Tiers:
#   smoke       plugin manifest validation + MCP server boot + tool exposure (no GCP writes)
#   mcp-direct  full bootstrap flow via the python MCP client (elicitation approved
#               programmatically) -> verifies the GCS ledger bucket is really created
#   cc          same flow driven by the Claude Code harness (--plugin-dir, Elicitation
#               hook auto-approval) -> verifies plugin import, skill exposure, and bucket
#
# Config comes from flags, then E2E_* env vars, then your current gcloud config.
# Run ./tests/run_e2e.sh -h for usage.

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TESTS_DIR="$REPO_ROOT/tests"
MCP_SERVER="$REPO_ROOT/servers/dag/mcp-server"
PLUGIN_TOOL_PREFIX="mcp__plugin_gke-agentic-migration_migration-dag"

PROJECT="${E2E_GCP_PROJECT:-}"
BUCKET_BASE="${E2E_BUCKET_BASE:-}"
ADMIN_EMAIL="${E2E_ADMIN_EMAIL:-}"
PLATFORM_ENGINEERS="${E2E_PLATFORM_ENGINEERS:-}"
NEW_OWNER="${E2E_NEW_OWNER:-}"
SKIP_CC=false
SKIP_SMOKE=false
SKIP_AGY=false
KEEP_BUCKET=false
ASSUME_YES=false

usage() {
  cat <<EOF
Usage: tests/run_e2e.sh [options]

Options:
  --project <id>              GCP project for test buckets
                              (default: \$E2E_GCP_PROJECT, then gcloud config)
  --bucket-base <name>        Base bucket name; '-mcp'/'-cc' suffixes are appended
                              (default: \$E2E_BUCKET_BASE, then claude-local-test-bucket-<rand>)
  --admin <email>             Admin email for the workspace registry
                              (default: \$E2E_ADMIN_EMAIL, then gcloud account)
  --platform-engineers <a;b>  Semicolon-separated platform engineer emails
                              (default: \$E2E_PLATFORM_ENGINEERS, then admin email)
  --new-owner <email>         An email NOT in the registry, used by the assessment tier
                              to exercise blocker-owner registration. It is granted a
                              real IAM binding on the test bucket, so it must be a
                              principal the project can bind. Omitted: that leg is
                              skipped (default: \$E2E_NEW_OWNER)
  --skip-smoke                Skip the smoke tier
  --skip-cc                   Skip the Claude Code harness tier
  --skip-agy                  Skip the Antigravity SDK MCP-import tier
                              (that tier installs google-antigravity + needs Vertex ADC)
  --keep-bucket               Keep created test buckets for inspection
  -y, --yes                   Do not ask for confirmation
  -h, --help                  Show this help

Prerequisites: gcloud + ADC (gcloud auth application-default login), python3,
and the claude CLI unless --skip-cc.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)            PROJECT="$2"; shift 2 ;;
    --bucket-base)        BUCKET_BASE="$2"; shift 2 ;;
    --admin)              ADMIN_EMAIL="$2"; shift 2 ;;
    --platform-engineers) PLATFORM_ENGINEERS="$2"; shift 2 ;;
    --new-owner)          NEW_OWNER="$2"; shift 2 ;;
    --skip-smoke)         SKIP_SMOKE=true; shift ;;
    --skip-cc)            SKIP_CC=true; shift ;;
    --skip-agy)           SKIP_AGY=true; shift ;;
    --keep-bucket)        KEEP_BUCKET=true; shift ;;
    -y|--yes)             ASSUME_YES=true; shift ;;
    -h|--help)            usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage; exit 2 ;;
  esac
done

log()  { printf '\n\033[1;34m[e2e]\033[0m %s\n' "$*"; }
err()  { printf '\033[1;31m[e2e:ERROR]\033[0m %s\n' "$*" >&2; }

# ---------- prerequisites ----------
command -v gcloud  >/dev/null 2>&1 || { err "gcloud CLI not found in PATH."; exit 1; }
command -v python3 >/dev/null 2>&1 || { err "python3 not found in PATH."; exit 1; }

ADC_PATH="${GOOGLE_APPLICATION_CREDENTIALS:-$HOME/.config/gcloud/application_default_credentials.json}"
if [[ ! -f "$ADC_PATH" ]]; then
  err "No Application Default Credentials found ($ADC_PATH)."
  err "Run: gcloud auth application-default login"
  exit 1
fi

HAVE_CLAUDE=true
command -v claude >/dev/null 2>&1 || HAVE_CLAUDE=false
if ! $SKIP_CC && ! $HAVE_CLAUDE; then
  err "claude CLI not found. Install Claude Code or pass --skip-cc."
  exit 1
fi

# ---------- config resolution ----------
if [[ -z "$PROJECT" ]]; then
  PROJECT="$(gcloud config get-value project 2>/dev/null)"
fi
if [[ -z "$PROJECT" || "$PROJECT" == "(unset)" ]]; then
  err "No GCP project. Pass --project, set E2E_GCP_PROJECT, or gcloud config set project."
  exit 1
fi
if [[ -z "$ADMIN_EMAIL" ]]; then
  ADMIN_EMAIL="$(gcloud config get-value account 2>/dev/null)"
fi
if [[ -z "$ADMIN_EMAIL" || "$ADMIN_EMAIL" == "(unset)" ]]; then
  err "No admin email. Pass --admin or set E2E_ADMIN_EMAIL."
  exit 1
fi
[[ -z "$PLATFORM_ENGINEERS" ]] && PLATFORM_ENGINEERS="$ADMIN_EMAIL"

RUN_ID="$(openssl rand -hex 3 2>/dev/null || od -An -N3 -tx1 /dev/urandom | tr -d ' \n')"
[[ -z "$BUCKET_BASE" ]] && BUCKET_BASE="claude-local-test-bucket-$RUN_ID"
WORKSPACE_BASE="claude-local-test-$RUN_ID"
MCP_BUCKET="${BUCKET_BASE}-mcp"
KNOWLEDGE_BUCKET="${BUCKET_BASE}-knowledge"
ASSESSMENT_BUCKET="${BUCKET_BASE}-assessment"
CC_APPROVE_BUCKET="${BUCKET_BASE}-cc-approve"
CC_REJECT_BUCKET="${BUCKET_BASE}-cc-reject"

cat <<EOF

  GKE Agentic Migration — local E2E suite
  ---------------------------------------
  Repo:               $REPO_ROOT
  GCP project:        $PROJECT
  Admin:              $ADMIN_EMAIL
  Platform engineers: $PLATFORM_ENGINEERS
  New owner:          ${NEW_OWNER:-(none — assessment tier skips member registration)}
  Test buckets:       gs://$MCP_BUCKET / gs://$KNOWLEDGE_BUCKET / gs://$ASSESSMENT_BUCKET $( $SKIP_CC || echo "/ gs://$CC_APPROVE_BUCKET / gs://$CC_REJECT_BUCKET" )
  Tiers:              $( $SKIP_SMOKE || echo -n "smoke " )mcp-direct knowledge-session assessment-flow $( $SKIP_CC || echo -n "cc-plugin cc-approve cc-reject " )$( $SKIP_AGY || echo -n "agy-mcp-import" )
  Cleanup:            $( $KEEP_BUCKET && echo "keep buckets" || echo "delete buckets on exit" )

EOF

if ! $ASSUME_YES; then
  if [[ -t 0 ]]; then
    read -r -p "This will create (and delete) GCS buckets in project '$PROJECT'. Proceed? [y/N] " answer
    [[ "$answer" == y* || "$answer" == Y* ]] || { log "Aborted."; exit 1; }
  else
    log "Non-interactive stdin; proceeding (pass -y to silence this note)."
  fi
fi

# ---------- test venv ----------
VENV="$TESTS_DIR/.venv"
if [[ ! -d "$VENV" ]]; then
  log "Creating tests virtualenv at $VENV"
  python3 -m venv "$VENV" || { err "venv creation failed"; exit 1; }
fi
"$VENV/bin/pip" install --quiet --index-url https://pypi.org/simple \
  -r "$TESTS_DIR/requirements.txt" || { err "pip install failed"; exit 1; }
PY="$VENV/bin/python"

# ---------- safety & cleanup ----------
# bootstrap_migration deletes both session caches — the legacy machine-global
# file AND the cwd-scoped one for this invocation directory (the MCP server
# children inherit our cwd) — and join_ledger rewrites the scoped one.
# Preserve any real session: back up both, restore both, and remove a scoped
# file the run created where none existed.
LEDGER_CFG="$HOME/.ledger_config.yaml"
SCOPED_CFG="$HOME/.ledger_config.d/$("$PY" -c \
  'import hashlib,os;print(hashlib.sha256(os.getcwd().encode()).hexdigest()[:16])').yaml"
LEDGER_BAK=""
SCOPED_BAK=""
SCOPED_EXISTED=false
if [[ -f "$LEDGER_CFG" ]]; then
  LEDGER_BAK="$(mktemp)"
  cp "$LEDGER_CFG" "$LEDGER_BAK"
  log "Backed up existing $LEDGER_CFG (will restore on exit)."
fi
if [[ -f "$SCOPED_CFG" ]]; then
  SCOPED_EXISTED=true
  SCOPED_BAK="$(mktemp)"
  cp "$SCOPED_CFG" "$SCOPED_BAK"
  log "Backed up existing $SCOPED_CFG (will restore on exit)."
fi

CREATED_BUCKETS=()
cleanup() {
  if [[ -n "$LEDGER_BAK" ]]; then
    cp "$LEDGER_BAK" "$LEDGER_CFG" 2>/dev/null
    rm -f "$LEDGER_BAK"
    log "Restored $LEDGER_CFG."
  fi
  if $SCOPED_EXISTED; then
    cp "$SCOPED_BAK" "$SCOPED_CFG" 2>/dev/null
    rm -f "$SCOPED_BAK"
    log "Restored $SCOPED_CFG."
  elif [[ -f "$SCOPED_CFG" ]]; then
    rm -f "$SCOPED_CFG"
    log "Removed $SCOPED_CFG (created by this run)."
  fi
  if [[ ${#CREATED_BUCKETS[@]} -gt 0 ]]; then
    if $KEEP_BUCKET; then
      log "Keeping test buckets (delete manually):"
      for b in "${CREATED_BUCKETS[@]}"; do
        echo "    gcloud storage rm -r gs://$b"
      done
    else
      for b in "${CREATED_BUCKETS[@]}"; do
        log "Deleting test bucket gs://$b"
        gcloud storage rm -r "gs://$b" >/dev/null 2>&1 \
          || err "Failed to delete gs://$b — delete manually: gcloud storage rm -r gs://$b"
      done
    fi
  fi
}
trap cleanup EXIT

RESULTS=()
record() { RESULTS+=("$1|$2"); }  # record <name> <PASS|FAIL|SKIP>

bucket_exists() {
  gcloud storage buckets describe "gs://$1" >/dev/null 2>&1
}

track_bucket_if_created() {
  if bucket_exists "$1"; then CREATED_BUCKETS+=("$1"); fi
}

verify_bucket_objects() {
  local bucket="$1" listing
  listing="$(gcloud storage ls -r "gs://$bucket/" 2>/dev/null)" || return 1
  grep -q "workspace_registry.yaml" <<<"$listing" \
    && grep -q "platform_dag.json" <<<"$listing" \
    && grep -q "platform/onboarding/state.json" <<<"$listing"
}

# Reads an IAM policy as JSON on stdin. Exits 0 if <role> is granted to <email>
# under a condition whose expression contains <expr>; pass no <expr> to require
# an *unconditional* binding. Matches the bare email so the check does not have
# to reimplement the server's user:/serviceAccount: prefixing.
policy_grants() {
  python3 -c '
import json, sys
role, email, expr = sys.argv[1], sys.argv[2], sys.argv[3]
for b in json.load(sys.stdin).get("bindings", []):
    if b.get("role") != role:
        continue
    if not any(m.split(":", 1)[-1] == email for m in b.get("members", [])):
        continue
    got = (b.get("condition") or {}).get("expression", "")
    if (expr in got) if expr else not got:
        sys.exit(0)
sys.exit(1)
' "$1" "$2" "${3:-}"
}

# Ground truth for the cloud-side access boundary: the prefix isolation the
# developer and platform personas rely on has to come from GCS, not from the
# server agreeing to check the registry first.
verify_ledger_iam() {
  local bucket="$1" admin="$2" platform="$3" bucket_policy folder

  for folder in platform workloads; do
    if ! gcloud storage managed-folders describe "gs://$bucket/$folder/" >/dev/null 2>&1; then
      err "ledger IAM: managed folder $folder/ was not created"
      return 1
    fi
  done

  if [[ "$(gcloud storage buckets describe "gs://$bucket" \
            --format='value(uniform_bucket_level_access)' 2>/dev/null)" != "True" ]]; then
    err "ledger IAM: uniform bucket-level access is off, so managed folders are not enforced"
    return 1
  fi

  bucket_policy="$(gcloud storage buckets get-iam-policy "gs://$bucket" --format=json 2>/dev/null)" || {
    err "ledger IAM: could not read the bucket policy"
    return 1
  }
  if ! policy_grants "roles/storage.admin" "$admin" <<<"$bucket_policy"; then
    err "ledger IAM: $admin does not hold roles/storage.admin on the bucket"
    return 1
  fi
  if ! policy_grants "roles/storage.objectViewer" "$admin" \
        "objects/workspace_registry.yaml" <<<"$bucket_policy"; then
    err "ledger IAM: registry read is not granted, or is not scoped by a condition"
    return 1
  fi
  if policy_grants "roles/storage.objectViewer" "$admin" <<<"$bucket_policy"; then
    err "ledger IAM: an unconditional objectViewer binding exposes every prefix"
    return 1
  fi

  if ! gcloud storage managed-folders get-iam-policy "gs://$bucket/platform/" --format=json 2>/dev/null \
        | policy_grants "roles/storage.objectAdmin" "$platform"; then
    err "ledger IAM: $platform does not hold objectAdmin on platform/"
    return 1
  fi
}

refuse_preexisting_bucket() {
  if bucket_exists "$1"; then
    err "Bucket gs://$1 already exists; refusing to reuse it. Pass a different --bucket-base."
    return 1
  fi
  return 0
}

json_array_from_semicolons() {
  local IFS=';' out="" item
  read -ra items <<<"$1"
  for item in "${items[@]}"; do
    [[ -n "$item" ]] && out+="\"$item\","
  done
  echo "[${out%,}]"
}

# =====================================================================
# Tier 1: smoke
# =====================================================================
if ! $SKIP_SMOKE; then
  if $HAVE_CLAUDE; then
    log "smoke: claude plugin validate"
    if claude plugin validate "$REPO_ROOT"; then
      record "smoke/plugin-validate" PASS
    else
      record "smoke/plugin-validate" FAIL
    fi
  else
    record "smoke/plugin-validate" SKIP
  fi

  log "smoke: manifest & skills validation"
  if "$PY" "$TESTS_DIR/smoke/test_manifest.py" "$REPO_ROOT"; then
    record "smoke/manifest" PASS
  else
    record "smoke/manifest" FAIL
  fi

  log "smoke: MCP server boot & tool exposure (first run may build servers/dag/.venv)"
  if E2E_MCP_SERVER="$MCP_SERVER" "$PY" "$TESTS_DIR/smoke/test_server_boot.py"; then
    record "smoke/server-boot" PASS
  else
    record "smoke/server-boot" FAIL
  fi

  # Runs after server-boot, which is what builds the venv this test borrows.
  log "smoke: malformed DAG refuses to start (sandboxed copy of servers/)"
  if E2E_MCP_SERVER="$MCP_SERVER" "$PY" "$TESTS_DIR/smoke/test_startup_validation.py"; then
    record "smoke/startup-validation" PASS
  else
    record "smoke/startup-validation" FAIL
  fi
else
  record "smoke" SKIP
fi

# =====================================================================
# Tier 2: mcp-direct e2e (deterministic bucket-creation gate)
# =====================================================================
log "mcp-direct: bootstrap flow -> gs://$MCP_BUCKET"
if refuse_preexisting_bucket "$MCP_BUCKET"; then
  if E2E_MCP_SERVER="$MCP_SERVER" \
     E2E_WORKSPACE="${WORKSPACE_BASE}-mcp" \
     E2E_GCP_PROJECT="$PROJECT" \
     E2E_LEDGER_BUCKET="gs://$MCP_BUCKET" \
     E2E_ADMINS="$ADMIN_EMAIL" \
     E2E_PLATFORM_ENGINEERS="$PLATFORM_ENGINEERS" \
     "$PY" "$TESTS_DIR/e2e/mcp_direct/bootstrap_flow.py"; then
    if ! verify_bucket_objects "$MCP_BUCKET"; then
      err "mcp-direct: flow reported success but bucket/objects not found"
      record "e2e/mcp-direct" FAIL
    elif ! verify_ledger_iam "$MCP_BUCKET" "$ADMIN_EMAIL" "${PLATFORM_ENGINEERS%%;*}"; then
      err "mcp-direct: flow reported success but the ledger has no cloud-side access boundary"
      record "e2e/mcp-direct" FAIL
    else
      log "mcp-direct: ground truth OK (bucket + 3 ledger objects, managed folders + IAM)"
      record "e2e/mcp-direct" PASS
    fi
  else
    record "e2e/mcp-direct" FAIL
  fi
  track_bucket_if_created "$MCP_BUCKET"
else
  record "e2e/mcp-direct" FAIL
fi

# Own bucket: this tier rewrites the ledger's current state, so it must not
# run against the graph mcp-direct just drove to completion.
log "knowledge-session: serve-once-per-process + re-send after restart -> gs://$KNOWLEDGE_BUCKET"
if refuse_preexisting_bucket "$KNOWLEDGE_BUCKET"; then
  if E2E_MCP_SERVER="$MCP_SERVER" \
     E2E_WORKSPACE="${WORKSPACE_BASE}-knw" \
     E2E_GCP_PROJECT="$PROJECT" \
     E2E_LEDGER_BUCKET="gs://$KNOWLEDGE_BUCKET" \
     E2E_ADMINS="$ADMIN_EMAIL" \
     E2E_PLATFORM_ENGINEERS="$PLATFORM_ENGINEERS" \
     "$PY" "$TESTS_DIR/e2e/mcp_direct/knowledge_session.py"; then
    record "e2e/knowledge-session" PASS
  else
    record "e2e/knowledge-session" FAIL
  fi
  track_bucket_if_created "$KNOWLEDGE_BUCKET"
else
  record "e2e/knowledge-session" FAIL
fi

# Own bucket for the same reason as knowledge-session: this tier parks the
# ledger on STATE_ASSESSMENT and then drives it through to landing zone.
log "assessment-flow: assessment approval elicitation + blocker guardrail -> gs://$ASSESSMENT_BUCKET"
if [[ -z "$NEW_OWNER" ]]; then
  log "assessment-flow: no --new-owner, so the unregistered-owner leg will be skipped"
fi
if refuse_preexisting_bucket "$ASSESSMENT_BUCKET"; then
  if E2E_MCP_SERVER="$MCP_SERVER" \
     E2E_WORKSPACE="${WORKSPACE_BASE}-asm" \
     E2E_GCP_PROJECT="$PROJECT" \
     E2E_LEDGER_BUCKET="gs://$ASSESSMENT_BUCKET" \
     E2E_ADMINS="$ADMIN_EMAIL" \
     E2E_PLATFORM_ENGINEERS="$PLATFORM_ENGINEERS" \
     E2E_NEW_OWNER="$NEW_OWNER" \
     "$PY" "$TESTS_DIR/e2e/mcp_direct/assessment_flow.py"; then
    record "e2e/assessment-flow" PASS
  else
    record "e2e/assessment-flow" FAIL
  fi
  track_bucket_if_created "$ASSESSMENT_BUCKET"
else
  record "e2e/assessment-flow" FAIL
fi

# =====================================================================
# Tier 3: cc e2e (Claude Code harness: plugin import + skill exposure +
#         elicitation answered via an Elicitation hook)
#
# Two cases, both driving the real Claude Code headless harness with a
# project-scoped Elicitation hook (no interactive UI):
#   approve  hook accepts -> DAG parks on STATE_WORKSPACE_ADMIN, bucket created
#   reject   hook declines -> flow stops in STATE_ABORTED, bucket NOT created
# =====================================================================

# run_cc_case <label> <mode: approve|reject> <bucket> <workspace>
# Records e2e/cc-<label>; tracks the bucket for cleanup if it was created.
run_cc_case() {
  local label="$1" mode="$2" bucket="$3" workspace="$4"
  local tier="e2e/cc-$label"

  log "cc-$label: bootstrap flow via Claude Code ($mode) -> gs://$bucket"

  if ! refuse_preexisting_bucket "$bucket"; then
    record "$tier" FAIL
    return
  fi

  local workdir hook_output
  workdir="$(mktemp -d "/tmp/gke-migration-e2e-cc-$label.XXXXXX")"
  mkdir -p "$workdir/.claude"

  if [[ "$mode" == "approve" ]]; then
    hook_output='{"hookSpecificOutput":{"hookEventName":"Elicitation","action":"accept","content":{"approved":true}}}'
  else
    hook_output='{"hookSpecificOutput":{"hookEventName":"Elicitation","action":"decline"}}'
  fi

  cat > "$workdir/elicitation-hook.sh" <<HOOK
#!/bin/bash
cat >> "$workdir/elicitation-input.log"
echo '$hook_output'
HOOK
  chmod +x "$workdir/elicitation-hook.sh"

  cat > "$workdir/.claude/settings.json" <<SETTINGS
{
  "hooks": {
    "Elicitation": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "$workdir/elicitation-hook.sh",
            "timeout": 60
          }
        ]
      }
    ]
  }
}
SETTINGS

  local admins_json pe_json prompt
  admins_json="$(json_array_from_semicolons "$ADMIN_EMAIL")"
  pe_json="$(json_array_from_semicolons "$PLATFORM_ENGINEERS")"
  prompt="This is an automated E2E test. Perform exactly these steps in order and nothing else:
1. Output the exact name of the available skill whose name contains 'bootstrapping'.
2. Call the MCP tool bootstrap_migration.
3. Call the MCP tool initialize_ledger with exactly these arguments: workspace_name=\"$workspace\", gcp_project=\"$PROJECT\", ledger_bucket=\"gs://$bucket\", roles={\"admins\": $admins_json, \"platform_engineers\": $pe_json, \"developers\": []}.
4. Call the MCP tool get_next_stage and output its result verbatim.
If any tool call returns an error, output the error verbatim and stop. Do not retry and do not call any other tools."

  local rc=1
  (
    cd "$workdir" && claude -p "$prompt" \
      --plugin-dir "$REPO_ROOT" \
      --allowedTools "${PLUGIN_TOOL_PREFIX}__*" \
      --max-turns 15 \
      --output-format json > result.json 2> stderr.log
  ) && rc=0

  local ok=true
  if [[ $rc -ne 0 ]]; then
    err "cc-$label: claude exited non-zero (see $workdir/stderr.log)"
    ok=false
  fi
  # Shared: the server's elicitation actually reached the hook.
  if $ok && [[ ! -s "$workdir/elicitation-input.log" ]]; then
    err "cc-$label: Elicitation hook was never invoked"
    ok=false
  fi

  if [[ "$mode" == "approve" ]]; then
    if $ok && ! grep -q "STATE_WORKSPACE_ADMIN" "$workdir/result.json"; then
      err "cc-$label: transcript does not report STATE_WORKSPACE_ADMIN (see $workdir/result.json)"
      ok=false
    fi
    if $ok && ! verify_bucket_objects "$bucket"; then
      err "cc-$label: ground truth failed — bucket/objects not found in GCS"
      ok=false
    fi
    $ok && log "cc-$label: ground truth OK (skill exposed, elicitation approved, bucket + objects present)"
  else
    # reject: the flow must stop and must NOT create the bucket.
    if $ok && ! grep -qE "STATE_ABORTED|rejected ledger creation" "$workdir/result.json"; then
      err "cc-$label: no evidence the flow stopped after decline (expected STATE_ABORTED / rejection; see $workdir/result.json)"
      ok=false
    fi
    if $ok && bucket_exists "$bucket"; then
      err "cc-$label: SECURITY — bucket gs://$bucket was created despite the user declining"
      ok=false
    fi
    $ok && log "cc-$label: ground truth OK (elicitation declined, flow aborted, no bucket created)"
  fi

  if $ok; then
    record "$tier" PASS
    rm -rf "$workdir"
  else
    err "cc-$label: artifacts kept at $workdir"
    record "$tier" FAIL
  fi
  track_bucket_if_created "$bucket"
}

# Deterministic check that the plugin loads under the harness and exposes the
# bootstrapping skill. Kept separate from the elicitation cases because a
# declined run legitimately stops early and its final message need not echo
# the skill name.
run_cc_plugin_probe() {
  local workdir out
  workdir="$(mktemp -d /tmp/gke-migration-e2e-cc-probe.XXXXXX)"
  log "cc-plugin: plugin import & skill exposure under the harness"
  (
    cd "$workdir" && claude -p \
      "Do not call any tools. List the exact names of all available skills, one per line." \
      --plugin-dir "$REPO_ROOT" \
      --max-turns 1 \
      --output-format json > result.json 2> stderr.log
  )
  if grep -q "bootstrapping" "$workdir/result.json" 2>/dev/null; then
    log "cc-plugin: bootstrapping skill exposed"
    record "e2e/cc-plugin" PASS
    rm -rf "$workdir"
  else
    err "cc-plugin: bootstrapping skill not exposed (artifacts at $workdir)"
    record "e2e/cc-plugin" FAIL
  fi
}

if ! $SKIP_CC; then
  run_cc_plugin_probe
  run_cc_case "approve" approve "$CC_APPROVE_BUCKET" "${WORKSPACE_BASE}-cc-approve"
  run_cc_case "reject"  reject  "$CC_REJECT_BUCKET"  "${WORKSPACE_BASE}-cc-reject"
else
  record "e2e/cc" SKIP
fi

# =====================================================================
# Tier 4 (opt-in): agy — Antigravity SDK MCP import + tool exposure
#
# Scoped to what the SDK supports: register the stdio MCP server and call
# bootstrap_migration + get_next_stage (neither triggers an elicitation).
# The elicitation/bucket flow is NOT run here — google-antigravity 0.1.8 has
# no way to answer an MCP elicitation. See tests/README.md.
# =====================================================================
if ! $SKIP_AGY; then
  log "agy: installing google-antigravity into $TESTS_DIR/.venv-agy (from PyPI)"
  AGY_VENV="$TESTS_DIR/.venv-agy"
  AGY_OK=true
  if [[ ! -d "$AGY_VENV" ]]; then
    python3 -m venv "$AGY_VENV" || AGY_OK=false
  fi
  if $AGY_OK; then
    "$AGY_VENV/bin/pip" install --quiet --index-url https://pypi.org/simple/ \
      -r "$TESTS_DIR/requirements-agy.txt" || AGY_OK=false
  fi

  if ! $AGY_OK; then
    err "agy: SDK install failed (see above); is PyPI reachable?"
    record "e2e/agy-mcp-import" FAIL
  else
    log "agy: MCP import & tool exposure via Antigravity SDK (Vertex, project $PROJECT)"
    if E2E_MCP_SERVER="$MCP_SERVER" \
       E2E_GCP_PROJECT="$PROJECT" \
       AGY_WORKSPACE="$(mktemp -d /tmp/gke-migration-e2e-agy.XXXXXX)" \
       "$AGY_VENV/bin/python" "$TESTS_DIR/e2e/agy/mcp_import.py"; then
      record "e2e/agy-mcp-import" PASS
    else
      err "agy: MCP import tier failed"
      record "e2e/agy-mcp-import" FAIL
    fi
  fi
fi

# =====================================================================
# Summary
# =====================================================================
OVERALL=0
printf '\n  %-24s %s\n  %-24s %s\n' "Tier" "Result" "----" "------"
for entry in "${RESULTS[@]}"; do
  name="${entry%%|*}"
  status="${entry##*|}"
  printf '  %-24s %s\n' "$name" "$status"
  [[ "$status" == FAIL ]] && OVERALL=1
done
if [[ $OVERALL -eq 0 ]]; then
  log "E2E suite PASSED"
else
  err "E2E suite FAILED"
fi
exit $OVERALL
