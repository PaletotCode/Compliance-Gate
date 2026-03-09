#!/usr/bin/env bash
set -u -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_DIR="$ROOT_DIR/retests/output"
REPORT_PATH="$OUTPUT_DIR/FRONTEND_INTEGRATION_FINAL_REPORT.md"
EXIT_CODES_PATH="$OUTPUT_DIR/verify_all_exit_codes.tsv"
FLOW_OUTPUT_JSON="$OUTPUT_DIR/main_view_full_flow_check.json"
LATENCY_OUTPUT_JSON="$OUTPUT_DIR/frontend_table_latency_metrics.json"
SCAN_RAW="$OUTPUT_DIR/frontend_url_scan_raw.txt"
SCAN_HITS="$OUTPUT_DIR/frontend_external_urls_hits.txt"
SCAN_GOOGLE="$OUTPUT_DIR/frontend_google_fonts_hits.txt"

API_BASE_URL="${VERIFY_API_BASE_URL:-http://localhost:8000}"
AUTH_USERNAME="${VERIFY_AUTH_USERNAME:-${AUTH_BOOTSTRAP_ADMIN_USERNAME:-admin}}"
AUTH_PASSWORD="${VERIFY_AUTH_PASSWORD:-${AUTH_BOOTSTRAP_ADMIN_PASSWORD:-Admin1234}}"
AUTH_CHECK_USERNAME="${VERIFY_AUTH_CHECK_USERNAME:-}"
AUTH_CHECK_PASSWORD="${VERIFY_AUTH_CHECK_PASSWORD:-AuthFlow123!}"
AUTO_START_DOCKER="${VERIFY_AUTO_START_DOCKER:-1}"
RUN_PLAYWRIGHT="${VERIFY_RUN_PLAYWRIGHT:-auto}"

if [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  PYTHON_BIN="${PYTHON_BIN:-python3}"
fi

mkdir -p "$OUTPUT_DIR"
printf "step\texit_code\tcommand\tlog_file\n" > "$EXIT_CODES_PATH"

declare -a STEP_NAMES
declare -a STEP_COMMANDS
declare -a STEP_LOGS
declare -a STEP_CODES
OVERALL_FAIL=0

record_step() {
  local step_name="$1"
  local command_str="$2"
  local log_path="$3"
  local exit_code="$4"

  STEP_NAMES+=("$step_name")
  STEP_COMMANDS+=("$command_str")
  STEP_LOGS+=("$log_path")
  STEP_CODES+=("$exit_code")

  printf "%s\t%s\t%s\t%s\n" "$step_name" "$exit_code" "$command_str" "$(basename "$log_path")" >> "$EXIT_CODES_PATH"

  if [[ "$exit_code" -ne 0 ]]; then
    OVERALL_FAIL=1
  fi
}

run_step() {
  local step_name="$1"
  shift
  local command_str="$*"
  local log_path="$OUTPUT_DIR/${step_name}.log"

  echo "[verify_all] RUN ${step_name}: ${command_str}"

  (
    cd "$ROOT_DIR"
    bash -lc "$command_str"
  ) >"$log_path" 2>&1
  local exit_code=$?

  record_step "$step_name" "$command_str" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  ${step_name}"
  else
    echo "[verify_all] FAIL ${step_name} (exit ${exit_code})"
  fi
}

wait_for_backend_health() {
  local timeout_seconds="$1"
  local waited=0

  while [[ "$waited" -lt "$timeout_seconds" ]]; do
    if curl -fsS "$API_BASE_URL/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done

  return 1
}

ensure_backend() {
  local log_path="$OUTPUT_DIR/backend_health.log"
  local exit_code=0

  {
    echo "API_BASE_URL=${API_BASE_URL}"
    echo "AUTO_START_DOCKER=${AUTO_START_DOCKER}"

    if wait_for_backend_health 10; then
      echo "Backend health check: already healthy."
    else
      echo "Backend health check: unhealthy at start."

      if [[ "$AUTO_START_DOCKER" == "1" ]]; then
        echo "Trying docker compose up -d db redis api ..."
        if ! (cd "$ROOT_DIR" && docker compose up -d db redis api); then
          echo "docker compose up failed."
          exit_code=1
        elif wait_for_backend_health 120; then
          echo "Backend became healthy after docker compose up."
        else
          echo "Backend still unhealthy after docker startup."
          exit_code=1
        fi
      else
        echo "AUTO_START_DOCKER disabled and backend is unhealthy."
        exit_code=1
      fi
    fi
  } >"$log_path" 2>&1

  record_step "backend_health" "curl ${API_BASE_URL}/health (auto-start docker if needed)" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  backend_health"
  else
    echo "[verify_all] FAIL backend_health (exit ${exit_code})"
  fi
}

scan_external_dependencies() {
  local log_path="$OUTPUT_DIR/frontend_external_dependency_scan.log"
  local exit_code=0

  {
    local -a targets=(
      "$ROOT_DIR/frontend/src"
      "$ROOT_DIR/frontend/public"
      "$ROOT_DIR/frontend/index.html"
    )
    local -a existing_targets=()

    for target in "${targets[@]}"; do
      if [[ -e "$target" ]]; then
        existing_targets+=("$target")
      fi
    done

    if [[ "${#existing_targets[@]}" -eq 0 ]]; then
      echo "No frontend targets found for URL scan."
      exit_code=1
    else
      echo "Scanning frontend for external URLs..."
      rg -n -S "https?://" "${existing_targets[@]}" > "$SCAN_RAW" || true

      grep -E "https?://" "$SCAN_RAW" \
        | grep -Ev "https?://(localhost|127\\.0\\.0\\.1)(:[0-9]+)?([/]|$)" \
        | grep -Ev "http://www\\.w3\\.org/2000/svg|http://www\\.w3\\.org/1999/xlink" \
        > "$SCAN_HITS" || true
      rg -n -S "fonts\\.googleapis\\.com|fonts\\.gstatic\\.com|@import\\s+url\\(['\"]https?://" "${existing_targets[@]}" > "$SCAN_GOOGLE" || true

      local external_count=0
      local google_count=0

      if [[ -s "$SCAN_HITS" ]]; then
        external_count="$(wc -l < "$SCAN_HITS" | tr -d ' ')"
      fi
      if [[ -s "$SCAN_GOOGLE" ]]; then
        google_count="$(wc -l < "$SCAN_GOOGLE" | tr -d ' ')"
      fi

      echo "external_url_hits=${external_count}"
      echo "google_fonts_hits=${google_count}"

      if [[ "$external_count" -gt 0 ]] || [[ "$google_count" -gt 0 ]]; then
        echo "External dependencies found."
        exit_code=1
      else
        echo "No external URL dependencies found in frontend runtime files."
      fi
    fi
  } >"$log_path" 2>&1

  record_step "frontend_external_url_scan" "rg scan for external frontend URLs (allowlist=localhost only)" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  frontend_external_url_scan"
  else
    echo "[verify_all] FAIL frontend_external_url_scan (exit ${exit_code})"
  fi
}

prepare_auth_check_user() {
  local log_path="$OUTPUT_DIR/frontend_prepare_auth_check_user.log"
  local exit_code=0

  if [[ -z "$AUTH_CHECK_USERNAME" ]]; then
    AUTH_CHECK_USERNAME="authcheck_$(date +%s)"
  fi

  "$PYTHON_BIN" - "$API_BASE_URL" "$AUTH_USERNAME" "$AUTH_PASSWORD" "$AUTH_CHECK_USERNAME" "$AUTH_CHECK_PASSWORD" >"$log_path" 2>&1 <<'PY'
import json
import sys

import requests

api_base, admin_user, admin_password, username, password = sys.argv[1:6]
session = requests.Session()

login = session.post(
    f"{api_base}/api/v1/auth/login",
    json={"username": admin_user, "password": admin_password},
    timeout=30,
)
login.raise_for_status()
payload = login.json()
if payload.get("mfa_required"):
    raise SystemExit("Admin login requires MFA; cannot auto-create auth check user.")

csrf = session.cookies.get("cg_csrf")
headers = {"Content-Type": "application/json"}
if csrf:
    headers["X-CSRF-Token"] = csrf

create_resp = session.post(
    f"{api_base}/api/v1/auth/users",
    headers=headers,
    json={"username": username, "password": password, "role": "DIRECTOR"},
    timeout=30,
)
if create_resp.status_code not in (201, 400):
    raise SystemExit(f"Failed to create auth check user: {create_resp.status_code} {create_resp.text}")

print(json.dumps({"auth_check_username": username, "status_code": create_resp.status_code}))
PY
  exit_code=$?

  record_step "frontend_prepare_auth_check_user" "create dedicated user for auth_flow_check" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  frontend_prepare_auth_check_user (${AUTH_CHECK_USERNAME})"
  else
    echo "[verify_all] FAIL frontend_prepare_auth_check_user (exit ${exit_code})"
  fi
}

verify_main_view_layout() {
  local log_path="$OUTPUT_DIR/frontend_main_view_structure.log"
  local exit_code=0

  {
    echo "Checking main_view file organization..."
    find "$ROOT_DIR/frontend/src" -type f | sort > "$OUTPUT_DIR/frontend_src_files_snapshot.txt"

    local out_of_place
    out_of_place="$(find "$ROOT_DIR/frontend/src" -type f | rg "main_view|MainView" | grep -Ev "^$ROOT_DIR/frontend/src/main_view/|^$ROOT_DIR/frontend/src/tests/main_view_" || true)"

    if [[ -n "$out_of_place" ]]; then
      echo "Found files related to main_view outside expected folders:"
      echo "$out_of_place"
      exit_code=1
    else
      echo "All main_view implementation files are under frontend/src/main_view or dedicated tests."
    fi
  } >"$log_path" 2>&1

  record_step "frontend_main_view_structure" "verify main_view files are isolated in frontend/src/main_view/" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  frontend_main_view_structure"
  else
    echo "[verify_all] FAIL frontend_main_view_structure (exit ${exit_code})"
  fi
}

probe_table_latency() {
  local log_path="$OUTPUT_DIR/frontend_table_latency_probe.log"
  local exit_code=0

  "$PYTHON_BIN" - "$API_BASE_URL" "$AUTH_USERNAME" "$AUTH_PASSWORD" "$FLOW_OUTPUT_JSON" "$LATENCY_OUTPUT_JSON" >"$log_path" 2>&1 <<'PY'
import http.cookiejar
import json
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

api_base, username, password, flow_json_path, latency_json_path = sys.argv[1:6]

flow_path = Path(flow_json_path)
if not flow_path.exists():
    raise SystemExit("main_view_full_flow_check.json not found; cannot measure table latency.")

flow = json.loads(flow_path.read_text(encoding="utf-8"))
dataset_version_id = str(flow.get("dataset_version_id") or "").strip()
if not dataset_version_id:
    raise SystemExit("dataset_version_id missing in main_view_full_flow_check.json")

jar = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar))

login_body = json.dumps({"username": username, "password": password}).encode("utf-8")
login_req = urllib.request.Request(
    f"{api_base}/api/v1/auth/login",
    data=login_body,
    headers={
        "Content-Type": "application/json",
        "Accept": "application/json",
    },
    method="POST",
)
with opener.open(login_req, timeout=30) as login_resp:
    login_payload = json.loads(login_resp.read().decode("utf-8") or "{}")

if isinstance(login_payload, dict) and login_payload.get("mfa_required"):
    raise SystemExit("Latency probe login returned MFA challenge; set VERIFY_AUTH_USERNAME/VERIFY_AUTH_PASSWORD for non-MFA user.")

samples = []
for _ in range(5):
    url = (
        f"{api_base}/api/v1/engine/tables/machines"
        f"?dataset_version_id={urllib.parse.quote(dataset_version_id)}&page=1&size=200"
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"}, method="GET")
    started = time.perf_counter()
    with opener.open(req, timeout=30) as response:
        response.read()
    elapsed = time.perf_counter() - started
    samples.append(round(elapsed, 4))

avg = round(sum(samples) / len(samples), 4)
result = {
    "dataset_version_id": dataset_version_id,
    "samples_seconds": samples,
    "avg_seconds": avg,
    "min_seconds": round(min(samples), 4),
    "max_seconds": round(max(samples), 4),
    "materialize_row_count": flow.get("materialize_row_count"),
    "table_total": flow.get("table_total"),
}

Path(latency_json_path).write_text(json.dumps(result, indent=2), encoding="utf-8")
print(json.dumps(result, indent=2))
PY
  exit_code=$?

  record_step "frontend_table_latency_probe" "probe average latency for GET /api/v1/engine/tables/machines (5 samples)" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  frontend_table_latency_probe"
  else
    echo "[verify_all] FAIL frontend_table_latency_probe (exit ${exit_code})"
  fi
}

generate_final_report() {
  local log_path="$OUTPUT_DIR/generate_frontend_integration_report.log"
  local exit_code=0

  "$PYTHON_BIN" - "$REPORT_PATH" "$EXIT_CODES_PATH" "$FLOW_OUTPUT_JSON" "$LATENCY_OUTPUT_JSON" "$SCAN_HITS" "$SCAN_GOOGLE" >"$log_path" 2>&1 <<'PY'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
exit_codes_path = Path(sys.argv[2])
flow_json_path = Path(sys.argv[3])
latency_json_path = Path(sys.argv[4])
scan_hits_path = Path(sys.argv[5])
scan_google_path = Path(sys.argv[6])

if not exit_codes_path.exists():
    raise SystemExit("verify_all_exit_codes.tsv not found.")

rows = []
for line in exit_codes_path.read_text(encoding="utf-8").splitlines()[1:]:
    if not line.strip():
        continue
    step, code, command, log_file = line.split("\t", 3)
    rows.append(
        {
            "step": step,
            "code": int(code),
            "command": command,
            "log_file": log_file,
        }
    )

by_step = {row["step"]: row for row in rows}

def step_ok(name: str) -> bool:
    return by_step.get(name, {}).get("code") == 0

flow = {}
if flow_json_path.exists():
    flow = json.loads(flow_json_path.read_text(encoding="utf-8"))

latency = {}
if latency_json_path.exists():
    latency = json.loads(latency_json_path.read_text(encoding="utf-8"))

external_hits = []
if scan_hits_path.exists():
    external_hits = [line for line in scan_hits_path.read_text(encoding="utf-8").splitlines() if line.strip()]

google_hits = []
if scan_google_path.exists():
    google_hits = [line for line in scan_google_path.read_text(encoding="utf-8").splitlines() if line.strip()]

def mark(ok: bool) -> str:
    return "PASS" if ok else "FAIL"

auth_ok = step_ok("backend_run_auth_retests") and step_ok("frontend_auth_flow_check")
profiles_ok = step_ok("frontend_main_view_profile_check")
preview_ok = step_ok("frontend_main_view_profile_check") and step_ok("frontend_main_view_full_flow_check")
ingest_ok = step_ok("frontend_main_view_full_flow_check") and int(flow.get("ingest_total_records") or 0) > 0
materialize_ok = step_ok("frontend_main_view_full_flow_check") and int(flow.get("materialize_row_count") or -1) >= 0
table_ok = step_ok("frontend_main_view_full_flow_check") and int(flow.get("table_total") or 0) > 0
summary_ok = step_ok("frontend_main_view_full_flow_check") and int(flow.get("summary_total") or 0) >= 0
export_ok = step_ok("frontend_main_view_full_flow_check") and int(flow.get("report_row_count") or 0) > 0
main_view_actions_ok = profiles_ok and ingest_ok and materialize_ok and table_ok and step_ok("frontend_main_view_structure")
external_ok = step_ok("frontend_external_url_scan")
playwright_present = "frontend_playwright_main_view" in by_step
playwright_ok = step_ok("frontend_playwright_main_view") if playwright_present else True

lines = []
lines.append("# FRONTEND_INTEGRATION_FINAL_REPORT")
lines.append("")
lines.append("## Status geral")
overall_ok = all(row["code"] == 0 for row in rows)
lines.append(f"- Resultado final do verify_all: **{'PASS' if overall_ok else 'FAIL'}**")
lines.append("")

lines.append("## Checklist de fluxos")
lines.append(f"- Auth completo (cookie + CSRF + MFA flow): **{mark(auth_ok)}**")
lines.append(f"- Profiles: **{mark(profiles_ok)}**")
lines.append(f"- Preview (raw/parsed + datasets preview): **{mark(preview_ok)}**")
lines.append(f"- Ingest: **{mark(ingest_ok)}**")
lines.append(f"- Materialize: **{mark(materialize_ok)}**")
lines.append(f"- Table final (CMDB): **{mark(table_ok)}**")
lines.append(f"- Summary/Filters: **{mark(summary_ok)}**")
lines.append(f"- Export (report/run): **{mark(export_ok)}**")
lines.append(f"- Main View com ações reais: **{mark(main_view_actions_ok)}**")
lines.append(f"- Playwright E2E (se presente): **{mark(playwright_ok)}**")
lines.append(f"- Sem dependências externas no frontend: **{mark(external_ok)}**")
lines.append("")

lines.append("## Comandos executados e exit codes")
lines.append("| Step | Exit Code | Comando | Log |")
lines.append("| --- | ---: | --- | --- |")
for row in rows:
    lines.append(
        f"| `{row['step']}` | `{row['code']}` | `{row['command']}` | [{row['log_file']}]({row['log_file']}) |"
    )
lines.append("")

lines.append("## Métricas mínimas")
lines.append(f"- `dataset_version_id`: `{flow.get('dataset_version_id', 'N/A')}`")
lines.append(f"- `ingest_total_records`: `{flow.get('ingest_total_records', 'N/A')}`")
lines.append(f"- `materialize_row_count`: `{flow.get('materialize_row_count', 'N/A')}`")
lines.append(f"- `table_total`: `{flow.get('table_total', 'N/A')}`")
lines.append(f"- `summary_total`: `{flow.get('summary_total', 'N/A')}`")
lines.append(f"- `report_row_count`: `{flow.get('report_row_count', 'N/A')}`")
lines.append(f"- `table_latency_avg_seconds` (5 amostras): `{latency.get('avg_seconds', 'N/A')}`")
lines.append(f"- `table_latency_min_seconds`: `{latency.get('min_seconds', 'N/A')}`")
lines.append(f"- `table_latency_max_seconds`: `{latency.get('max_seconds', 'N/A')}`")
lines.append("")

lines.append("## Evidências e outputs")
lines.append(f"- [verify_all_exit_codes.tsv]({exit_codes_path.name})")
if flow_json_path.exists():
    lines.append(f"- [{flow_json_path.name}]({flow_json_path.name})")
if latency_json_path.exists():
    lines.append(f"- [{latency_json_path.name}]({latency_json_path.name})")
if scan_hits_path.exists():
    lines.append(f"- [{scan_hits_path.name}]({scan_hits_path.name})")
if scan_google_path.exists():
    lines.append(f"- [{scan_google_path.name}]({scan_google_path.name})")
lines.append("")

lines.append("## Prova de sem dependências externas")
lines.append(
    f"- `frontend_external_urls_hits`: `{len(external_hits)}` ocorrência(s)"
)
lines.append(
    f"- `frontend_google_fonts_hits`: `{len(google_hits)}` ocorrência(s)"
)
if external_hits:
    lines.append("- Primeiras ocorrências de URL externa:")
    for item in external_hits[:10]:
        lines.append(f"  - `{item}`")
if google_hits:
    lines.append("- Primeiras ocorrências de Google Fonts:")
    for item in google_hits[:10]:
        lines.append(f"  - `{item}`")
lines.append("")

lines.append("## Critério de DONE")
lines.append(
    "- `DONE` somente quando todos os itens acima estão em `PASS` e todos os exit codes são `0`."
)

report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
print(f"Report generated: {report_path}")
PY
  exit_code=$?

  record_step "frontend_integration_report" "generate FRONTEND_INTEGRATION_FINAL_REPORT.md" "$log_path" "$exit_code"

  if [[ "$exit_code" -eq 0 ]]; then
    echo "[verify_all] OK  frontend_integration_report"
  else
    echo "[verify_all] FAIL frontend_integration_report (exit ${exit_code})"
  fi
}

main() {
  ensure_backend

  run_step "backend_pytest" "\"$PYTHON_BIN\" -m pytest"
  run_step "backend_run_auth_retests" "\"$PYTHON_BIN\" retests/scripts/run_auth_retests.py"
  run_step "backend_run_engine_retests" "\"$PYTHON_BIN\" retests/scripts/run_engine_retests.py"
  run_step "backend_run_rbac_retests" "\"$PYTHON_BIN\" retests/scripts/run_rbac_retests.py"
  ensure_backend
  run_step "backend_run_retests" "WORKSPACE=\"$ROOT_DIR/workspace\" API_BASE_URL=\"$API_BASE_URL\" \"$PYTHON_BIN\" retests/scripts/run_retests.py"
  run_step "backend_run_parity_retests" "WORKSPACE=\"$ROOT_DIR/workspace\" API_BASE_URL=\"$API_BASE_URL\" \"$PYTHON_BIN\" retests/scripts/run_parity_retests.py"

  ensure_backend
  run_step "frontend_build" "cd frontend && npm run build"
  run_step "frontend_test" "cd frontend && npm test"
  prepare_auth_check_user
  run_step "frontend_auth_flow_check" "AUTH_CHECK_BASE_URL=\"$API_BASE_URL\" AUTH_CHECK_USERNAME=\"$AUTH_CHECK_USERNAME\" AUTH_CHECK_PASSWORD=\"$AUTH_CHECK_PASSWORD\" node frontend/scripts/auth_flow_check.ts"
  run_step "frontend_main_view_profile_check" "MAIN_VIEW_CHECK_BASE_URL=\"$API_BASE_URL\" MAIN_VIEW_CHECK_USERNAME=\"$AUTH_USERNAME\" MAIN_VIEW_CHECK_PASSWORD=\"$AUTH_PASSWORD\" node frontend/scripts/main_view_profile_check.ts"
  run_step "frontend_main_view_full_flow_check" "MAIN_VIEW_CHECK_BASE_URL=\"$API_BASE_URL\" MAIN_VIEW_CHECK_USERNAME=\"$AUTH_USERNAME\" MAIN_VIEW_CHECK_PASSWORD=\"$AUTH_PASSWORD\" node frontend/scripts/main_view_full_flow_check.ts"

  if [[ "$RUN_PLAYWRIGHT" == "1" ]] || { [[ "$RUN_PLAYWRIGHT" == "auto" ]] && [[ -f "$ROOT_DIR/frontend/e2e/main_view_full_flow.e2e.ts" ]]; }; then
    run_step "frontend_playwright_main_view" "cd frontend && npm run e2e:main-view"
  fi

  verify_main_view_layout
  scan_external_dependencies
  probe_table_latency
  generate_final_report

  if [[ "$OVERALL_FAIL" -eq 0 ]]; then
    echo "[verify_all] DONE (exit 0)"
    exit 0
  fi

  echo "[verify_all] FAILED (exit 1)"
  exit 1
}

main "$@"
