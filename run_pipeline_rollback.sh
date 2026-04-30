#!/usr/bin/env bash
set -Eeuo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_pipeline_rollback.sh --config <pipeline_rollback_script.json> --env <envName> \
    [--resume|--rerun-all] \
    [--stop-on-error|--continue-on-error] \
    [--tee-to-console|--no-tee-to-console] \
    [--dry-run|--no-dry-run]

Defaults:
  --resume
  continueOnError defaults from config (recommended default: true)
  teeToConsole defaults from config
  dryRun defaults from config (settings.dryRunDefault) or false if missing

Notes:
  - This rollback pipeline automatically targets the latest run of the main pipeline:
      state/pipeline_all_script/<env>/latest_run.json
    and uses exports from:
      <expDir>/<sourceRunId>
EOF
}

CONFIG=""
ENV_NAME=""
MODE="resume"            # resume | rerun-all
CLI_CONTINUE_ON_ERROR="" # empty => use config
CLI_TEE_TO_CONSOLE=""    # empty => use config
CLI_DRY_RUN=""           # empty => use config

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config) CONFIG="${2:-}"; shift 2 ;;
    --env) ENV_NAME="${2:-}"; shift 2 ;;
    --resume) MODE="resume"; shift ;;
    --rerun-all) MODE="rerun-all"; shift ;;
    --stop-on-error) CLI_CONTINUE_ON_ERROR="false"; shift ;;
    --continue-on-error) CLI_CONTINUE_ON_ERROR="true"; shift ;;
    --tee-to-console) CLI_TEE_TO_CONSOLE="true"; shift ;;
    --no-tee-to-console) CLI_TEE_TO_CONSOLE="false"; shift ;;
    --dry-run) CLI_DRY_RUN="true"; shift ;;
    --no-dry-run) CLI_DRY_RUN="false"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1"; usage; exit 2 ;;
  esac
done

if [[ -z "${CONFIG}" ]]; then echo "Missing --config"; usage; exit 2; fi
if [[ ! -f "${CONFIG}" ]]; then echo "Config file not found: ${CONFIG}"; exit 2; fi
if [[ -z "${ENV_NAME}" ]]; then echo "Missing --env <envName>"; usage; exit 2; fi

require_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Missing command: $1"; exit 2; }; }
require_cmd jq
require_cmd date
require_cmd mkdir
require_cmd printf
require_cmd tee

# timeout is optional (we will detect it)
has_timeout="false"
if command -v timeout >/dev/null 2>&1; then has_timeout="true"; fi

# ---------- Read config ----------
pipelineName="$(jq -r '.pipelineName' "${CONFIG}")"
pipelineVersion="$(jq -r '.version // ""' "${CONFIG}")"

mongoshPath="$(jq -r '.tools.mongoshPath // "/data/mongosh/bin/mongosh"' "${CONFIG}")"

baseDir="$(jq -r '.paths.baseDir // "/data/Mongo_Sh_Script"' "${CONFIG}")"
scriptsDir="$(jq -r '.paths.scriptsDir // ( .paths.baseDir // "/data/Mongo_Sh_Script" )' "${CONFIG}")"
expDir="$(jq -r '.paths.expDir // ( .paths.baseDir + "/EXP")' "${CONFIG}")"
logsDir="$(jq -r '.paths.logsDir // ( .paths.baseDir + "/logs")' "${CONFIG}")"
stateDir="$(jq -r '.paths.stateDir // ( .paths.baseDir + "/state")' "${CONFIG}")"

locking="$(jq -r '.settings.locking // true' "${CONFIG}")"
teeToConsole_cfg="$(jq -r '.settings.teeToConsole // false' "${CONFIG}")"
defaultTimeoutSec="$(jq -r '.settings.defaultTimeoutSec // 0' "${CONFIG}")"
continueOnError_cfg="$(jq -r '.settings.continueOnError // true' "${CONFIG}")"
dryRun_cfg="$(jq -r '.settings.dryRunDefault // false' "${CONFIG}")"

if [[ -n "${CLI_CONTINUE_ON_ERROR}" ]]; then continueOnError="${CLI_CONTINUE_ON_ERROR}"; else continueOnError="${continueOnError_cfg}"; fi
if [[ -n "${CLI_TEE_TO_CONSOLE}" ]]; then teeToConsole="${CLI_TEE_TO_CONSOLE}"; else teeToConsole="${teeToConsole_cfg}"; fi
if [[ -n "${CLI_DRY_RUN}" ]]; then dryRun="${CLI_DRY_RUN}"; else dryRun="${dryRun_cfg}"; fi

# Multi-env mongo config
uriBase="$(jq -r --arg env "${ENV_NAME}" '.envs[$env].mongo.uriBase // empty' "${CONFIG}")"
dbName="$(jq -r --arg env "${ENV_NAME}" '.envs[$env].mongo.dbName // empty' "${CONFIG}")"
uriParams="$(jq -r --arg env "${ENV_NAME}" '.envs[$env].mongo.uriParams // ""' "${CONFIG}")"

if [[ -z "${pipelineName}" || "${pipelineName}" == "null" ]]; then echo "pipelineName missing"; exit 2; fi
if [[ -z "${uriBase}" ]]; then echo "env '${ENV_NAME}' not found or envs[${ENV_NAME}].mongo.uriBase missing"; exit 2; fi
if [[ -z "${dbName}" ]]; then echo "env '${ENV_NAME}' not found or envs[${ENV_NAME}].mongo.dbName missing"; exit 2; fi

mongoUri="${uriBase%/}/${dbName}"
if [[ -n "${uriParams}" && "${uriParams}" != "null" ]]; then mongoUri="${mongoUri}?${uriParams}"; fi

# ---------- Determine source run of main pipeline (pipeline_all_script) ----------
sourcePipelineName="pipeline_all_script"
sourceLatestRunPath="${stateDir}/${sourcePipelineName}/${ENV_NAME}/latest_run.json"

if [[ ! -f "${sourceLatestRunPath}" ]]; then
  echo "ERROR: Cannot find source latest_run.json for main pipeline:"
  echo "  ${sourceLatestRunPath}"
  echo "Run the main pipeline at least once for this env, or create latest_run.json."
  exit 2
fi

sourceRunId="$(jq -r '.runId // empty' "${sourceLatestRunPath}")"
if [[ -z "${sourceRunId}" || "${sourceRunId}" == "null" ]]; then
  echo "ERROR: source runId missing in: ${sourceLatestRunPath}"
  exit 2
fi

sourceExpDir="${expDir%/}/${sourceRunId}"

# ---------- State dirs (for rollback pipeline) ----------
pipelineStateDir="${stateDir}/${pipelineName}/${ENV_NAME}"
mkdir -p "${pipelineStateDir}"

latestRunPath="${pipelineStateDir}/latest_run.json"
lockPath="${pipelineStateDir}/run.lock"

if [[ "${locking}" == "true" ]]; then
  if [[ -f "${lockPath}" ]]; then
    echo "LOCK EXISTS: ${lockPath}"
    echo "Another run may be active. If stale, remove it."
    exit 3
  fi
  echo "$$" > "${lockPath}"
fi

cleanup() {
  local code=$?
  if [[ "${locking}" == "true" ]]; then rm -f "${lockPath}" >/dev/null 2>&1 || true; fi
  exit $code
}
trap cleanup EXIT

new_run_id() { date -u +"%Y%m%dT%H%M%SZ"; }

runId=""
resumeFromRunId=""

if [[ "${MODE}" == "rerun-all" ]]; then
  runId="$(new_run_id)"
else
  if [[ -f "${latestRunPath}" ]]; then resumeFromRunId="$(jq -r '.runId // ""' "${latestRunPath}" 2>/dev/null || true)"; fi
  if [[ -n "${resumeFromRunId}" && "${resumeFromRunId}" != "null" ]]; then runId="${resumeFromRunId}"; else runId="$(new_run_id)"; fi
fi

runDir="${pipelineStateDir}/${runId}"
runLogsDir="${logsDir}/${pipelineName}/${ENV_NAME}/${runId}"
mkdir -p "${runDir}" "${runLogsDir}" "${expDir}"

masterLog="${runLogsDir}/master.log"
runStatePath="${runDir}/run_state.json"
runtimePath="${runDir}/runtime.json"

log() {
  local msg="$1"
  local ts
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "[${ts}] ${msg}" | tee -a "${masterLog}" >/dev/null
}

cat > "${latestRunPath}" <<EOF
{
  "pipelineName": $(jq -Rn --arg v "${pipelineName}" '$v'),
  "env": $(jq -Rn --arg v "${ENV_NAME}" '$v'),
  "runId": $(jq -Rn --arg v "${runId}" '$v'),
  "updatedAt": $(jq -Rn --arg v "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" '$v')
}
EOF

cat > "${runtimePath}" <<EOF
{
  "runId": $(jq -Rn --arg v "${runId}" '$v'),
  "pipelineName": $(jq -Rn --arg v "${pipelineName}" '$v'),
  "pipelineVersion": $(jq -Rn --arg v "${pipelineVersion}" '$v'),
  "env": $(jq -Rn --arg v "${ENV_NAME}" '$v'),
  "mongo": {
    "uri": $(jq -Rn --arg v "${mongoUri}" '$v'),
    "uriBase": $(jq -Rn --arg v "${uriBase}" '$v'),
    "dbName": $(jq -Rn --arg v "${dbName}" '$v'),
    "uriParams": $(jq -Rn --arg v "${uriParams}" '$v')
  },
  "paths": {
    "baseDir": $(jq -Rn --arg v "${baseDir}" '$v'),
    "scriptsDir": $(jq -Rn --arg v "${scriptsDir}" '$v'),
    "expDir": $(jq -Rn --arg v "${expDir}" '$v'),
    "logsDir": $(jq -Rn --arg v "${logsDir}" '$v'),
    "stateDir": $(jq -Rn --arg v "${stateDir}" '$v'),
    "pipelineStateDir": $(jq -Rn --arg v "${pipelineStateDir}" '$v'),
    "runDir": $(jq -Rn --arg v "${runDir}" '$v'),
    "runLogsDir": $(jq -Rn --arg v "${runLogsDir}" '$v')
  },
  "settings": {
    "mode": $(jq -Rn --arg v "${MODE}" '$v'),
    "continueOnError": ${continueOnError},
    "teeToConsole": ${teeToConsole},
    "locking": ${locking},
    "defaultTimeoutSec": ${defaultTimeoutSec},
    "hasTimeoutCmd": ${has_timeout},
    "dryRun": ${dryRun}
  },
  "rollback": {
    "sourcePipelineName": $(jq -Rn --arg v "${sourcePipelineName}" '$v'),
    "sourceLatestRunPath": $(jq -Rn --arg v "${sourceLatestRunPath}" '$v'),
    "sourceRunId": $(jq -Rn --arg v "${sourceRunId}" '$v'),
    "sourceExpDir": $(jq -Rn --arg v "${sourceExpDir}" '$v')
  },
  "generatedAt": $(jq -Rn --arg v "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" '$v')
}
EOF

# ---------- init run_state.json ----------
if [[ "${MODE}" == "rerun-all" || ! -f "${runStatePath}" ]]; then
  jq -n \
    --arg runId "${runId}" \
    --arg pipelineName "${pipelineName}" \
    --arg pipelineVersion "${pipelineVersion}" \
    --arg env "${ENV_NAME}" \
    --arg configPath "${CONFIG}" \
    --arg createdAt "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    '{
      runId: $runId,
      pipelineName: $pipelineName,
      pipelineVersion: $pipelineVersion,
      env: $env,
      configPath: $configPath,
      status: "PENDING",
      createdAt: $createdAt,
      startedAt: null,
      endedAt: null,
      steps: []
    }' > "${runStatePath}"

  stepsLen="$(jq '.steps | length' "${CONFIG}")"
  for ((i=0; i<stepsLen; i++)); do
    stepId="$(jq -r ".steps[$i].id" "${CONFIG}")"
    stepName="$(jq -r ".steps[$i].name" "${CONFIG}")"
    stepEnabled="$(jq -r ".steps[$i].enabled" "${CONFIG}")"
    stepFile="$(jq -r ".steps[$i].file" "${CONFIG}")"
    stepTimeout="$(jq -r ".steps[$i].timeoutSec // empty" "${CONFIG}")"
    stepParams="$(jq ".steps[$i].params // {}" "${CONFIG}")"

    if [[ -z "${stepTimeout}" || "${stepTimeout}" == "null" ]]; then
      stepTimeout="${defaultTimeoutSec}"
    fi

    jq \
      --arg id "${stepId}" \
      --arg name "${stepName}" \
      --arg file "${stepFile}" \
      --argjson enabled "${stepEnabled}" \
      --argjson timeoutSec "${stepTimeout}" \
      --argjson params "${stepParams}" \
      '
      .steps += [{
        id: ($id|tonumber),
        name: $name,
        file: $file,
        enabled: $enabled,
        status: "PENDING",
        startedAt: null,
        endedAt: null,
        exitCode: null,
        durationMs: null,
        logPath: null,
        timeoutSec: $timeoutSec,
        params: $params
      }]
      ' "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
  done
fi

log "START: pipelineName=${pipelineName} env=${ENV_NAME} runId=${runId} mode=${MODE} continueOnError=${continueOnError} teeToConsole=${teeToConsole} dryRun=${dryRun}"
log "CONFIG: ${CONFIG}"
log "MONGO_URI: ${mongoUri}"
log "ROLLBACK_SOURCE: pipeline=${sourcePipelineName} latestRun=${sourceLatestRunPath} sourceRunId=${sourceRunId} sourceExpDir=${sourceExpDir}"

if [[ "$(jq -r '.startedAt' "${runStatePath}")" == "null" ]]; then
  jq --arg t "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
    '.startedAt=$t | .status="RUNNING"' \
    "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
fi

orderedIdxs="$(jq -r '.steps | to_entries | sort_by(.value.id) | .[].key' "${runStatePath}")"
overallFailed="false"

run_mongosh_step() {
  local stepTimeout="$1"
  local scriptPath="$2"
  if [[ "${stepTimeout}" -gt 0 && "${has_timeout}" == "true" ]]; then
    timeout --preserve-status "${stepTimeout}" "${mongoshPath}" "${mongoUri}" --quiet --file "${scriptPath}"
  else
    "${mongoshPath}" "${mongoUri}" --quiet --file "${scriptPath}"
  fi
}

for idx in ${orderedIdxs}; do
  enabled="$(jq -r ".steps[${idx}].enabled" "${runStatePath}")"
  stepStatus="$(jq -r ".steps[${idx}].status" "${runStatePath}")"
  stepId="$(jq -r ".steps[${idx}].id" "${runStatePath}")"
  stepName="$(jq -r ".steps[${idx}].name" "${runStatePath}")"
  stepFile="$(jq -r ".steps[${idx}].file" "${runStatePath}")"
  stepTimeout="$(jq -r ".steps[${idx}].timeoutSec // 0" "${runStatePath}")"

  if [[ "${enabled}" != "true" ]]; then
    if [[ "${stepStatus}" != "SKIPPED" ]]; then
      jq --arg t "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
        '.steps['"${idx}"'].status="SKIPPED"
         | .steps['"${idx}"'].startedAt=$t
         | .steps['"${idx}"'].endedAt=$t
         | .steps['"${idx}"'].exitCode=0
         | .steps['"${idx}"'].durationMs=0' \
        "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
    fi
    log "STEP_SKIP: id=${stepId} name=${stepName} (enabled=false)"
    continue
  fi

  if [[ "${MODE}" == "resume" && "${stepStatus}" == "SUCCESS" ]]; then
    log "STEP_RESUME_SKIP: id=${stepId} name=${stepName} (already SUCCESS)"
    continue
  fi

  scriptPath="${scriptsDir}/${stepFile}"
  if [[ ! -f "${scriptPath}" ]]; then
    log "STEP_ERROR: script not found: ${scriptPath}"
    jq --arg t "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" \
      '.steps['"${idx}"'].status="FAILED"
       | .steps['"${idx}"'].startedAt=$t
       | .steps['"${idx}"'].endedAt=$t
       | .steps['"${idx}"'].exitCode=2
       | .steps['"${idx}"'].durationMs=0' \
      "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
    overallFailed="true"
    if [[ "${continueOnError}" == "true" ]]; then
      log "CONTINUE_ON_ERROR: continuing to next step."
      continue
    else
      log "STOP_ON_ERROR: stopping rollback pipeline."
      break
    fi
  fi

  stepLog="${runLogsDir}/$(printf "%03d" "${stepId}")_${stepName}.log"
  contextPath="${runDir}/context_step_$(printf "%03d" "${stepId}").json"

  stepParams="$(jq ".steps[${idx}].params // {}" "${runStatePath}")"
  cat > "${contextPath}" <<EOF
{
  "runId": $(jq -Rn --arg v "${runId}" '$v'),
  "pipelineName": $(jq -Rn --arg v "${pipelineName}" '$v'),
  "pipelineVersion": $(jq -Rn --arg v "${pipelineVersion}" '$v'),
  "env": $(jq -Rn --arg v "${ENV_NAME}" '$v'),
  "step": {
    "id": ${stepId},
    "name": $(jq -Rn --arg v "${stepName}" '$v'),
    "file": $(jq -Rn --arg v "${stepFile}" '$v'),
    "enabled": true
  },
  "mongo": {
    "uri": $(jq -Rn --arg v "${mongoUri}" '$v'),
    "dbName": $(jq -Rn --arg v "${dbName}" '$v')
  },
  "paths": {
    "expDir": $(jq -Rn --arg v "${expDir}" '$v'),
    "runDir": $(jq -Rn --arg v "${runDir}" '$v'),
    "stepLogPath": $(jq -Rn --arg v "${stepLog}" '$v')
  },
  "rollback": {
    "sourcePipelineName": $(jq -Rn --arg v "${sourcePipelineName}" '$v'),
    "sourceRunId": $(jq -Rn --arg v "${sourceRunId}" '$v'),
    "sourceExpDir": $(jq -Rn --arg v "${sourceExpDir}" '$v')
  },
  "params": ${stepParams},
  "createdAt": $(jq -Rn --arg v "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" '$v')
}
EOF

  log "STEP_START: id=${stepId} name=${stepName} file=${stepFile} timeoutSec=${stepTimeout} log=${stepLog}"

  if [[ "${stepTimeout}" -gt 0 && "${has_timeout}" != "true" ]]; then
    log "STEP_NOTE: timeoutSec=${stepTimeout} requested but 'timeout' command not found; running without timeout."
  fi

  stepStartMs="$(date +%s%3N)"
  jq --arg t "$(date -u +"%Y-%m-%dT%H:%M:%SZ")" --arg lp "${stepLog}" \
    '.steps['"${idx}"'].status="RUNNING"
     | .steps['"${idx}"'].startedAt=$t
     | .steps['"${idx}"'].logPath=$lp' \
    "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"

  exitCode=0

  if [[ "${teeToConsole}" == "true" ]]; then
    (
      export RUNTIME_PATH="${runtimePath}" STEP_CONTEXT_PATH="${contextPath}"
      run_mongosh_step "${stepTimeout}" "${scriptPath}" 2>&1 | tee -a "${stepLog}"
    ) || exitCode=$?
  else
    (
      export RUNTIME_PATH="${runtimePath}" STEP_CONTEXT_PATH="${contextPath}"
      run_mongosh_step "${stepTimeout}" "${scriptPath}"
    ) >"${stepLog}" 2>&1 || exitCode=$?
  fi

  stepEndMs="$(date +%s%3N)"
  durationMs=$((stepEndMs - stepStartMs))
  endedAt="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

  if [[ "${exitCode}" -eq 0 ]]; then
    jq --arg t "${endedAt}" --argjson d "${durationMs}" \
      '.steps['"${idx}"'].status="SUCCESS"
       | .steps['"${idx}"'].endedAt=$t
       | .steps['"${idx}"'].exitCode=0
       | .steps['"${idx}"'].durationMs=$d' \
      "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
    log "STEP_SUCCESS: id=${stepId} name=${stepName} durationMs=${durationMs}"
  else
    jq --arg t "${endedAt}" --argjson d "${durationMs}" --argjson c "${exitCode}" \
      '.steps['"${idx}"'].status="FAILED"
       | .steps['"${idx}"'].endedAt=$t
       | .steps['"${idx}"'].exitCode=$c
       | .steps['"${idx}"'].durationMs=$d' \
      "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
    log "STEP_FAILED: id=${stepId} name=${stepName} exitCode=${exitCode} durationMs=${durationMs} (see ${stepLog})"
    overallFailed="true"
    if [[ "${continueOnError}" == "true" ]]; then
      log "CONTINUE_ON_ERROR: continuing to next step."
    else
      log "STOP_ON_ERROR: stopping rollback pipeline."
      break
    fi
  fi
done

endTs="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
finalStatus="SUCCESS"
finalExit=0
if [[ "${overallFailed}" == "true" ]]; then
  finalStatus="FAILED"
  finalExit=1
fi

jq --arg t "${endTs}" --arg s "${finalStatus}" '.endedAt=$t | .status=$s' \
  "${runStatePath}" > "${runStatePath}.tmp" && mv "${runStatePath}.tmp" "${runStatePath}"
log "END: status=${finalStatus} runId=${runId}"

# ---------- RESULT SUMMARY ----------
resultsSummaryPath="${runDir}/results_summary.jsonl"
: > "${resultsSummaryPath}"

log "RESULT_SUMMARY_START: scanning step logs for type=result JSON lines"
shopt -s nullglob
stepLogs=( "${runLogsDir}"/*.log )
shopt -u nullglob

resultsCount=0
for lf in "${stepLogs[@]}"; do
  while IFS= read -r line; do
    if echo "${line}" | jq -e 'type=="object" and .type=="result"' >/dev/null 2>&1; then
      echo "${line}" >> "${resultsSummaryPath}"
      resultsCount=$((resultsCount+1))
    fi
  done < "${lf}"
done

log "RESULT_SUMMARY_END: resultsFound=${resultsCount} path=${resultsSummaryPath}"

if [[ "${resultsCount}" -gt 0 ]]; then
  log "RESULT_SUMMARY_JSONL_BEGIN"
  cat "${resultsSummaryPath}" | tee -a "${masterLog}" >/dev/null
  log "RESULT_SUMMARY_JSONL_END"
fi

exit "${finalExit}"