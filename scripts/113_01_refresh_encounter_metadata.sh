: <<'NOTE'
ESECUZIONE DA SHELL
-------------------
bash /data/Mongo_Sh_Script/113_01_refresh_encounter_metadata.sh \
  2>&1 | tee /data/Mongo_Sh_Script/EXP/run_$(date +%F_%H%M%S).log

SPOSTARE IL FILE DA WINDOWS A UBUNTU (KUBERNETES)
------------------------------------------------
Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR_TEST\113_01_refresh_encounter_metadata.sh" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/113_01_refresh_encounter_metadata.sh'



OUTPUT:
[2026-04-13T14:39:09+00:00] Starting...
BASE_URL=https://ellipse-t.auslromagna.it
MAP_FILE_PATH=/data/Mongo_Sh_Script/EXP/sio_ihub_hc40_adt_keys_map.js
LOG_DIR=/data/Mongo_Sh_Script/EXP
PROCESS_SIO=false PROCESS_IHUB=false PROCESS_HC40_ADT=true
NUM_ENCOUNTER_CONCURRENT=10
RUN_LOG_PATH=/data/Mongo_Sh_Script/EXP/run_2026-04-13_143909.log
[2026-04-13T14:39:09+00:00] Loading mappings from map file...
[2026-04-13T14:39:09+00:00] Selected distinct encounters: 16
[2026-04-13T14:39:09+00:00] Login OK. Token length: 1160
[2026-04-13T14:39:09+00:00] Processing with concurrency=10 ...
xargs: warning: options --max-args and --replace/-I/-i are mutually exclusive, ignoring previous --max-args value
[BATCH] processed=10 batchOk=2 batchFail=8
[BATCH] processed=16 batchOk=2 batchFail=4
FAIL PUT Encounter/HC40-ADT.8ff0b0d5-b3e6-4527-a851-83ffbd4501cf status=422
FAIL PUT Encounter/HC40-ADT.629cd208-dbe2-46a1-a2a8-d5e3508def26 status=422
FAIL PUT Encounter/HC40-ADT.1054847e-bcd2-4ef0-aff6-b97d28c608fe status=422
FAIL PUT Encounter/HC40-ADT.0b4e8080-9393-45c7-b3b2-b11ec9ab7f5b status=422
FAIL PUT Encounter/HC40-ADT.3f6cb795-1f4d-470b-9dac-bd0978ca19ef status=422
FAIL PUT Encounter/HC40-ADT.1f99538f-115e-4c3b-8754-76c33bdc7285 status=400
OK Encounter/HC40-ADT.44392e61-5c90-44d0-93d6-d696732da62f putStatus=200
FAIL PUT Encounter/HC40-ADT.8280f2b3-cb0e-4cb6-830e-045774baf97c status=400
OK Encounter/HC40-ADT.0cfc9a08-f2e3-46ed-b60e-8b44ddea240e putStatus=200
FAIL PUT Encounter/HC40-ADT.add9b36a-f5a5-4d21-b710-83ec6ef59c6b status=422
FAIL PUT Encounter/HC40-ADT.104995c4-c616-473e-bd4a-91953e794c43 status=400
FAIL PUT Encounter/HC40-ADT.dd96c964-4587-4a59-a363-45de6c292466 status=422
OK Encounter/HC40-ADT.b567ccd0-adc1-4a4a-b361-dcbaf3398b22 putStatus=200
FAIL PUT Encounter/HC40-ADT.db9b2099-a872-48c9-87c0-46bc24aeb7c4 status=400
FAIL PUT Encounter/HC40-ADT.de6d9df1-1eae-4529-b675-0d8103d3cdd8 status=422
OK Encounter/HC40-ADT.fb4bdafe-d39f-40bd-90cf-c80fa445b649 putStatus=200
[2026-04-13T14:39:12+00:00] Done.
Processed log: /data/Mongo_Sh_Script/EXP/processed.jsonl
Not processed log: /data/Mongo_Sh_Script/EXP/not_processed.jsonl
Run log: /data/Mongo_Sh_Script/EXP/run_2026-04-13_143909.log

NOTE

###############################################################################
# 113_01_refresh_encounter_metadata.sh
#
# CONTENUTO / OBIETTIVO
# ---------------------
# Questo script automatizza la “bonifica/refresh” degli Encounter sul CDR
# partendo da una mappa SIO -> (IHUB oppure HC40-ADT).
#
# La mappa è contenuta nel file:
#   /data/Mongo_Sh_Script/EXP/sio_ihub_hc40_adt_keys_map.js
#
# ed è una lista di stringhe nel formato:
#   "Encounter/SIO.0800...R#Encounter/IHUB.2024....-....."
#   "Encounter/SIO.0800...R#Encounter/HC40-ADT.<uuid>"
#
# DOVE:
#   - Encounter/SIO.*      rappresenta il duplicato da eliminare (spesso NON presente in ADT)
#   - Encounter/IHUB.*     rappresenta l’Encounter “buono” che rimpiazza SIO
#   - Encounter/HC40-ADT.* rappresenta l’Encounter “buono” che rimpiazza SIO
#
# NOTA: lo script NON modifica il file .js di mapping e NON richiede export/module.exports.
#       Legge il file come testo ed estrae le stringhe "Encounter/..." presenti nell’array.
#
# COSA FA OPERATIVAMENTE (PIPELINE)
# --------------------------------
# Per ogni Encounter selezionato, esegue queste chiamate REST:
#
#   1) LOGIN (una sola volta all’avvio, poi riuso token)
#      POST  {BASE_URL}/api/token/login
#      Body: {"username":"...","password":"..."}
#      Output: token Bearer (stringa)
#
#   2) GET DA ADT (lettura risorsa)
#      GET   {BASE_URL}/api/adt/fhir/Encounter/{id}
#      Header: Authorization: Bearer <token>
#      Header: Accept: application/fhir+json
#
#   3) PUT SU CDR / index-recuperi (scrittura/refresh risorsa)
#      PUT   {BASE_URL}/api/index-recuperi/fhir/Encounter/{id}
#      Header: Authorization: Bearer <token>
#      Header: Content-Type: application/json
#      Body: esattamente il JSON ottenuto dal GET ADT
#
# SELEZIONE TIPOLOGIE (BOOLEANE)
# ------------------------------
# Puoi scegliere quali encounter processare con:
#   PROCESS_SIO=true|false
#   PROCESS_IHUB=true|false
#   PROCESS_HC40_ADT=true|false
#
# La selezione avviene così:
#   - se PROCESS_SIO=true:      usa la parte sinistra (prima di #) "Encounter/SIO..."
#   - se PROCESS_IHUB=true:     usa la parte destra (dopo #) solo se "Encounter/IHUB..."
#   - se PROCESS_HC40_ADT=true: usa la parte destra (dopo #) solo se "Encounter/HC40-ADT..."
#
# Gli Encounter selezionati vengono poi:
#   - deduplicati (distinti) via sort -u
#
# CONCORRENZA
# -----------
# Lo script usa concorrenza tramite xargs:
#   NUM_ENCOUNTER_CONCURRENT=N
# cioè lavora fino a N Encounter in parallelo.
# Inoltre stampa un riepilogo “batch” ogni N completati (OK/FAIL).
#
# GESTIONE ERRORI / ROBUSTEZZA
# ----------------------------
# - Se GET ADT fallisce (404, 500, ecc.) l’Encounter viene registrato in not_processed.jsonl
#   con phase=GET_ADT, e lo script prosegue.
# - Se PUT CDR fallisce (non-2xx) l’Encounter viene registrato in not_processed.jsonl
#   con phase=PUT_CDR, e lo script prosegue.
# - Se il token risulta scaduto (401/403) su GET o PUT:
#     - viene eseguito un nuovo login
#     - la richiesta viene ritentata UNA volta
#
# FILE DI OUTPUT / LOG
# --------------------
# Nella cartella /data/Mongo_Sh_Script/EXP vengono creati:
#   - run_YYYY-MM-DD_HHMMSS.log      : log completo dell’esecuzione
#   - processed.jsonl                : 1 riga JSON per ogni Encounter processato con successo
#   - not_processed.jsonl            : 1 riga JSON per ogni Encounter non processato (con motivo)
#
# I JSONL includono anche la tipologia (SIO/IHUB/HC40-ADT).
#
# ESECUZIONE (ESEMPI)
# -------------------
# Default (da config): lavorare solo HC40-ADT
#   bash /data/Mongo_Sh_Script/113_01_refresh_encounter_metadata.sh
#
# Override via env (esempio: HC40-ADT + IHUB, concorrenza 20):
#   PROCESS_HC40_ADT=true PROCESS_IHUB=true PROCESS_SIO=false NUM_ENCOUNTER_CONCURRENT=20 \
#   bash /data/Mongo_Sh_Script/113_01_refresh_encounter_metadata.sh
#
###############################################################################
#!/usr/bin/env bash
set -eo pipefail

# ======== PIPELINE CONTEXT LOAD ========
CONTEXT_PATH="${STEP_CONTEXT_PATH:-${1:-}}"
if [[ ! -f "$CONTEXT_PATH" ]]; then
  echo "[ERROR] Context file not found: $CONTEXT_PATH"
  exit 1
fi

jqval() { jq -r "$1 // empty" "$CONTEXT_PATH"; }

# PATH/ENV/PARAMS DA PIPELINE
runId=$(jqval '.runId')
expDir=$(jqval '.paths.expDir')
params=$(jq '.params' "$CONTEXT_PATH")
mapFileName=$(jq -r '.params.mapFileName // "sio_ihub_hc40_adt_keys_map.js"' "$CONTEXT_PATH")
baseUrl=$(jq -r '.params.baseUrl // "https://ellipse-t.auslromagna.it"' "$CONTEXT_PATH")
username=$(jq -r '.params.username // "user_cct"' "$CONTEXT_PATH")
password=$(jq -r '.params.password // "Admin123"' "$CONTEXT_PATH")
processSIO=$(jq -r '.params.processSIO // "false"' "$CONTEXT_PATH")
processIHUB=$(jq -r '.params.processIHUB // "false"' "$CONTEXT_PATH")
processHC40_ADT=$(jq -r '.params.processHC40_ADT // "true"' "$CONTEXT_PATH")
numEncounterConcurrent=$(jq -r '.params.numEncounterConcurrent // 10' "$CONTEXT_PATH")

# ==== MODIFICA: usa la sottodirectory run associata all'esecuzione ====
runDir="${expDir}/${runId}"
logDir="${runDir}"
mkdir -p "$logDir"
mapFilePath="${runDir}/${mapFileName}"
processedFile="${logDir}/refresh_encounter_metadata_processed.jsonl"
notProcessedFile="${logDir}/refresh_encounter_metadata_not_processed.jsonl"
runLogFile="${logDir}/run_${runId}_refresh_encounter_metadata.log"

# ======== LOGGING/AUDIT CONTEXT ========
log() { echo "[$(date -Is)] $*"; }
elog() { echo "[$(date -Is)] ERROR: $*" >&2; }

exec > >(tee -a "$runLogFile") 2>&1

log "START: runId=$runId baseUrl=$baseUrl mapFile=$mapFilePath processSIO=$processSIO processIHUB=$processIHUB processHC40_ADT=$processHC40_ADT numEncounterConcurrent=$numEncounterConcurrent"

: > "$processedFile"
: > "$notProcessedFile"

if [[ ! -f "$mapFilePath" ]]; then
  elog "Mapping file not found: $mapFilePath"
  exit 2
fi

# --------- Build encounter list (dedup) ----------
log "Loading mappings from: $mapFilePath"
map_lines="$(grep -oE '"Encounter/[^"]+"' "$mapFilePath" | tr -d '"')"

if [[ -z "$map_lines" ]]; then
  elog "No Encounter mappings in $mapFilePath"
  exit 2
fi

tmpList="$(mktemp)"
trap 'rm -f "$tmpList" "$tmpList.dedup"' EXIT

while IFS= read -r row; do
  left="${row%%#*}"
  right=""
  if [[ "$row" == *"#"* ]]; then right="${row#*#}"; fi

  if [[ "$processSIO" == "true" && "$left" == Encounter/SIO.* ]]; then
    echo "$left" >> "$tmpList"
  fi
  if [[ "$processIHUB" == "true" && "$right" == Encounter/IHUB.* ]]; then
    echo "$right" >> "$tmpList"
  fi
  if [[ "$processHC40_ADT" == "true" && "$right" == Encounter/HC40-ADT.* ]]; then
    echo "$right" >> "$tmpList"
  fi
done <<< "$map_lines"

sort -u "$tmpList" | sed '/^$/d' > "$tmpList.dedup"
total=$(wc -l < "$tmpList.dedup" | tr -d ' ')
log "Selected distinct encounters: $total"

if [[ "$total" -eq 0 ]]; then
  log "Nothing to do, exiting."
  exit 0
fi

# --------- Login (autentica una sola volta ma riloggia in caso di 401/403) ---------
login() {
  curl -sS -X POST "$baseUrl/api/token/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"$username\",\"password\":\"$password\"}"
}

TOKEN="$(login | tr -d '\r\n')"
if [[ -z "$TOKEN" ]]; then
  elog "Login empty token"
  exit 2
fi
log "Login OK. Token length: ${#TOKEN}"

# ==== MODIFICA: esporta tutte le variabili di ambiente necessarie per xargs ====
export BASE_URL="$baseUrl"
export USERNAME="$username"
export PASSWORD="$password"
export NOT_PROCESSED_PATH="$notProcessedFile"
export PROCESSED_PATH="$processedFile"

# --------- Worker function per chiamate Encounter (GET+PUT) ---------
cat > "$tmpList.worker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

ts() { date -Is; }
enc_type() {
  case "$1" in
    Encounter/SIO.*) echo "SIO" ;;
    Encounter/IHUB.*) echo "IHUB" ;;
    Encounter/HC40-ADT.*) echo "HC40-ADT" ;;
    *) echo "UNKNOWN" ;;
  esac
}
strip_prefix() { echo "${1#Encounter/}"; }
jsonl_append() { printf '%s\n' "$2" >> "$1"; }
login() {
  curl -sS -X POST "$BASE_URL/api/token/login" \
    -H 'Content-Type: application/json' \
    -d "{\"username\":\"$USERNAME\",\"password\":\"$PASSWORD\"}"
}
one() {
  encRef="$1"
  type="$(enc_type "$encRef")"
  encId="$(strip_prefix "$encRef")"

  tmpBody="$(mktemp)"; tmpHdr="$(mktemp)"; trap 'rm -f "$tmpBody" "$tmpHdr"' RETURN

  curl -sS -D "$tmpHdr" -o "$tmpBody" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/fhir+json" \
    "$BASE_URL/api/adt/fhir/Encounter/$encId" || true

  status="$(head -n 1 "$tmpHdr" | awk '{print $2}' 2>/dev/null || echo "")"
  [[ -z "$status" ]] && status="000"

  if [[ "$status" == "401" || "$status" == "403" ]]; then
    TOKEN="$(login | tr -d '\r\n')"
    curl -sS -D "$tmpHdr" -o "$tmpBody" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Accept: application/fhir+json" \
      "$BASE_URL/api/adt/fhir/Encounter/$encId" || true
    status="$(head -n 1 "$tmpHdr" | awk '{print $2}' 2>/dev/null || echo "000")"
  fi

  if [[ "$status" != "200" ]]; then
    diag="$(jq -r '.issue[0].diagnostics // empty' "$tmpBody" 2>/dev/null || true)"
    [[ -z "$diag" ]] && diag="$(head -c 800 "$tmpBody" 2>/dev/null | tr '\n' ' ' || true)"

    jsonl_append "$NOT_PROCESSED_PATH" "$(jq -cn \
      --arg ts "$(ts)" \
      --arg encounterRef "$encRef" \
      --arg encounterId "$encId" \
      --arg type "$type" \
      --arg phase "GET_ADT" \
      --arg httpStatus "$status" \
      --arg diagnostics "$diag" \
      '{ts:$ts, encounterRef:$encounterRef, encounterId:$encounterId, type:$type, phase:$phase, httpStatus:($httpStatus|tonumber?), diagnostics:$diagnostics}')"
    echo "FAIL GET $encRef status=$status"
    return 0
  fi

  rt="$(jq -r '.resourceType // empty' "$tmpBody" 2>/dev/null || true)"
  if [[ "$rt" != "Encounter" ]]; then
    jsonl_append "$NOT_PROCESSED_PATH" "$(jq -cn \
      --arg ts "$(ts)" \
      --arg encounterRef "$encRef" \
      --arg encounterId "$encId" \
      --arg type "$type" \
      --arg phase "GET_ADT_PARSE" \
      --arg diagnostics "Unexpected resourceType=$rt" \
      '{ts:$ts, encounterRef:$encounterRef, encounterId:$encounterId, type:$type, phase:$phase, diagnostics:$diagnostics}')"
    echo "FAIL PARSE $encRef"
    return 0
  fi

  tmpPutBody="$(mktemp)"; tmpPutHdr="$(mktemp)"; trap 'rm -f "$tmpBody" "$tmpHdr" "$tmpPutBody" "$tmpPutHdr"' RETURN

  curl -sS -D "$tmpPutHdr" -o "$tmpPutBody" \
    -X PUT "$BASE_URL/api/index-recuperi/fhir/Encounter/$encId" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    --data-binary @"$tmpBody" || true

  putStatus="$(head -n 1 "$tmpPutHdr" | awk '{print $2}' 2>/dev/null || echo "")"
  [[ -z "$putStatus" ]] && putStatus="000"

  if [[ "$putStatus" == "401" || "$putStatus" == "403" ]]; then
    TOKEN="$(login | tr -d '\r\n')"
    curl -sS -D "$tmpPutHdr" -o "$tmpPutBody" \
      -X PUT "$BASE_URL/api/index-recuperi/fhir/Encounter/$encId" \
      -H "Authorization: Bearer $TOKEN" \
      -H "Content-Type: application/json" \
      --data-binary @"$tmpBody" || true
    putStatus="$(head -n 1 "$tmpPutHdr" | awk '{print $2}' 2>/dev/null || echo "000")"
  fi

  if [[ ! "$putStatus" =~ ^2 ]]; then
    diag2="$(jq -r '.issue[0].diagnostics // empty' "$tmpPutBody" 2>/dev/null || true)"
    [[ -z "$diag2" ]] && diag2="$(head -c 800 "$tmpPutBody" 2>/dev/null | tr '\n' ' ' || true)"

    jsonl_append "$NOT_PROCESSED_PATH" "$(jq -cn \
      --arg ts "$(ts)" \
      --arg encounterRef "$encRef" \
      --arg encounterId "$encId" \
      --arg type "$type" \
      --arg phase "PUT_CDR" \
      --arg httpStatus "$putStatus" \
      --arg diagnostics "$diag2" \
      '{ts:$ts, encounterRef:$encounterRef, encounterId:$encounterId, type:$type, phase:$phase, httpStatus:($httpStatus|tonumber?), diagnostics:$diagnostics}')"
    echo "FAIL PUT $encRef status=$putStatus"
    return 0
  fi

  jsonl_append "$PROCESSED_PATH" "$(jq -cn \
    --arg ts "$(ts)" \
    --arg encounterRef "$encRef" \
    --arg encounterId "$encId" \
    --arg type "$type" \
    --arg putStatus "$putStatus" \
    '{ts:$ts, encounterRef:$encounterRef, encounterId:$encounterId, type:$type, phase:"DONE", putStatus:($putStatus|tonumber?)}')"

  echo "OK $encRef putStatus=$putStatus"
}
one "$1"
EOF
chmod +x "$tmpList.worker"

awk_batch='
BEGIN {n=0; ok=0; fail=0; total=0;}
{
  total++; n++;
  if ($1=="OK") ok++; else if ($1=="FAIL") fail++;
  print $0;
  if (n=='"$numEncounterConcurrent"' ) {
    printf("[BATCH] processed=%d batchOk=%d batchFail=%d\n", total, ok, fail) > "/dev/stderr";
    n=0; ok=0; fail=0;
  }
}
END {
  if (n>0) printf("[BATCH] processed=%d batchOk=%d batchFail=%d\n", total, ok, fail) > "/dev/stderr";
}'

# ==== MODIFICA: passaggio variabili ambiente robusto nei worker ====
  cat "$tmpList.dedup" \
  | xargs -n 1 -P "$numEncounterConcurrent" -I {} env BASE_URL="$baseUrl" USERNAME="$username" PASSWORD="$password" TOKEN="$TOKEN" NOT_PROCESSED_PATH="$notProcessedFile" PROCESSED_PATH="$processedFile" "$tmpList.worker" "{}" \
  | awk "$awk_batch"
  
log "Done."
log "Processed log: $processedFile"
log "Not processed log: $notProcessedFile"
log "Run log: $runLogFile"

printf '{"type":"result","script":"113_01_refresh_encounter_metadata.sh","runId":"%s","processedFile":"%s","notProcessedFile":"%s","runLogFile":"%s","encounterCount":%s,"ts":"%s"}\n' \
  "$runId" "$processedFile" "$notProcessedFile" "$runLogFile" "$total" "$(date -Is)"