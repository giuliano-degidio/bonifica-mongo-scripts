// Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
  --quiet ^
  --file "C:\Appo09\Mongo_Prod_EXP\099_01_03B_esportazione_su_file_encounter_ihub_hc40_adt_without_status_original.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
  --quiet ^
  --file "C:\Appo10\Mongo_Prod_EXP\099_01_03B_esportazione_su_file_encounter_ihub_hc40_adt_without_status_original.js"
*/

// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_01_03B_esportazione_su_file_encounter_ihub_hc40_adt_without_status_original.js"

Spostare il file da Windows a Ubuntu Kubernetes:
/*
Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\099_01_03B_esportazione_su_file_encounter_ihub_hc40_adt_without_status_original.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_01_03B_esportazione_su_file_encounter_ihub_hc40_adt_without_status_original.js'
*/

/*
log:
START: 2026-04-23T11:17:37.723Z | DEBUG=false | DEBUG_DOC_ID=(none) | MAP_PATH=C:\Appo10\Mongo_Prod_EXP\EXP\sio_ihub_hc40_adt_keys_map.js
LOG: logPath=C:\Appo10\Mongo_Prod_EXP\EXP\encounter_without_status_original.log | LOG_FILE_FLAGS=a
CONFIG: MAP_CHUNK_SIZE=1000 | BATCH_SIZE=2000 | LOG_EVERY=1000 | DEBUG_NO_WRITE=false
STATE: GLOBAL_STATE_PATH=C:\Appo10\Mongo_Prod_EXP\EXP/encounter_without_status_original.chunks.state.json
MAP_TOTAL: pairs=36 | chunkSize=1000 | chunkCount=1
GLOBAL_STATE: completedChunks=(none)
CHUNK_START: #1/1 | map_from=0 | map_to=35 | map_rows=36 | outPath=C:\Appo10\Mongo_Prod_EXP\EXP/encounter_without_status_original_chunk0001.jsonl | statePath=C:\Appo10\Mongo_Prod_EXP\EXP/encounter_without_status_original_chunk0001.state.json
CHUNK_KEYS: #1 | encounterIds=36 | badRows=0
CHUNK_RESUME: #1 | lastId=(null) | exported=0 | outFlags=w
BATCH_DONE: chunk#1 batch#1 | docs=36 | lastId=Encounter/IHUB.2024802062-425101 | chunkExported=19 | elapsed=0h 0m 2s
CHUNK_END: #1/1 | exported=19 | outPath=C:\Appo10\Mongo_Prod_EXP\EXP/encounter_without_status_original_chunk0001.jsonl | completed=true
END: 2026-04-23T11:17:40.038Z | elapsed=0h 0m 2s

*/

// SCOPO
// -----
// - Come 099_01_03, ma esporta il documento ORIGINALE (senza modifiche).
// - Prende gli encounter id di DESTRA dalla mappa sio_ihub_hc40_adt_keys_map.js.
// - Se in attribute_search manca l'elemento con name == "status:status", esporta l'intero documento
//   nel file encounter_without_status_original_chunkXXXX.jsonl.
// - Chunk + resume come 006_03B.
//
// Prima di rilanciare da zero: cancellare i file encounter_without_status_original* in EXP.

const fs = require("fs");

// ====== DEBUG ======
const DEBUG = false;
const DEBUG_DOC_ID = "";
const DEBUG_NO_WRITE = false;

// ====== LOG FILE ======
const LOG_TO_FILE = true;
const LOG_TO_CONSOLE = true;
//const logPath = "/data/Mongo_Sh_Script/EXP/encounter_without_status_original.log";
const logPath = "C:\\Appo10\\Mongo_Prod_EXP\\EXP\\encounter_without_status_original.log";
const LOG_FILE_FLAGS = "a";

// ====== INPUT MAPPA ======
//const MAP_PATH = "/data/Mongo_Sh_Script/EXP/sio_ihub_hc40_adt_keys_map.js";
const MAP_PATH = "C:\\Appo10\\Mongo_Prod_EXP\\EXP\\sio_ihub_hc40_adt_keys_map.js";
// ====== OUTPUT ======
//const OUT_DIR = "/data/Mongo_Sh_Script/EXP";
const OUT_DIR = "C:\\Appo10\\Mongo_Prod_EXP\\EXP";
const OUT_BASE = "encounter_without_status_original";
const OUT_EXT = ".jsonl";

// ====== CHUNK MAPPA ======
const MAP_CHUNK_SIZE = 1000;

// ====== RESUME ======
const GLOBAL_STATE_PATH = `${OUT_DIR}/${OUT_BASE}.chunks.state.json`;

// ====== BATCH / LOG ======
const BATCH_SIZE = 2000;
const LOG_EVERY = 1000;

// --- logger ---
const __logStream = LOG_TO_FILE
  ? fs.createWriteStream(logPath, { encoding: "utf8", flags: LOG_FILE_FLAGS })
  : null;

function logLine(s) {
  const line = String(s);
  if (LOG_TO_CONSOLE) print(line);
  if (__logStream) __logStream.write(line + "\n");
}
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}
function readJsonIfExists(path, fallback) {
  try {
    if (!fs.existsSync(path)) return fallback;
    const raw = fs.readFileSync(path, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    return fallback;
  }
}
function writeJson(path, obj) {
  fs.writeFileSync(path, JSON.stringify(obj, null, 2) + "\n", "utf8");
}
function pad4(n) { return String(n).padStart(4, "0"); }
function chunkOutPath(chunkNo1Based) {
  return `${OUT_DIR}/${OUT_BASE}_chunk${pad4(chunkNo1Based)}${OUT_EXT}`;
}
function chunkStatePath(chunkNo1Based) {
  return `${OUT_DIR}/${OUT_BASE}_chunk${pad4(chunkNo1Based)}.state.json`;
}

function extractRightEncounterIds(pairs) {
  const ids = [];
  let bad = 0;
  for (const row of pairs) {
    if (typeof row !== "string") { bad++; continue; }
    const idx = row.indexOf("#");
    if (idx < 1) { bad++; continue; }
    const right = row.slice(idx + 1);
    if (!right) { bad++; continue; }
    ids.push(right);
  }
  return { ids: Array.from(new Set(ids)), badRows: bad };
}

function hasStatusInAttributeSearch(doc) {
  const as = doc && doc.attribute_search;
  if (!Array.isArray(as)) return false;
  for (const a of as) {
    if (a && typeof a === "object" && a.name === "status:status") return true;
  }
  return false;
}

// ------------------ MAIN ------------------
const startMs = Date.now();
logLine(`START: ${new Date(startMs).toISOString()} | DEBUG=${DEBUG} | DEBUG_DOC_ID=${DEBUG_DOC_ID || "(none)"} | MAP_PATH=${MAP_PATH}`);
logLine(`LOG: logPath=${logPath} | LOG_FILE_FLAGS=${LOG_FILE_FLAGS}`);
logLine(`CONFIG: MAP_CHUNK_SIZE=${MAP_CHUNK_SIZE} | BATCH_SIZE=${BATCH_SIZE} | LOG_EVERY=${LOG_EVERY} | DEBUG_NO_WRITE=${DEBUG_NO_WRITE}`);
logLine(`STATE: GLOBAL_STATE_PATH=${GLOBAL_STATE_PATH}`);

try {
  if (!fs.existsSync(OUT_DIR)) fs.mkdirSync(OUT_DIR, { recursive: true });

  load(MAP_PATH);
  if (!Array.isArray(SIO_IHUB_HC40_ADT_KEYS) || SIO_IHUB_HC40_ADT_KEYS.length === 0) {
    throw new Error("SIO_IHUB_HC40_ADT_KEYS non è un array valido o è vuoto. Controlla sio_ihub_hc40_adt_keys_map.js");
  }

  const totalPairs = SIO_IHUB_HC40_ADT_KEYS.length;
  const chunkSize = (MAP_CHUNK_SIZE && MAP_CHUNK_SIZE > 0) ? MAP_CHUNK_SIZE : totalPairs;
  const chunkCount = Math.ceil(totalPairs / chunkSize);

  logLine(`MAP_TOTAL: pairs=${totalPairs} | chunkSize=${chunkSize} | chunkCount=${chunkCount}`);

  const globalState = readJsonIfExists(GLOBAL_STATE_PATH, { completedChunks: [], updated_at: null });
  const completedSet = new Set(Array.isArray(globalState.completedChunks) ? globalState.completedChunks : []);
  logLine(`GLOBAL_STATE: completedChunks=${Array.from(completedSet).sort((a,b)=>a-b).join(",") || "(none)"}`);

  for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
    const chunkNo = chunkIndex + 1;
    const from = chunkIndex * chunkSize;
    const to = Math.min(totalPairs, from + chunkSize);
    const chunkPairs = SIO_IHUB_HC40_ADT_KEYS.slice(from, to);

    const outPathChunk = chunkOutPath(chunkNo);
    const statePathChunk = chunkStatePath(chunkNo);

    if (completedSet.has(chunkNo) && !DEBUG) {
      logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (global state). outPath=${outPathChunk}`);
      continue;
    }

    logLine(`CHUNK_START: #${chunkNo}/${chunkCount} | map_from=${from} | map_to=${to - 1} | map_rows=${chunkPairs.length} | outPath=${outPathChunk} | statePath=${statePathChunk}`);

    const { ids: encounterIds, badRows } = extractRightEncounterIds(chunkPairs);
    logLine(`CHUNK_KEYS: #${chunkNo} | encounterIds=${encounterIds.length} | badRows=${badRows}`);

    if (!encounterIds.length) {
      completedSet.add(chunkNo);
      globalState.completedChunks = Array.from(completedSet).sort((a,b)=>a-b);
      globalState.updated_at = new Date().toISOString();
      writeJson(GLOBAL_STATE_PATH, globalState);
      logLine(`CHUNK_EMPTY_IDS: #${chunkNo} -> completed`);
      continue;
    }

    const chunkState = readJsonIfExists(statePathChunk, { lastId: null, exported: 0, completed: false });
    let lastId = (typeof chunkState.lastId === "string") ? chunkState.lastId : null;
    let exported = (typeof chunkState.exported === "number") ? chunkState.exported : 0;

    if (chunkState.completed && !DEBUG) {
      completedSet.add(chunkNo);
      globalState.completedChunks = Array.from(completedSet).sort((a,b)=>a-b);
      globalState.updated_at = new Date().toISOString();
      writeJson(GLOBAL_STATE_PATH, globalState);
      logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (chunk state). outPath=${outPathChunk}`);
      continue;
    }

    const outFlags = (!DEBUG && exported > 0 && fs.existsSync(outPathChunk)) ? "a" : "w";
    const out = (DEBUG && DEBUG_NO_WRITE)
      ? null
      : fs.createWriteStream(outPathChunk, { encoding: "utf8", flags: outFlags });

    logLine(`CHUNK_RESUME: #${chunkNo} | lastId=${lastId ? lastId : "(null)"} | exported=${exported} | outFlags=${outFlags}`);

    const baseQuery = { _id: { $in: encounterIds } };
    if (DEBUG_DOC_ID) baseQuery._id = DEBUG_DOC_ID;

    let batchNo = 0;
    const startChunkMs = Date.now();

    while (true) {
      batchNo++;

      const q = Object.assign({}, baseQuery);
      if (!DEBUG_DOC_ID && lastId) q._id = { $in: encounterIds, $gt: lastId };

      const docs = db.encounter
        .find(q)
        .sort({ _id: 1 })
        .limit(BATCH_SIZE)
        .toArray();

      if (!docs.length) break;

      for (const doc of docs) {
        const has = hasStatusInAttributeSearch(doc);
        if (!has) {
          if (out) out.write(EJSON.stringify(doc, { relaxed: true }) + "\n");
          exported++;

          if (LOG_EVERY > 0 && exported % LOG_EVERY === 0) {
            const nowMs = Date.now();
            logLine(
              `PROGRESS: chunk#${chunkNo} ${new Date(nowMs).toISOString()} | exported=${exported} | lastId=${doc._id} | elapsed=${fmtDuration(nowMs - startMs)}`
            );
          }
        }

        lastId = doc._id;
      }

      if (!DEBUG) {
        writeJson(statePathChunk, { lastId, exported, completed: false, updated_at: new Date().toISOString() });
      }

      logLine(`BATCH_DONE: chunk#${chunkNo} batch#${batchNo} | docs=${docs.length} | lastId=${lastId} | chunkExported=${exported} | elapsed=${fmtDuration(Date.now() - startChunkMs)}`);

      if (DEBUG) break;
    }

    if (out) out.end();

    if (!DEBUG) {
      writeJson(statePathChunk, { lastId, exported, completed: true, completed_at: new Date().toISOString() });
      completedSet.add(chunkNo);
      globalState.completedChunks = Array.from(completedSet).sort((a,b)=>a-b);
      globalState.updated_at = new Date().toISOString();
      writeJson(GLOBAL_STATE_PATH, globalState);
    }

    logLine(`CHUNK_END: #${chunkNo}/${chunkCount} | exported=${exported} | outPath=${outPathChunk} | completed=${!DEBUG}`);
    if (DEBUG) { logLine("DEBUG: stop after first processed chunk (DEBUG=true)."); break; }
  }

  const endMs = Date.now();
  logLine(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)}`);
  if (__logStream) __logStream.end();
} catch (e) {
  logLine("ERROR: " + (e && e.stack ? e.stack : e));
  if (__logStream) __logStream.end();
  quit(1);
}