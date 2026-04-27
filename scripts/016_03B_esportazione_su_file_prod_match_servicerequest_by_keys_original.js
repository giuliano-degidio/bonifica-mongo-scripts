const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

// ====== DEBUG ======
const DEBUG = false;
const DEBUG_DOC_ID = "";
const DEBUG_NO_WRITE = false;

// ====== DEFAULTS ======
const DEFAULT_MAP_CHUNK_SIZE = 1000;
const DEFAULT_BATCH_SIZE = 2000;
const DEFAULT_LOG_EVERY = 1000;

const DEFAULT_MATCH_NAME = "encounter:encounter.reference";
const DEFAULT_HINT_NAME = "attribute_search.name_1_attribute_search.value_1__id_1";

function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

function readJsonIfExists(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    const raw = fs.readFileSync(p, "utf8");
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function writeJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function pad4(n) { return String(n).padStart(4, "0"); }

// Estrae left keys "Encounter/SIO..." da "left#right"
function extractLeftKeys(pairs) {
  const keys = [];
  let bad = 0;
  for (const row of pairs) {
    if (typeof row !== "string") { bad++; continue; }
    const idx = row.indexOf("#");
    if (idx < 1) { bad++; continue; }
    const left = row.slice(0, idx);
    if (!left) { bad++; continue; }
    keys.push(left);
  }
  return { keys, badRows: bad };
}

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const outBase = params.outBase || "servicerequest_impacted_original";
  const outExt = params.outExt || ".jsonl";

  const mapFileName = params.mapFileName || "sio_ihub_hc40_adt_keys_map.js";
  const mapConstName = params.mapConstName || "SIO_IHUB_HC40_ADT_KEYS";
  const mapPath = params.mapPath || path.join(outDir, mapFileName);

  const mapChunkSize = params.mapChunkSize ?? DEFAULT_MAP_CHUNK_SIZE;
  const batchSize = params.batchSize ?? DEFAULT_BATCH_SIZE;
  const logEvery = params.logEvery ?? DEFAULT_LOG_EVERY;

  const matchName = params.matchName || DEFAULT_MATCH_NAME;
  const hintName = params.hintName || DEFAULT_HINT_NAME;

  const logToFile = params.logToFile ?? true;
  const logToConsole = params.logToConsole ?? true;
  const logFileFlags = params.logFileFlags || "a";
  const logPath = params.logPath || path.join(outDir, `${outBase}.log`);

  const globalStatePath = params.globalStatePath || path.join(outDir, `${outBase}.chunks.state.json`);

  function chunkOutPath(chunkNo1Based) { return `${outDir}/${outBase}_chunk${pad4(chunkNo1Based)}${outExt}`; }
  function chunkStatePath(chunkNo1Based) { return `${outDir}/${outBase}_chunk${pad4(chunkNo1Based)}.state.json`; }

  const __logStream = logToFile ? fs.createWriteStream(logPath, { encoding: "utf8", flags: logFileFlags }) : null;

  function logLine(s) {
    const line = String(s);
    if (logToConsole) print(line);
    if (__logStream) __logStream.write(line + "\n");
  }

  let summary = {
    totalPairs: 0,
    chunkSize: 0,
    chunkCount: 0,
    completedChunksBefore: [],
    completedChunksAfter: [],
    totalExportedDocs: 0,
    chunks: []
  };

  const startMs = Date.now();
  logLine(`START: ${new Date(startMs).toISOString()} | DEBUG=${DEBUG} | DEBUG_DOC_ID=${DEBUG_DOC_ID || "(none)"} | MAP_PATH=${mapPath}`);
  logLine(`LOG: logPath=${logPath} | LOG_TO_CONSOLE=${logToConsole} | LOG_FILE_FLAGS=${logFileFlags}`);
  logLine(`CONFIG: MAP_CHUNK_SIZE=${mapChunkSize} | BATCH_SIZE=${batchSize} | LOG_EVERY=${logEvery} | DEBUG_NO_WRITE=${DEBUG_NO_WRITE}`);
  logLine(`STATE: GLOBAL_STATE_PATH=${globalStatePath}`);

  // continue in part 2/2
    try {
    load(mapPath);
    const pairsAll = globalThis[mapConstName];

    if (!Array.isArray(pairsAll) || pairsAll.length === 0) {
      throw new Error(`${mapConstName} non è un array valido o è vuoto. File=${mapPath}`);
    }

    const totalPairs = pairsAll.length;
    const chunkSize = (mapChunkSize && mapChunkSize > 0) ? mapChunkSize : totalPairs;
    const chunkCount = Math.ceil(totalPairs / chunkSize);

    summary.totalPairs = totalPairs;
    summary.chunkSize = chunkSize;
    summary.chunkCount = chunkCount;

    logLine(`MAP_TOTAL: pairs=${totalPairs} | chunkSize=${chunkSize} | chunkCount=${chunkCount}`);

    const globalState = readJsonIfExists(globalStatePath, { completedChunks: [], updated_at: null });
    const completedSet = new Set(Array.isArray(globalState.completedChunks) ? globalState.completedChunks : []);
    summary.completedChunksBefore = Array.from(completedSet).sort((a, b) => a - b);

    logLine(`GLOBAL_STATE: completedChunks=${summary.completedChunksBefore.join(",") || "(none)"}`);

    for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      const chunkNo = chunkIndex + 1;
      const from = chunkIndex * chunkSize;
      const to = Math.min(totalPairs, from + chunkSize);
      const chunkPairs = pairsAll.slice(from, to);

      const outPathChunk = chunkOutPath(chunkNo);
      const statePathChunk = chunkStatePath(chunkNo);

      if (completedSet.has(chunkNo) && !DEBUG) {
        logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (global state). outPath=${outPathChunk}`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported: 0, completed: true, skipped: true });
        continue;
      }

      logLine(`CHUNK_START: #${chunkNo}/${chunkCount} | map_from=${from} | map_to=${to - 1} | map_rows=${chunkPairs.length} | outPath=${outPathChunk} | statePath=${statePathChunk}`);

      const { keys: sioKeysFullUpper, badRows } = extractLeftKeys(chunkPairs);
      if (!sioKeysFullUpper.length) {
        logLine(`CHUNK_EMPTY_KEYS: #${chunkNo} | badRows=${badRows} -> skip`);

        completedSet.add(chunkNo);
        globalState.completedChunks = Array.from(completedSet).sort((a, b) => a - b);
        globalState.updated_at = new Date().toISOString();
        writeJson(globalStatePath, globalState);

        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported: 0, completed: true, skipped: true });
        continue;
      }

      logLine(`CHUNK_KEYS: #${chunkNo} | keys=${sioKeysFullUpper.length} | badRows=${badRows}`);

      const chunkState = readJsonIfExists(statePathChunk, { lastId: null, exported: 0, completed: false });
      let lastId = (typeof chunkState.lastId === "string") ? chunkState.lastId : null;
      let exported = (typeof chunkState.exported === "number") ? chunkState.exported : 0;

      if (chunkState.completed && !DEBUG) {
        completedSet.add(chunkNo);
        globalState.completedChunks = Array.from(completedSet).sort((a, b) => a - b);
        globalState.updated_at = new Date().toISOString();
        writeJson(globalStatePath, globalState);

        logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (chunk state). outPath=${outPathChunk}`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported, completed: true, skipped: true });
        continue;
      }

      const outFlags = (!DEBUG && exported > 0 && fs.existsSync(outPathChunk)) ? "a" : "w";
      const out = (DEBUG && DEBUG_NO_WRITE) ? null : fs.createWriteStream(outPathChunk, { encoding: "utf8", flags: outFlags });

      logLine(`CHUNK_RESUME: #${chunkNo} | lastId=${lastId ? lastId : "(null)"} | exported=${exported} | outFlags=${outFlags}`);

      const baseQuery = {
        attribute_search: { $elemMatch: { name: matchName, value: { $in: sioKeysFullUpper } } }
      };
      if (DEBUG_DOC_ID) baseQuery._id = DEBUG_DOC_ID;

      let batchNo = 0;

      while (true) {
        batchNo++;

        const q = Object.assign({}, baseQuery);
        if (!DEBUG_DOC_ID && lastId) q._id = { $gt: lastId };

        const cursor = db.servicerequest.find(q).sort({ _id: 1 }).limit(batchSize);
        if (!DEBUG) {
          try { cursor.hint(hintName); } catch (_) {}
        }

        const docs = cursor.toArray();
        if (docs.length === 0) break;

        for (const doc of docs) {
          if (out) out.write(EJSON.stringify(doc, { relaxed: true }) + "\n");
          exported++;
          summary.totalExportedDocs++;
          lastId = doc._id;

          if (logEvery > 0 && exported % logEvery === 0) {
            const nowMs = Date.now();
            logLine(`PROGRESS: chunk#${chunkNo} ${new Date(nowMs).toISOString()} | exported=${exported} | lastId=${lastId} | elapsed=${fmtDuration(nowMs - startMs)}`);
          }
        }

        if (!DEBUG) {
          writeJson(statePathChunk, { lastId, exported, completed: false, updated_at: new Date().toISOString() });
        }

        logLine(`BATCH_DONE: chunk#${chunkNo} batch#${batchNo} | docs=${docs.length} | lastId=${lastId} | chunkExported=${exported}`);

        if (DEBUG) break;
      }

      if (out) out.end();

      if (!DEBUG) {
        writeJson(statePathChunk, { lastId, exported, completed: true, completed_at: new Date().toISOString() });

        completedSet.add(chunkNo);
        globalState.completedChunks = Array.from(completedSet).sort((a, b) => a - b);
        globalState.updated_at = new Date().toISOString();
        writeJson(globalStatePath, globalState);
      }

      logLine(`CHUNK_END: #${chunkNo}/${chunkCount} | exported=${exported} | outPath=${outPathChunk} | completed=${!DEBUG}`);

      summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported, completed: !DEBUG, skipped: false });

      if (DEBUG) {
        logLine("DEBUG: stop after first processed chunk (DEBUG=true).");
        break;
      }
    }

    summary.completedChunksAfter = Array.from(
      new Set(readJsonIfExists(globalStatePath, { completedChunks: [] }).completedChunks || [])
    ).sort((a, b) => a - b);

    const endMs = Date.now();
    logLine(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)}`);

    try { if (__logStream) __logStream.end(); } catch (_) {}

    print(
      JSON.stringify({
        type: "result",
        script: "016_03B_esportazione_su_file_prod_match_servicerequest_by_keys_original.js",
        runId,
        stepId,
        dbName,

        expDir,
        outDir,
        outBase,
        outExt,
        logPath,
        mapPath,
        mapConstName,
        globalStatePath,

        debug: DEBUG,
        debugDocId: DEBUG_DOC_ID || null,
        debugNoWrite: DEBUG_NO_WRITE,

        mapChunkSize,
        batchSize,
        logEvery,
        matchName,
        hintName,

        summary,
        ts: new Date().toISOString()
      })
    );
  } catch (e) {
    logLine("ERROR: " + (e && e.stack ? e.stack : e));
    try { if (__logStream) __logStream.end(); } catch (_) {}
    quit(1);
  }
})();
