const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

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
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return fallback;
  }
}
function writeJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}
function pad4(n) { return String(n).padStart(4, "0"); }

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
function ensureAttributeSearchArray(doc) {
  if (!doc || typeof doc !== "object") return;
  if (!Array.isArray(doc.attribute_search)) doc.attribute_search = [];
}
function addStatusFinished(doc) {
  if (!doc.resource || typeof doc.resource !== "object" || Array.isArray(doc.resource)) doc.resource = {};
  doc.resource.status = "finished";
  ensureAttributeSearchArray(doc);
  doc.attribute_search.push({ name: "status:status", value: "finished" });
}

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  const mapFileName = params.mapFileName || "sio_ihub_hc40_adt_keys_map.js";
  const mapConstName = params.mapConstName || "SIO_IHUB_HC40_ADT_KEYS";

  const outBase = params.outBase || "encounter_without_status_add";
  const outExt = params.outExt || ".jsonl";

  const mapChunkSize = params.mapChunkSize ?? 1000;
  const batchSize = params.batchSize ?? 2000;
  const logEvery = params.logEvery ?? 1000;

  const logToFile = params.logToFile ?? true;
  const logToConsole = params.logToConsole ?? true;
  const logFileName = params.logFileName || "encounter_without_status_add.log";
  const logFileFlags = params.logFileFlags || "a";

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const mapPath = path.join(outDir, mapFileName);
  const globalStatePath = path.join(outDir, `${outBase}.chunks.state.json`);
  const logPath = path.join(outDir, logFileName);

  const logStream = logToFile ? fs.createWriteStream(logPath, { encoding: "utf8", flags: logFileFlags }) : null;
  function logLine(s) {
    const line = String(s);
    if (logToConsole) print(line);
    if (logStream) logStream.write(line + "\n");
  }

  function chunkOutPath(chunkNo1Based) {
    return path.join(outDir, `${outBase}_chunk${pad4(chunkNo1Based)}${outExt}`);
  }
  function chunkStatePath(chunkNo1Based) {
    return path.join(outDir, `${outBase}_chunk${pad4(chunkNo1Based)}.state.json`);
  }

  const startMs = Date.now();
  logLine(`START: ${new Date(startMs).toISOString()} | runId=${runId} | mapPath=${mapPath}`);
  logLine(`LOG: logPath=${logPath} | logToFile=${logToFile} | logToConsole=${logToConsole} | flags=${logFileFlags}`);
  logLine(`CONFIG: outBase=${outBase} outExt=${outExt} mapChunkSize=${mapChunkSize} batchSize=${batchSize} logEvery=${logEvery}`);
  logLine(`STATE: globalStatePath=${globalStatePath}`);

  let summary = {
    mapTotalPairs: 0,
    chunkCount: 0,
    completedChunksBefore: [],
    completedChunksAfter: [],
    totalExportedDocs: 0,
    chunks: []
  };

  try {
    if (!fs.existsSync(mapPath)) throw new Error(`Map file non trovato: ${mapPath}`);

    load(mapPath);

    const pairs = globalThis[mapConstName];
    if (!Array.isArray(pairs) || pairs.length === 0) throw new Error(`${mapConstName} non è un array valido o è vuoto. Check ${mapFileName}`);

    const totalPairs = pairs.length;
    const chunkSize = (mapChunkSize && mapChunkSize > 0) ? mapChunkSize : totalPairs;
    const chunkCount = Math.ceil(totalPairs / chunkSize);

    summary.mapTotalPairs = totalPairs;
    summary.chunkCount = chunkCount;

    const globalState = readJsonIfExists(globalStatePath, { completedChunks: [], updated_at: null });
    const completedSet = new Set(Array.isArray(globalState.completedChunks) ? globalState.completedChunks : []);
    summary.completedChunksBefore = Array.from(completedSet).sort((a, b) => a - b);

    logLine(`MAP_TOTAL: pairs=${totalPairs} chunkSize=${chunkSize} chunkCount=${chunkCount}`);
    logLine(`GLOBAL_STATE: completedChunks=${summary.completedChunksBefore.join(",") || "(none)"}`);

    for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      const chunkNo = chunkIndex + 1;
      const from = chunkIndex * chunkSize;
      const to = Math.min(totalPairs, from + chunkSize);
      const chunkPairs = pairs.slice(from, to);

      const outPathChunk = chunkOutPath(chunkNo);
      const statePathChunk = chunkStatePath(chunkNo);

      if (completedSet.has(chunkNo)) {
        logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (global state) outPath=${outPathChunk}`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported: 0, completed: true, skipped: true });
        continue;
      }

      logLine(`CHUNK_START: #${chunkNo}/${chunkCount} | map_from=${from} | map_to=${to - 1} | map_rows=${chunkPairs.length} | outPath=${outPathChunk} | statePath=${statePathChunk}`);

      const { ids: encounterIds, badRows } = extractRightEncounterIds(chunkPairs);
      logLine(`CHUNK_KEYS: #${chunkNo} | encounterIds=${encounterIds.length} | badRows=${badRows}`);

      if (!encounterIds.length) {
        completedSet.add(chunkNo);
        writeJson(globalStatePath, { completedChunks: Array.from(completedSet).sort((a, b) => a - b), updated_at: new Date().toISOString() });
        logLine(`CHUNK_EMPTY_IDS: #${chunkNo} -> completed`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported: 0, completed: true, skipped: false });
        continue;
      }

      const chunkState = readJsonIfExists(statePathChunk, { lastId: null, exported: 0, completed: false });
      let lastId = (typeof chunkState.lastId === "string") ? chunkState.lastId : null;
      let exported = (typeof chunkState.exported === "number") ? chunkState.exported : 0;

      if (chunkState.completed) {
        completedSet.add(chunkNo);
        writeJson(globalStatePath, { completedChunks: Array.from(completedSet).sort((a, b) => a - b), updated_at: new Date().toISOString() });
        logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (chunk state) outPath=${outPathChunk}`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported, completed: true, skipped: true });
        continue;
      }

      const outFlags = (exported > 0 && fs.existsSync(outPathChunk)) ? "a" : "w";
      const out = fs.createWriteStream(outPathChunk, { encoding: "utf8", flags: outFlags });
      logLine(`CHUNK_RESUME: #${chunkNo} | lastId=${lastId ? lastId : "(null)"} | exported=${exported} | outFlags=${outFlags}`);

      let batchNo = 0;
      const startChunkMs = Date.now();

      while (true) {
        batchNo++;
        const q = { _id: { $in: encounterIds } };
        if (lastId) q._id = { $in: encounterIds, $gt: lastId };

        const docs = db.encounter.find(q).sort({ _id: 1 }).limit(batchSize).toArray();
        if (!docs.length) break;

        for (const doc of docs) {
          if (!hasStatusInAttributeSearch(doc)) {
            addStatusFinished(doc);
            out.write(EJSON.stringify(doc, { relaxed: true }) + "\n");
            exported++;

            if (logEvery > 0 && exported % logEvery === 0) {
              logLine(`PROGRESS: chunk#${chunkNo} exported=${exported} lastId=${doc._id} | elapsed=${fmtDuration(Date.now() - startMs)}`);
            }
          }
          lastId = doc._id;
        }

        writeJson(statePathChunk, { lastId, exported, completed: false, updated_at: new Date().toISOString() });
        logLine(`BATCH_DONE: chunk#${chunkNo} batch#${batchNo} | docs=${docs.length} | lastId=${lastId} | chunkExported=${exported} | elapsed=${fmtDuration(Date.now() - startChunkMs)}`);
      }

      out.end();

      writeJson(statePathChunk, { lastId, exported, completed: true, completed_at: new Date().toISOString() });

      completedSet.add(chunkNo);
      writeJson(globalStatePath, { completedChunks: Array.from(completedSet).sort((a, b) => a - b), updated_at: new Date().toISOString() });

      logLine(`CHUNK_END: #${chunkNo}/${chunkCount} | exported=${exported} | outPath=${outPathChunk} | completed=true`);

      summary.totalExportedDocs += exported;
      summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairs.length, outPath: outPathChunk, statePath: statePathChunk, exported, completed: true, skipped: false });
    }

    summary.completedChunksAfter = Array.from(new Set(readJsonIfExists(globalStatePath, { completedChunks: [] }).completedChunks || [])).sort((a, b) => a - b);

    logLine(`END: ${new Date().toISOString()} | elapsed=${fmtDuration(Date.now() - startMs)}`);
    try { if (logStream) logStream.end(); } catch (_) {}

    print(JSON.stringify({
      type: "result",
      script: "099_01_03_esportazione_su_file_encounter_ihub_hc40_adt_without_status_add.js",
      runId, stepId, dbName,
      expDir, outDir,
      mapPath, mapConstName,
      outBase, outExt,
      logPath,
      globalStatePath,
      mapChunkSize, batchSize, logEvery,
      summary,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    logLine("ERROR: " + (e && e.stack ? e.stack : e));
    try { if (logStream) logStream.end(); } catch (_) {}
    quit(1);
  }
})();