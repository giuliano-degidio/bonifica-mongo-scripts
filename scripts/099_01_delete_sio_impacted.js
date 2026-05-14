const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

function now() { return new Date().toISOString(); }
function log(msg) { print(`[${now()}] ${msg}`); }
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}
function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
function assertArray(name, arr) {
  if (!Array.isArray(arr) || arr.length === 0) throw new Error(`${name} non è un array valido o è vuoto.`);
}
function askYesNoBlocking(question) {
  if (typeof readlineSync !== "function") {
    throw new Error("readlineSync non disponibile in questo ambiente mongosh: impossibile chiedere conferma interattiva.");
  }
  return readlineSync(question);
}

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  // Modifica: usa solo il file delle chiavi mappate  
  const idsFileNameMap = params.idsFileNameMap || "sio_ihub_hc40_adt_keys_map.js";
  const idsPathMap = path.join(expDir, String(runId || "no-runid"), idsFileNameMap);

  const targetCollection = params.targetCollection || "encounter";
  const batchIds = params.batchIds ?? 1000;
  const logEveryDeletes = params.logEveryDeletes ?? 1000;
  const requireConfirmation = params.requireConfirmation ?? false;

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const startMs = Date.now();
  log(`START: idsPathMap=${idsPathMap} targetCollection=${targetCollection} batchIds=${batchIds} logEveryDeletes=${logEveryDeletes} requireConfirmation=${requireConfirmation}`);

  try {
    if (!fs.existsSync(idsPathMap)) throw new Error(`File non trovato: ${idsPathMap}`);

    load(idsPathMap);

    assertArray("SIO_IHUB_HC40_ADT_KEYS", SIO_IHUB_HC40_ADT_KEYS);

    // Prendi solo la parte prima di #
    const idsClean = SIO_IHUB_HC40_ADT_KEYS
      .map(x => typeof x === "string" && x.includes("#") ? x.split("#")[0].trim() : null)
      .filter(x => !!x);

    const idsUnique = Array.from(new Set(idsClean));

    log(`IDS_LOADED: SIO_IHUB_HC40_ADT_KEYS=${SIO_IHUB_HC40_ADT_KEYS.length}`);
    log(`IDS_CLEANED: cleaned=${idsClean.length} unique=${idsUnique.length}`);

    if (requireConfirmation) {
      log("WARNING: Questa operazione CANCELLERA' DEFINITIVAMENTE documenti dalla collezione target.");
      log(`WARNING: Target collection = ${targetCollection}`);
      log(`WARNING: Numero _id in input (unique) = ${idsUnique.length}`);
      log("WARNING: Per procedere digitare esattamente: y  (qualsiasi altra risposta annulla)");
      const ans = askYesNoBlocking("CONFERMI LA CANCELLAZIONE DEFINITIVA? (y/N): ");
      if (String(ans).trim() !== "y") {
        log(`ABORTED: risposta='${ans}' (atteso 'y')`);
        quit(2);
      }
      log("CONFIRMED: procedo con la cancellazione.");
    }

    const target = db.getCollection(targetCollection);

    const idChunks = chunkArray(idsUnique, batchIds);
    log(`ID_CHUNKS: chunks=${idChunks.length} chunkSize=${batchIds}`);

    let totalDeleted = 0;
    let chunksDone = 0;

    for (let ci = 0; ci < idChunks.length; ci++) {
      const ids = idChunks[ci];

      const delStart = Date.now();
      const res = target.deleteMany({ _id: { $in: ids } });
      const deleted = (res && typeof res.deletedCount === "number") ? res.deletedCount : 0;
      totalDeleted += deleted;
      chunksDone++;

      log(`CHUNK_END #${ci + 1}/${idChunks.length}: ids=${ids.length} deleted=${deleted} | elapsed=${fmtDuration(Date.now() - delStart)} | totalDeleted=${totalDeleted}`);

      if (logEveryDeletes > 0 && totalDeleted > 0 && (totalDeleted % logEveryDeletes) < deleted) {
        log(`PROGRESS: chunksDone=${chunksDone}/${idChunks.length} totalDeleted=${totalDeleted} | elapsed=${fmtDuration(Date.now() - startMs)}`);
      }
    }

    log(`TOTALS: ids_unique=${idsUnique.length} chunks=${idChunks.length} totalDeleted=${totalDeleted}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | targetCollection=${targetCollection}`);

    print(JSON.stringify({
      type: "result",
      script: "099_01_delete_sio_impacted.js",
      runId, stepId, dbName,
      outDir, idsPathMap,
      targetCollection,
      batchIds, logEveryDeletes,
      requireConfirmation,
      idsUnique: idsUnique.length,
      totalDeleted,
      chunks: idChunks.length,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
    log("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();