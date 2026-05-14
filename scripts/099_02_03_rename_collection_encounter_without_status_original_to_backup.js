const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

function now() { return new Date().toISOString(); }
function log(msg) { print(`[${now()}] ${msg}`); }
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(ms / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}
function collectionExists(name) {
  return db.getCollectionNames().includes(name) || db.getCollectionInfos({ name }).length > 0;
}

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const sourceCollection = params.sourceCollection || "encounter_without_status_original";
  const targetCollection = params.targetCollection || "bonifica_encounter_without_status_original_deleted_bck";
  const dropTargetIfExists = params.dropTargetIfExists ?? true;

  const startMs = Date.now();
  log(`START: sourceCollection=${sourceCollection} targetCollection=${targetCollection} dropTargetIfExists=${dropTargetIfExists}`);

  try {
    // ATTENZIONE: se la sorgente non esiste --> SKIP & exit(0)
    if (!collectionExists(sourceCollection)) {
      log(`SKIP: sourceCollection not found: ${sourceCollection} (nessuna operazione eseguita)`);
      log(`END: status=SKIP elapsed=${fmtDuration(Date.now() - startMs)}`);
      quit(0);
    }

    const renameStart = Date.now();
    log(`RENAME_START: ${sourceCollection} -> ${targetCollection}`);
    db.getCollection(sourceCollection).renameCollection(targetCollection, dropTargetIfExists);
    log(`RENAME_END: ${sourceCollection} -> ${targetCollection} | elapsed=${fmtDuration(Date.now() - renameStart)}`);

    log(`VERIFY: sourceExists=${collectionExists(sourceCollection)} targetExists=${collectionExists(targetCollection)}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)}`);

    print(JSON.stringify({
      type: "result",
      script: "099_02_03_rename_collection_encounter_without_status_original_to_backup.js",
      runId, stepId, dbName,
      sourceCollection,
      targetCollection,
      dropTargetIfExists,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
    log("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();