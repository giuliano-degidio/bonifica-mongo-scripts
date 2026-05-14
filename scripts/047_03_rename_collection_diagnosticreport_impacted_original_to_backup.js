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
function collectionExists(name) {
  return db.getCollectionNames().includes(name) || db.getCollectionInfos({ name }).length > 0;
}

(() => {
  const { runtime, context } = readRuntimeAndContext();
  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const sourceColl = params.sourceColl || "diagnosticreport_impacted_original";
  const targetColl = params.targetColl || "bonifica_diagnosticreport_deleted_bck";
  const dropTargetIfExists = params.dropTargetIfExists ?? true;

  const startMs = Date.now();
  log(`START: SOURCE_COLL=${sourceColl} | TARGET_COLL=${targetColl} | dropTargetIfExists=${dropTargetIfExists}`);

  try {
   try {  
     if (!collectionExists(sourceColl)) {
      log(`ATTENZIONE: source collection ${sourceColl} non trovata, script saltato senza errore.`);
      return; 
     }
   } catch (e) {
      log(`ERRORE nella guardia sulla collection: ${e && e.stack ? e.stack : e}`);
      return;
    } 

    const renameStart = Date.now();
    log(`RENAME_START: ${sourceColl} -> ${targetColl}`);

    db.getCollection(sourceColl).renameCollection(targetColl, dropTargetIfExists);

    const renameEnd = Date.now();
    log(`RENAME_END: ${sourceColl} -> ${targetColl} | elapsed=${fmtDuration(renameEnd - renameStart)}`);
    log(`VERIFY: sourceExists=${collectionExists(sourceColl)} | targetExists=${collectionExists(targetColl)}`);

    const endMs = Date.now();
    log(`END: elapsed=${fmtDuration(endMs - startMs)}`);

    print(JSON.stringify({
      type: "result",
      script: "047_03_rename_collection_diagnosticreport_impacted_original_to_backup.js",
      runId, stepId, dbName,
      sourceColl, targetColl, dropTargetIfExists,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    const errMs = Date.now();
    log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
    print("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();