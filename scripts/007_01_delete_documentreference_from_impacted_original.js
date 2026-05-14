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

(async () => {
  const { runtime, context } = readRuntimeAndContext();
  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const sourceColl = params.sourceColl || "documentreference_impacted_original";
  const targetColl = params.targetColl || "documentreference";

  const bulkDelete = params.bulkDelete ?? 1000;
  const logEveryDeletes = params.logEveryDeletes ?? 1000;
  const dryRun = params.dryRun ?? false;

  const startMs = Date.now();
  log(`START: SOURCE_COLL=${sourceColl} | TARGET_COLL=${targetColl} | DRY_RUN=${dryRun} | BULK_DELETE=${bulkDelete} | LOG_EVERY_DELETES=${logEveryDeletes}`);
  if (!collectionExists(sourceColl)) {
    log(`ATTENZIONE: source collection ${sourceColl} non trovata, script saltato senza errore.`);
    return;
  }
  
  try {
    const source = db.getCollection(sourceColl);
    const target = db.getCollection(targetColl);

    const cursor = source.find({}, { projection: { _id: 1 } }).sort({ _id: 1 });

    let ops = [];
    let readIds = 0;

    let totalDeletesRequested = 0;
    let totalDeleted = 0;
    let bulks = 0;

    while (cursor.hasNext()) {
      const d = cursor.next();
      readIds++;

      if (d && d._id !== undefined) {
        ops.push({ deleteOne: { filter: { _id: d._id } } });
        totalDeletesRequested++;
      }

      if (ops.length >= bulkDelete) {
        bulks++;
        const bulkStart = Date.now();

        let deletedThisBulk = 0;
        if (!dryRun) {
          const res = target.bulkWrite(ops, { ordered: false });
          deletedThisBulk = res.deletedCount || 0;
          totalDeleted += deletedThisBulk;
        }

        log(`BULK_END #${bulks}: ops=${ops.length} deleted=${deletedThisBulk} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
        ops = [];

        if (logEveryDeletes > 0 && totalDeleted > 0 && (totalDeleted % logEveryDeletes) < deletedThisBulk) {
          log(`PROGRESS: readIds=${readIds} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(Date.now() - startMs)}`);
        }
      }
    }

    if (ops.length > 0) {
      bulks++;
      const bulkStart = Date.now();

      let deletedThisBulk = 0;
      if (!dryRun) {
        const res = target.bulkWrite(ops, { ordered: false });
        deletedThisBulk = res.deletedCount || 0;
        totalDeleted += deletedThisBulk;
      }

      log(`BULK_END #${bulks}: ops=${ops.length} deleted=${deletedThisBulk} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
    }

    const endMs = Date.now();
    log(`TOTALS: readIds=${readIds} deleteOpsPrepared=${totalDeletesRequested} bulks=${bulks} totalDeleted=${totalDeleted} DRY_RUN=${dryRun}`);
    log(`END: elapsed=${fmtDuration(endMs - startMs)} | SOURCE_COLL=${sourceColl} | TARGET_COLL=${targetColl}`);

    print(JSON.stringify({
      type: "result",
      script: "007_01_delete_documentreference_from_impacted_original.js",
      runId, stepId, dbName,
      sourceColl, targetColl,
      dryRun, bulkDelete, logEveryDeletes,
      readIds, deleteOpsPrepared: totalDeletesRequested, bulks, totalDeleted,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    const errMs = Date.now();
    log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
    print("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();