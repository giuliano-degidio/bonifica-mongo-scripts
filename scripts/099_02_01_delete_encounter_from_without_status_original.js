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

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const sourceCollection = params.sourceCollection || "encounter_without_status_original";
  const targetCollection = params.targetCollection || "encounter";

  const bulkDelete = params.bulkDelete ?? 1000;
  const logEveryDeletes = params.logEveryDeletes ?? 1000;
  const dryRun = params.dryRun ?? false;

  const startMs = Date.now();
  log(`START: sourceCollection=${sourceCollection} targetCollection=${targetCollection} dryRun=${dryRun} bulkDelete=${bulkDelete} logEveryDeletes=${logEveryDeletes}`);

  try {
    const source = db.getCollection(sourceCollection);
    const target = db.getCollection(targetCollection);

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

        if (logEveryDeletes > 0 && totalDeleted > 0 && (totalDeleted % logEveryDeletes) < deletedThisBulk) {
          log(`PROGRESS: readIds=${readIds} totalDeleted=${totalDeleted} | elapsed=${fmtDuration(Date.now() - startMs)}`);
        }

        ops = [];
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

    log(`TOTALS: readIds=${readIds} deleteOpsPrepared=${totalDeletesRequested} bulks=${bulks} totalDeleted=${totalDeleted} dryRun=${dryRun}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | sourceCollection=${sourceCollection} targetCollection=${targetCollection}`);

    print(JSON.stringify({
      type: "result",
      script: "099_02_01_delete_encounter_from_without_status_original.js",
      runId, stepId, dbName,
      sourceCollection,
      targetCollection,
      bulkDelete,
      logEveryDeletes,
      dryRun,
      readIds,
      deleteOpsPrepared: totalDeletesRequested,
      bulks,
      totalDeleted,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
    log("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();