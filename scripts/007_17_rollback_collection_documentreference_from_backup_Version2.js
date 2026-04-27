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
function collectionExists(name) {
  return db.getCollectionNames().includes(name) || db.getCollectionInfos({ name }).length > 0;
}

(() => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};

  const modifiedBckColl = params.modifiedBckColl || "bonifica_documentreference_inserted_bck";
  const originalBckColl = params.originalBckColl || "bonifica_documentreference_deleted_bck";
  const targetColl = params.targetColl || "documentreference";

  const historyColl = params.historyColl || "documentreference_history";
  const historyExportFileName = params.historyExportFileName || "documentreference_history_inserted_id.js";
  const historyExportConstName = params.historyExportConstName || "DOCUMENTREFERENCE_HISTORY_INSERTED_ID";

  const dryRun = params.dryRun ?? false;
  const requireConfirmation = params.requireConfirmation ?? false;

  const bulkDelete = params.bulkDelete ?? 1000;
  const bulkUpsert = params.bulkUpsert ?? 1000;

  const logEveryDeletes = params.logEveryDeletes ?? 5000;
  const logEveryUpserts = params.logEveryUpserts ?? 5000;
  const logEveryHistoryDeletes = params.logEveryHistoryDeletes ?? 5000;

  const runOutDir = path.join(expDir, String(runId || "no-runid"));
  const historyExportFilePath = path.join(runOutDir, historyExportFileName);

  function loadHistoryIdsIfFileExists() {
    const p = String(historyExportFilePath || "");
    if (!p) return null;
    if (!fs.existsSync(p)) return null;

    load(p);
    const ids = globalThis[historyExportConstName];
    if (!Array.isArray(ids)) {
      throw new Error(`History export file found but const '${historyExportConstName}' is not an array. File=${p}`);
    }
    return ids;
  }

  function deleteHistoryByIds(ids) {
    if (!ids || ids.length === 0) {
      log("STEP_C_SKIP: nessun id history da cancellare.");
      return { deleted: 0, bulks: 0 };
    }
    if (!collectionExists(historyColl)) {
      log(`STEP_C_SKIP: collection history non trovata: ${historyColl}`);
      return { deleted: 0, bulks: 0 };
    }

    const stepCStart = Date.now();
    log(`STEP_C_START: delete from ${historyColl} using ids from export file | ids=${ids.length} | DRY_RUN=${dryRun}`);

    let ops = [];
    let processed = 0;
    let bulks = 0;
    let deletedTotal = 0;

    for (const id of ids) {
      processed++;
      ops.push({ deleteOne: { filter: { _id: id } } });

      if (ops.length >= bulkDelete) {
        bulks++;
        if (!dryRun) {
          const res = db.getCollection(historyColl).bulkWrite(ops, { ordered: false });
          deletedTotal += res.deletedCount || 0;
        }
        ops = [];

        if (logEveryHistoryDeletes > 0 && processed % logEveryHistoryDeletes === 0) {
          log(`STEP_C_PROGRESS: processed=${processed}/${ids.length} bulks=${bulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(Date.now() - stepCStart)}`);
        }
      }
    }

    if (ops.length > 0) {
      bulks++;
      if (!dryRun) {
        const res = db.getCollection(historyColl).bulkWrite(ops, { ordered: false });
        deletedTotal += res.deletedCount || 0;
      }
      ops = [];
    }

    log(`STEP_C_END: processed=${processed} bulks=${bulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(Date.now() - stepCStart)}`);
    return { deleted: deletedTotal, bulks };
  }

  const startMs = Date.now();
  log(`START: rollback documentreference | TARGET_COLL=${targetColl} | MODIFIED_BCK_COLL=${modifiedBckColl} | ORIGINAL_BCK_COLL=${originalBckColl} | HISTORY_COLL=${historyColl} | DRY_RUN=${dryRun} | REQUIRE_CONFIRMATION=${requireConfirmation} | BULK_DELETE=${bulkDelete} | BULK_UPSERT=${bulkUpsert} | HISTORY_EXPORT_FILE_PATH=${historyExportFilePath}`);

  try {
    if (!collectionExists(modifiedBckColl)) throw new Error(`Collection not found: ${modifiedBckColl}`);
    if (!collectionExists(originalBckColl)) throw new Error(`Collection not found: ${originalBckColl}`);
    if (!collectionExists(targetColl)) throw new Error(`Target collection not found: ${targetColl}`);

    const modifiedCount = db.getCollection(modifiedBckColl).countDocuments();
    const originalCount = db.getCollection(originalBckColl).countDocuments();
    const targetCountBefore = db.getCollection(targetColl).countDocuments();

    log(`COUNTS_BEFORE: modified_bck=${modifiedCount} | original_bck=${originalCount} | target_before=${targetCountBefore}`);

    // Confirmation interactive intentionally not supported here (mongosh readlineSync not always available)
    if (requireConfirmation && !dryRun) {
      log("REQUIRE_CONFIRMATION=true but interactive prompt is not supported in this standardized version. Aborting for safety.");
      quit(2);
    }

    // STEP A
    const stepAStart = Date.now();
    log(`STEP_A_START: delete modified docs from ${targetColl} using _id list from ${modifiedBckColl}`);

    const modifiedCursor = db.getCollection(modifiedBckColl).find({}, { projection: { _id: 1 } }).sort({ _id: 1 });

    let delOps = [];
    let deleteOpsPrepared = 0;
    let deletedTotal = 0;
    let delBulks = 0;

    while (modifiedCursor.hasNext()) {
      const d = modifiedCursor.next();
      if (d && d._id !== undefined) {
        delOps.push({ deleteOne: { filter: { _id: d._id } } });
        deleteOpsPrepared++;
      }

      if (delOps.length >= bulkDelete) {
        delBulks++;
        if (!dryRun) {
          const res = db.getCollection(targetColl).bulkWrite(delOps, { ordered: false });
          deletedTotal += res.deletedCount || 0;
        }
        delOps = [];

        if (logEveryDeletes > 0 && deleteOpsPrepared % logEveryDeletes === 0) {
          log(`STEP_A_PROGRESS: prepared=${deleteOpsPrepared} deletedTotal=${deletedTotal} delBulks=${delBulks} | elapsed=${fmtDuration(Date.now() - stepAStart)}`);
        }
      }
    }

    if (delOps.length > 0) {
      delBulks++;
      if (!dryRun) {
        const res = db.getCollection(targetColl).bulkWrite(delOps, { ordered: false });
        deletedTotal += res.deletedCount || 0;
      }
      delOps = [];
    }

    log(`STEP_A_END: deleteOpsPrepared=${deleteOpsPrepared} deletedTotal=${deletedTotal} delBulks=${delBulks} | elapsed=${fmtDuration(Date.now() - stepAStart)}`);

    // STEP B
    const stepBStart = Date.now();
    log(`STEP_B_START: restore original docs into ${targetColl} from ${originalBckColl} (upsert)`);

    const originalCursor = db.getCollection(originalBckColl).find({}).sort({ _id: 1 });

    let upOps = [];
    let upsertOpsPrepared = 0;
    let upBulks = 0;

    let upInserted = 0;
    let upUpserted = 0;
    let upModified = 0;

    while (originalCursor.hasNext()) {
      const doc = originalCursor.next();
      if (doc == null || typeof doc !== "object" || Array.isArray(doc)) throw new Error(`Documento non valido letto da ${originalBckColl}`);
      if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${originalBckColl}`);

      upOps.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
      upsertOpsPrepared++;

      if (upOps.length >= bulkUpsert) {
        upBulks++;
        if (!dryRun) {
          const res = db.getCollection(targetColl).bulkWrite(upOps, { ordered: false });
          upInserted += res.insertedCount || 0;
          upUpserted += res.upsertedCount || 0;
          upModified += res.modifiedCount || 0;
        }
        upOps = [];

        if (logEveryUpserts > 0 && upsertOpsPrepared % logEveryUpserts === 0) {
          log(`STEP_B_PROGRESS: prepared=${upsertOpsPrepared} upBulks=${upBulks} inserted=${upInserted} upserted=${upUpserted} modified=${upModified} | elapsed=${fmtDuration(Date.now() - stepBStart)}`);
        }
      }
    }

    if (upOps.length > 0) {
      upBulks++;
      if (!dryRun) {
        const res = db.getCollection(targetColl).bulkWrite(upOps, { ordered: false });
        upInserted += res.insertedCount || 0;
        upUpserted += res.upsertedCount || 0;
        upModified += res.modifiedCount || 0;
      }
      upOps = [];
    }

    log(`STEP_B_END: upsertOpsPrepared=${upsertOpsPrepared} upBulks=${upBulks} inserted=${upInserted} upserted=${upUpserted} modified=${upModified} | elapsed=${fmtDuration(Date.now() - stepBStart)}`);

    // STEP C
    const historyIds = loadHistoryIdsIfFileExists();
    let stepC = { deleted: 0, bulks: 0, skipped: true };
    if (!historyIds) {
      log(`STEP_C_SKIP: file export non trovato. path=${historyExportFilePath}`);
    } else {
      stepC = { ...deleteHistoryByIds(historyIds), skipped: false };
    }

    const targetCountAfter = db.getCollection(targetColl).countDocuments();
    log(`COUNTS_AFTER: target_after=${targetCountAfter} | delta=${targetCountAfter - targetCountBefore}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | DRY_RUN=${dryRun}`);

    print(JSON.stringify({
      type: "result",
      script: "007_17_rollback_collection_documentreference_from_backup.js",
      runId, stepId, dbName,
      targetColl,
      modifiedBckColl, originalBckColl,
      historyColl,
      historyExportFilePath,
      dryRun,
      bulkDelete, bulkUpsert,
      countsBefore: { modifiedCount, originalCount, targetCountBefore },
      countsAfter: { targetCountAfter, delta: targetCountAfter - targetCountBefore },
      stepA: { deleteOpsPrepared, deletedTotal, delBulks },
      stepB: { upsertOpsPrepared, upBulks, upInserted, upUpserted, upModified },
      stepC,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR: ${e && e.stack ? e.stack : e}`);
    quit(1);
  }
})();