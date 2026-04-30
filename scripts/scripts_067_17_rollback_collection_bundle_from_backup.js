/**
 * Pipeline-ready version of:
 * temp/067_17_rollback_collection_bundle_from_backup.js
 *
 * Standard:
 * - require("/data/Mongo_Sh_Script/lib/read_context.js")
 * - SKIP (exit 0) if required collections are missing
 * - dryRun from params.dryRun OR runtime.settings.dryRun
 * - history export file read from sourceExpDir/<historyExportFile> (optional)
 */

const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

const MODIFIED_BCK_COLL = "bonifica_bundle_inserted_bck";
const ORIGINAL_BCK_COLL = "bonifica_bundle_deleted_bck";
const TARGET_COLL = "bundle";

// optional history cleanup
const HISTORY_COLL = "bundle_history";
const HISTORY_EXPORT_CONST_NAME = "BUNDLE_HISTORY_INSERTED_ID";

// tuning
const BULK_DELETE = 1000;
const BULK_UPSERT = 1000;

// log
const LOG_EVERY_DELETES = 5000;
const LOG_EVERY_UPSERTS = 5000;
const LOG_EVERY_HISTORY_DELETES = 5000;

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

function loadIdsFromExportFileIfExists(exportFilePath, constName) {
  if (!exportFilePath) return null;
  const p = String(exportFilePath);
  if (!fs.existsSync(p)) return null;

  load(p);
  const ids = globalThis[constName];
  if (!Array.isArray(ids)) {
    throw new Error(`Export file exists but does not define global array '${constName}'. File=${p}`);
  }
  return ids;
}

function deleteHistoryByIds(ids, dryRun) {
  if (!ids || ids.length === 0) {
    log("STEP_C_SKIP: no history ids to delete.");
    return { deleted: 0, bulks: 0 };
  }
  if (!collectionExists(HISTORY_COLL)) {
    log(`STEP_C_SKIP: history collection not found: ${HISTORY_COLL}`);
    return { deleted: 0, bulks: 0 };
  }

  const stepCStart = Date.now();
  log(`STEP_C_START: delete from ${HISTORY_COLL} using ids from export file | ids=${ids.length} | DRY_RUN=${dryRun}`);

  let ops = [];
  let processed = 0;
  let bulks = 0;
  let deletedTotal = 0;

  for (const id of ids) {
    processed++;
    ops.push({ deleteOne: { filter: { _id: id } } });

    if (ops.length >= BULK_DELETE) {
      bulks++;
      if (!dryRun) {
        const res = db.getCollection(HISTORY_COLL).bulkWrite(ops, { ordered: false });
        deletedTotal += res.deletedCount || 0;
      }
      ops = [];

      if (LOG_EVERY_HISTORY_DELETES > 0 && processed % LOG_EVERY_HISTORY_DELETES === 0) {
        log(`STEP_C_PROGRESS: processed=${processed}/${ids.length} bulks=${bulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(Date.now() - stepCStart)}`);
      }
    }
  }

  if (ops.length > 0) {
    bulks++;
    if (!dryRun) {
      const res = db.getCollection(HISTORY_COLL).bulkWrite(ops, { ordered: false });
      deletedTotal += res.deletedCount || 0;
    }
    ops = [];
  }

  log(`STEP_C_END: processed=${processed} bulks=${bulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(Date.now() - stepCStart)}`);
  return { deleted: deletedTotal, bulks };
}

// ====== MAIN ======
const startMs = Date.now();
const { runtime, context } = readRuntimeAndContext();

const params = context?.params || {};
const dryRun = (params.dryRun ?? runtime?.settings?.dryRun ?? false) === true;

const runId = runtime?.runId || context?.runId || null;
const env = runtime?.env || context?.env || null;

const sourceRunId = runtime?.rollback?.sourceRunId || context?.rollback?.sourceRunId || null;
const sourceExpDir = runtime?.rollback?.sourceExpDir || context?.rollback?.sourceExpDir || null;

const historyExportFile = params.historyExportFile || "bundle_history_inserted_id.js";
const historyExportFilePath = sourceExpDir ? path.join(String(sourceExpDir), String(historyExportFile)) : null;

log(`START: rollback bundle | runId=${runId} env=${env} | sourceRunId=${sourceRunId} sourceExpDir=${sourceExpDir} | TARGET_COLL=${TARGET_COLL} | MODIFIED_BCK_COLL=${MODIFIED_BCK_COLL} | ORIGINAL_BCK_COLL=${ORIGINAL_BCK_COLL} | DRY_RUN=${dryRun}`);

try {
  // REQUIRED collections
  const missingCollections = [];
  if (!collectionExists(MODIFIED_BCK_COLL)) missingCollections.push(MODIFIED_BCK_COLL);
  if (!collectionExists(ORIGINAL_BCK_COLL)) missingCollections.push(ORIGINAL_BCK_COLL);
  if (!collectionExists(TARGET_COLL)) missingCollections.push(TARGET_COLL);

  if (missingCollections.length > 0) {
    log(`SKIP: missing required collections: ${missingCollections.join(", ")}`);
    log(`END: status=SKIP elapsed=${fmtDuration(Date.now() - startMs)} DRY_RUN=${dryRun}`);
    quit(0);
  }

  const modifiedCount = db.getCollection(MODIFIED_BCK_COLL).countDocuments();
  const originalCount = db.getCollection(ORIGINAL_BCK_COLL).countDocuments();
  const targetCountBefore = db.getCollection(TARGET_COLL).countDocuments();
  log(`COUNTS_BEFORE: modified_bck=${modifiedCount} | original_bck=${originalCount} | target_before=${targetCountBefore}`);

  // STEP A delete modified
  const stepAStart = Date.now();
  log(`STEP_A_START: delete modified docs from ${TARGET_COLL} using _id list from ${MODIFIED_BCK_COLL}`);

  const modifiedCursor = db.getCollection(MODIFIED_BCK_COLL)
    .find({}, { projection: { _id: 1 } })
    .sort({ _id: 1 });

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

    if (delOps.length >= BULK_DELETE) {
      delBulks++;
      if (!dryRun) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
        deletedTotal += res.deletedCount || 0;
      }

      if (LOG_EVERY_DELETES > 0 && deleteOpsPrepared % LOG_EVERY_DELETES === 0) {
        log(`STEP_A_PROGRESS: prepared=${deleteOpsPrepared} deletedTotal=${deletedTotal} delBulks=${delBulks} | elapsed=${fmtDuration(Date.now() - stepAStart)}`);
      }
      delOps = [];
    }
  }

  if (delOps.length > 0) {
    delBulks++;
    if (!dryRun) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
      deletedTotal += res.deletedCount || 0;
    }
    delOps = [];
  }

  log(`STEP_A_END: deleteOpsPrepared=${deleteOpsPrepared} deletedTotal=${deletedTotal} delBulks=${delBulks} | elapsed=${fmtDuration(Date.now() - stepAStart)}`);

  // STEP B restore original
  const stepBStart = Date.now();
  log(`STEP_B_START: restore original docs into ${TARGET_COLL} from ${ORIGINAL_BCK_COLL} (upsert)`);

  const originalCursor = db.getCollection(ORIGINAL_BCK_COLL).find({}).sort({ _id: 1 });

  let upOps = [];
  let readOriginalDocs = 0;
  let upsertOpsPrepared = 0;
  let upBulks = 0;

  while (originalCursor.hasNext()) {
    const doc = originalCursor.next();
    readOriginalDocs++;

    if (!doc || typeof doc !== "object" || Array.isArray(doc)) throw new Error(`Invalid doc read from ${ORIGINAL_BCK_COLL}`);
    if (doc._id === undefined) throw new Error(`Doc without _id read from ${ORIGINAL_BCK_COLL}`);

    upOps.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    upsertOpsPrepared++;

    if (upOps.length >= BULK_UPSERT) {
      upBulks++;
      if (!dryRun) db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });

      if (LOG_EVERY_UPSERTS > 0 && upsertOpsPrepared % LOG_EVERY_UPSERTS === 0) {
        log(`STEP_B_PROGRESS: prepared=${upsertOpsPrepared} readOriginalDocs=${readOriginalDocs} upBulks=${upBulks} | elapsed=${fmtDuration(Date.now() - stepBStart)}`);
      }
      upOps = [];
    }
  }

  if (upOps.length > 0) {
    upBulks++;
    if (!dryRun) db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
    upOps = [];
  }

  log(`STEP_B_END: readOriginalDocs=${readOriginalDocs} upsertOpsPrepared=${upsertOpsPrepared} upBulks=${upBulks} | elapsed=${fmtDuration(Date.now() - stepBStart)}`);

  // STEP C optional history cleanup
  if (!sourceExpDir) {
    log(`STEP_C_SKIP: sourceExpDir not available in runtime/context (cannot resolve history export file).`);
  } else {
    const historyIds = loadIdsFromExportFileIfExists(historyExportFilePath, HISTORY_EXPORT_CONST_NAME);
    if (!historyIds) log(`STEP_C_SKIP: history export file not configured or not found. path=${historyExportFilePath}`);
    else deleteHistoryByIds(historyIds, dryRun);
  }

  const targetCountAfter = db.getCollection(TARGET_COLL).countDocuments();
  log(`COUNTS_AFTER: target_after=${targetCountAfter} | delta=${targetCountAfter - targetCountBefore}`);
  log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | DRY_RUN=${dryRun}`);
} catch (e) {
  log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}