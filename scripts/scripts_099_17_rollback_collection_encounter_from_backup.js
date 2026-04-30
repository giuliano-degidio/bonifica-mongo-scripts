/**
 * Pipeline-ready version of:
 * temp/099_17_rollback_collection_encounter_from_backup.js
 *
 * Standard:
 * - require("/data/Mongo_Sh_Script/lib/read_context.js")
 * - SKIP (exit 0) if REQUIRED collections are missing
 * - dryRun from params.dryRun OR runtime.settings.dryRun
 * - history export files read from sourceExpDir/<historyExportFile> and sourceExpDir/<wsHistoryExportFile> (optional)
 *
 * NOTE:
 * This script performs 2 rollback "blocks":
 *  1) Old restore: bonifica_encounter_deleted_bck -> encounter  (required in original script)
 *  2) Without-status rollback:
 *     - delete ids from bonifica_encounter_without_status_modified_inserted_bck
 *     - restore docs from bonifica_encounter_without_status_original_deleted_bck
 * Optional:
 *  - cleanup encounter_history using export file(s) if present
 */

const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

// ============================================================================
// COLLECTIONS
// ============================================================================
const TARGET_COLL = "encounter";
const HISTORY_COLL = "encounter_history";

// --- rollback "vecchio" ---
const BACKUP_COLL_OLD = "bonifica_encounter_deleted_bck"; // restore full docs -> encounter (upsert)

// --- rollback "encounter_without_status" ---
const WS_MODIFIED_BCK_COLL = "bonifica_encounter_without_status_modified_inserted_bck";
const WS_ORIGINAL_BCK_COLL = "bonifica_encounter_without_status_original_deleted_bck";

// ============================================================================
// EXPORT CONST NAMES (as per original script)
// ============================================================================
const OLD_HISTORY_EXPORT_CONST_NAME = "ENCOUNTER_HISTORY_INSERTED_ID";
const WS_HISTORY_EXPORT_CONST_NAME = "ENCOUNTER_HISTORY_WITHOUT_STATUS_INSERTED_ID";

// ============================================================================
// TUNING
// ============================================================================
const BATCH_RESTORE_OLD = 1000;
const BULK_DELETE = 1000;
const BULK_UPSERT = 1000;
const LOG_EVERY = 5000;

// ============================================================================
// UTILS
// ============================================================================
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

function loadIdsIfFileExists(filePath, constName) {
  if (!filePath) return null;
  const p = String(filePath);
  if (!fs.existsSync(p)) return null;

  load(p);
  const ids = globalThis[constName];
  if (!Array.isArray(ids)) {
    throw new Error(`Export file exists but does not define global array '${constName}'. File=${p}`);
  }
  return ids;
}

function bulkDeleteByIds(collName, ids, label, dryRun, startedAtMs) {
  if (!ids || ids.length === 0) {
    log(`${label}_SKIP: no ids to delete.`);
    return { deleted: 0, bulks: 0, processed: 0 };
  }
  if (!collectionExists(collName)) {
    log(`${label}_SKIP: collection not found: ${collName}`);
    return { deleted: 0, bulks: 0, processed: 0 };
  }

  log(`${label}_START: coll=${collName} ids=${ids.length} DRY_RUN=${dryRun} BULK_DELETE=${BULK_DELETE}`);

  let ops = [];
  let processed = 0;
  let bulks = 0;
  let deleted = 0;

  for (const id of ids) {
    processed++;
    ops.push({ deleteOne: { filter: { _id: id } } });

    if (ops.length >= BULK_DELETE) {
      bulks++;
      if (!dryRun) {
        const res = db.getCollection(collName).bulkWrite(ops, { ordered: false });
        deleted += res.deletedCount || 0;
      }
      ops = [];

      if (LOG_EVERY > 0 && processed % LOG_EVERY === 0) {
        log(`${label}_PROGRESS: processed=${processed}/${ids.length} bulks=${bulks} deleted=${deleted} | elapsed=${fmtDuration(Date.now() - startedAtMs)}`);
      }
    }
  }

  if (ops.length > 0) {
    bulks++;
    if (!dryRun) {
      const res = db.getCollection(collName).bulkWrite(ops, { ordered: false });
      deleted += res.deletedCount || 0;
    }
    ops = [];
  }

  log(`${label}_END: processed=${processed} bulks=${bulks} deleted=${deleted}`);
  return { deleted, bulks, processed };
}

// ============================================================================
// MAIN
// ============================================================================
const startMs = Date.now();
const { runtime, context } = readRuntimeAndContext();

const params = context?.params || {};
const dryRun = (params.dryRun ?? runtime?.settings?.dryRun ?? false) === true;

const runId = runtime?.runId || context?.runId || null;
const env = runtime?.env || context?.env || null;

const sourceRunId = runtime?.rollback?.sourceRunId || context?.rollback?.sourceRunId || null;
const sourceExpDir = runtime?.rollback?.sourceExpDir || context?.rollback?.sourceExpDir || null;

const oldHistoryExportFile = params.historyExportFile || "encounter_history_inserted_id.js";
const wsHistoryExportFile = params.wsHistoryExportFile || "encounter_history_without_status_inserted_id.js";

const oldHistoryExportPath = sourceExpDir ? path.join(String(sourceExpDir), String(oldHistoryExportFile)) : null;
const wsHistoryExportPath = sourceExpDir ? path.join(String(sourceExpDir), String(wsHistoryExportFile)) : null;

log(
  `START: rollback encounter | runId=${runId} env=${env} | sourceRunId=${sourceRunId} sourceExpDir=${sourceExpDir} | TARGET_COLL=${TARGET_COLL} | BACKUP_COLL_OLD=${BACKUP_COLL_OLD} | WS_MODIFIED_BCK_COLL=${WS_MODIFIED_BCK_COLL} | WS_ORIGINAL_BCK_COLL=${WS_ORIGINAL_BCK_COLL} | DRY_RUN=${dryRun}`
);

try {
  // ------------------------------------------------------------------------
  // REQUIRED prereqs (based on the original script)
  // ------------------------------------------------------------------------
  const missingCollections = [];
  if (!collectionExists(TARGET_COLL)) missingCollections.push(TARGET_COLL);
  if (!collectionExists(BACKUP_COLL_OLD)) missingCollections.push(BACKUP_COLL_OLD);
  if (!collectionExists(WS_MODIFIED_BCK_COLL)) missingCollections.push(WS_MODIFIED_BCK_COLL);
  if (!collectionExists(WS_ORIGINAL_BCK_COLL)) missingCollections.push(WS_ORIGINAL_BCK_COLL);

  if (missingCollections.length > 0) {
    log(`SKIP: missing required collections: ${missingCollections.join(", ")}`);
    log(`END: status=SKIP elapsed=${fmtDuration(Date.now() - startMs)} DRY_RUN=${dryRun}`);
    quit(0);
  }

  const historyExists = collectionExists(HISTORY_COLL);

  const backupOldCount = db.getCollection(BACKUP_COLL_OLD).countDocuments();
  const wsModifiedCount = db.getCollection(WS_MODIFIED_BCK_COLL).countDocuments();
  const wsOriginalCount = db.getCollection(WS_ORIGINAL_BCK_COLL).countDocuments();

  const targetCountBefore = db.getCollection(TARGET_COLL).countDocuments();
  const historyCountBefore = historyExists ? db.getCollection(HISTORY_COLL).countDocuments() : 0;

  log(
    `COUNTS_BEFORE: backupOldCount=${backupOldCount} | ws_modified_bck=${wsModifiedCount} | ws_original_bck=${wsOriginalCount} | target_before=${targetCountBefore} | history_exists=${historyExists} | history_before=${historyCountBefore}`
  );

  // ========================================================================
  // STEP 1: restore encounter from old backup
  // ========================================================================
  const step1Start = Date.now();
  log(`STEP_1_START: restore encounter from old backup | from=${BACKUP_COLL_OLD} -> to=${TARGET_COLL} | DRY_RUN=${dryRun} | BATCH=${BATCH_RESTORE_OLD}`);

  const cursorOld = db.getCollection(BACKUP_COLL_OLD).find({}).sort({ _id: 1 });

  let ops = [];
  let processed = 0;
  let bulks = 0;

  let inserted = 0;
  let upserted = 0;
  let modified = 0;

  while (cursorOld.hasNext()) {
    const doc = cursorOld.next();
    processed++;

    if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
      throw new Error(`Invalid doc read from ${BACKUP_COLL_OLD} (processed=${processed})`);
    }
    if (doc._id === undefined) {
      throw new Error(`Doc without _id read from ${BACKUP_COLL_OLD} (processed=${processed})`);
    }

    ops.push({
      replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true }
    });

    if (ops.length >= BATCH_RESTORE_OLD) {
      bulks++;

      if (!dryRun) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(ops, { ordered: false });
        inserted += res.insertedCount || 0;
        upserted += res.upsertedCount || 0;
        modified += res.modifiedCount || 0;
      }

      ops = [];

      if (LOG_EVERY > 0 && processed % LOG_EVERY === 0) {
        log(`STEP_1_PROGRESS: processed=${processed} bulks=${bulks} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(Date.now() - step1Start)}`);
      }
    }
  }

  if (ops.length > 0) {
    bulks++;
    if (!dryRun) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(ops, { ordered: false });
      inserted += res.insertedCount || 0;
      upserted += res.upsertedCount || 0;
      modified += res.modifiedCount || 0;
    }
    ops = [];
  }

  log(`STEP_1_END: processed=${processed} bulks=${bulks} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(Date.now() - step1Start)}`);

  // ========================================================================
  // STEP 2 (optional): cleanup encounter_history from old export file
  // ========================================================================
  if (!sourceExpDir) {
    log(`STEP_2_HISTORY_DELETE_SKIP: sourceExpDir not available (cannot resolve old history export file).`);
  } else {
    const oldHistoryIds = loadIdsIfFileExists(oldHistoryExportPath, OLD_HISTORY_EXPORT_CONST_NAME);
    if (!oldHistoryIds) {
      log(`STEP_2_HISTORY_DELETE_SKIP: export file not configured or not found. path=${oldHistoryExportPath}`);
    } else if (!historyExists) {
      log(`STEP_2_HISTORY_DELETE_SKIP: history collection not found: ${HISTORY_COLL}`);
    } else {
      bulkDeleteByIds(HISTORY_COLL, oldHistoryIds, "STEP_2_HISTORY_DELETE", dryRun, startMs);
    }
  }

  // ========================================================================
  // STEP A: delete encounter modified (without-status) from target
  // ========================================================================
  const stepAStart = Date.now();
  log(`STEP_A_START: delete encounter modified (without-status) from ${TARGET_COLL} using ids from ${WS_MODIFIED_BCK_COLL} | DRY_RUN=${dryRun} | BULK_DELETE=${BULK_DELETE}`);

  const wsModifiedCursor = db.getCollection(WS_MODIFIED_BCK_COLL).find({}, { projection: { _id: 1 } }).sort({ _id: 1 });

  let delOps = [];
  let processedIds = 0;
  let delBulks = 0;
  let deletedTotal = 0;

  while (wsModifiedCursor.hasNext()) {
    const d = wsModifiedCursor.next();
    processedIds++;

    if (d && d._id !== undefined) delOps.push({ deleteOne: { filter: { _id: d._id } } });

    if (delOps.length >= BULK_DELETE) {
      delBulks++;
      if (!dryRun) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
        deletedTotal += res.deletedCount || 0;
      }
      delOps = [];

      if (LOG_EVERY > 0 && processedIds % LOG_EVERY === 0) {
        log(`STEP_A_PROGRESS: processedIds=${processedIds} delBulks=${delBulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(Date.now() - stepAStart)}`);
      }
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

  log(`STEP_A_END: processedIds=${processedIds} delBulks=${delBulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(Date.now() - stepAStart)}`);

  // ========================================================================
  // STEP B: restore encounter original (without-status) into target (upsert)
  // ========================================================================
  const stepBStart = Date.now();
  log(`STEP_B_START: restore encounter original (without-status) | from=${WS_ORIGINAL_BCK_COLL} -> to=${TARGET_COLL} | DRY_RUN=${dryRun} | BULK_UPSERT=${BULK_UPSERT}`);

  const wsOriginalCursor = db.getCollection(WS_ORIGINAL_BCK_COLL).find({}).sort({ _id: 1 });

  let upOps = [];
  let wsProcessed = 0;
  let upBulks = 0;

  while (wsOriginalCursor.hasNext()) {
    const doc = wsOriginalCursor.next();
    wsProcessed++;

    if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
      throw new Error(`Invalid doc read from ${WS_ORIGINAL_BCK_COLL} (processed=${wsProcessed})`);
    }
    if (doc._id === undefined) {
      throw new Error(`Doc without _id read from ${WS_ORIGINAL_BCK_COLL} (processed=${wsProcessed})`);
    }

    upOps.push({
      replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true }
    });

    if (upOps.length >= BULK_UPSERT) {
      upBulks++;
      if (!dryRun) db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
      upOps = [];

      if (LOG_EVERY > 0 && wsProcessed % LOG_EVERY === 0) {
        log(`STEP_B_PROGRESS: processed=${wsProcessed} upBulks=${upBulks} | elapsed=${fmtDuration(Date.now() - stepBStart)}`);
      }
    }
  }

  if (upOps.length > 0) {
    upBulks++;
    if (!dryRun) db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
    upOps = [];
  }

  log(`STEP_B_END: processed=${wsProcessed} upBulks=${upBulks} | elapsed=${fmtDuration(Date.now() - stepBStart)}`);

  // ========================================================================
  // STEP C (optional): cleanup encounter_history for without-status export file
  // ========================================================================
  if (!sourceExpDir) {
    log(`STEP_C_HISTORY_DELETE_SKIP: sourceExpDir not available (cannot resolve WS history export file).`);
  } else {
    const wsHistoryIds = loadIdsIfFileExists(wsHistoryExportPath, WS_HISTORY_EXPORT_CONST_NAME);
    if (!wsHistoryIds) {
      log(`STEP_C_HISTORY_DELETE_SKIP: export file not configured or not found. path=${wsHistoryExportPath}`);
    } else if (!historyExists) {
      log(`STEP_C_HISTORY_DELETE_SKIP: history collection not found: ${HISTORY_COLL}`);
    } else {
      bulkDeleteByIds(HISTORY_COLL, wsHistoryIds, "STEP_C_HISTORY_DELETE", dryRun, startMs);
    }
  }

  // ========================================================================
  // END / COUNTS
  // ========================================================================
  const targetCountAfter = db.getCollection(TARGET_COLL).countDocuments();
  const historyCountAfter = historyExists ? db.getCollection(HISTORY_COLL).countDocuments() : 0;

  log(
    `COUNTS_AFTER: target_after=${targetCountAfter} | delta=${targetCountAfter - targetCountBefore} | history_after=${historyCountAfter} | history_delta=${historyCountAfter - historyCountBefore}`
  );
  log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | DRY_RUN=${dryRun}`);
} catch (e) {
  log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}