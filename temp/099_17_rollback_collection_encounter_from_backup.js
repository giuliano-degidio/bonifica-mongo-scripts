// Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_17_rollback_collection_encounter_from_backup.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_17_rollback_collection_encounter_from_backup.js"
*/

// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_17_rollback_collection_encounter_from_backup.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Appo09\Mongo_Prod_EXP\099_17_rollback_collection_encounter_from_backup.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_17_rollback_collection_encounter_from_backup.js'

*/

/**
 * 099_17_rollback_collection_encounter_from_backup.js
 *
 * SCOPO
 * -----
 * Rollback bonifiche encounter.
 *
 * Questo script CONSERVA quello che faceva in precedenza e AGGIUNGE il rollback della bonifica
 * "encounter_without_status" (quella fatta con gli step 099_01_03B ... 099_02_04).
 *
 * VERSIONE PRECEDENTE (conservata):
 *  1) Ripristina nella collezione `encounter` tutti i documenti presenti nel backup
 *     `bonifica_encounter_deleted_bck` (creato da 099_backup_sio_impacted.js / rollback cancellazione Encounter).
 *  2) (OPZIONALE) Se presente un file export (vecchio), elimina da `encounter_history` i documenti inseriti
 *     dallo step 099_00B_insert_encounter_history.js.
 *
 * AGGIUNTA (ESEGUITA SEMPRE):
 *  A) Rollback encounter_without_status:
 *     - STEP A: rimuove da `encounter` i documenti "modificati" inseriti dalla bonifica
 *       (identificati dagli _id presenti in `bonifica_encounter_without_status_modified_inserted_bck`)
 *     - STEP B: ripristina in `encounter` i documenti originali cancellati dalla bonifica
 *       (leggendo i documenti da `bonifica_encounter_without_status_original_deleted_bck`)
 *     - STEP C (OPZIONALE): elimina da `encounter_history` i record inseriti da:
 *         099_02_01B_insert_encounter_history_from_without_status_original.js
 *       usando gli _id presenti nel file export:
 *         /data/Mongo_Sh_Script/EXP/encounter_history_without_status_inserted_id.js
 *
 * PRECONDIZIONI
 * ------------
 * - Deve esistere la collezione di backup: bonifica_encounter_deleted_bck
 * - Devono esistere le collection di backup della bonifica "without status":
 *     - bonifica_encounter_without_status_modified_inserted_bck
 *     - bonifica_encounter_without_status_original_deleted_bck
 * - Deve esistere la target collection: encounter
 *
 * SICUREZZA
 * ---------
 * - DRY_RUN: se true non scrive nulla, fa solo conteggi e simulazione.
 * - REQUIRE_CONFIRMATION: se true (e DRY_RUN=false) chiede conferma interattiva (digitare esattamente 'y').
 *
 * MODALITA' TECNICA
 * ----------------
 * - Ripristino encounter (rollback "vecchio"):
 *     legge tutti i documenti dal backup `bonifica_encounter_deleted_bck` (sorted per _id)
 *     bulkWrite di replaceOne con upsert:true su `encounter` (idempotente).
 *
 * - Cleanup encounter_history (rollback "vecchio", se file presente):
 *     legge lista _id dal file export (HISTORY_EXPORT_FILE_PATH / HISTORY_EXPORT_CONST_NAME)
 *     e fa bulkWrite di deleteOne su `encounter_history`.
 *
 * - Rollback encounter_without_status (AGGIUNTA):
 *     STEP A) delete da `encounter`:
 *       legge gli _id dalla collezione backup `bonifica_encounter_without_status_modified_inserted_bck`
 *       e fa bulkWrite di deleteOne su `encounter`.
 *
 *     STEP B) restore su `encounter`:
 *       legge tutti i documenti dalla collezione backup `bonifica_encounter_without_status_original_deleted_bck`
 *       e fa bulkWrite di replaceOne con upsert:true su `encounter`.
 *
 *     STEP C) cleanup encounter_history (without status, se file presente):
 *       legge lista _id dal file export (WS_HISTORY_EXPORT_FILE_PATH / WS_HISTORY_EXPORT_CONST_NAME)
 *       e fa bulkWrite di deleteOne su `encounter_history`.
 * LOG
 * ---
 * - START / COUNTS_BEFORE / STEP_* / PROGRESS / COUNTS_AFTER / END / ERROR_TIME
 */

const fs = require("fs");

// ============================================================================
// BACKUP / TARGET
// ============================================================================

// --- rollback "vecchio" (conservato) ---
const BACKUP_COLL = "bonifica_encounter_deleted_bck"; // restore full docs -> encounter (upsert)
const TARGET_COLL = "encounter";
const HISTORY_COLL = "encounter_history";

// --- rollback "encounter_without_status" (AGGIUNTA) ---
const WS_MODIFIED_BCK_COLL = "bonifica_encounter_without_status_modified_inserted_bck";
const WS_ORIGINAL_BCK_COLL = "bonifica_encounter_without_status_original_deleted_bck";

// ============================================================================
// PARAMETRI
// ============================================================================
const DRY_RUN = false;               // <<< mettere false per eseguire davvero
const REQUIRE_CONFIRMATION = false;  // <<< richiede input 'y' per procedere

// --- rollback encounter_history (OPZIONALE) - VECCHIO (conservato) ---
const HISTORY_EXPORT_FILE_PATH = "C:\\Appo10\\Mongo_Prod_EXP\\EXP\\encounter_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_CONST_NAME = "ENCOUNTER_HISTORY_INSERTED_ID"; // deve combaciare col file export

// --- rollback encounter_history (OPZIONALE) - WITHOUT STATUS (AGGIUNTA) ---
const WS_HISTORY_EXPORT_FILE_PATH = "/data/Mongo_Sh_Script/EXP/encounter_history_without_status_inserted_id.js"; // file export generato da 099_02_01B...
const WS_HISTORY_EXPORT_CONST_NAME = "ENCOUNTER_HISTORY_WITHOUT_STATUS_INSERTED_ID"; // deve combaciare col file export

// tuning
const BATCH = 1000;          // bulkWrite batch size (generico: restore vecchio)
const BULK_DELETE = 1000;    // per delete encounter (without status + history)
const BULK_UPSERT = 1000;    // per restore encounter (without status)
const LOG_EVERY = 5000;      // progress ogni N doc processati

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

function askYesNoBlocking(question) {
  if (typeof readlineSync !== "function") {
    throw new Error("readlineSync non disponibile in questo ambiente mongosh: impossibile chiedere conferma interattiva.");
  }
  return readlineSync(question);
}

function loadIdsIfFileExists(filePath, constName) {
  if (!filePath) return null;

  const p = String(filePath);
  if (!fs.existsSync(p)) return null;

 // carica il file che definisce la costante (es: ENCOUNTER_HISTORY_INSERTED_ID)
  load(p);
  const ids = globalThis[constName];
  if (!Array.isArray(ids)) {
    throw new Error(`Il file export esiste ma non definisce un array globale '${constName}'. File=${p}`);
  }
  return ids;
}

function bulkDeleteByIds(collName, ids, label) {
  if (!ids || ids.length === 0) {
    log(`${label}_SKIP: nessun id da cancellare.`);
    return { deleted: 0, bulks: 0, processed: 0 };
  }

  log(`${label}_START: coll=${collName} ids=${ids.length} DRY_RUN=${DRY_RUN} BULK_DELETE=${BULK_DELETE}`);

  let ops = [];
  let processed = 0;
  let bulks = 0;
  let deleted = 0;

  for (const id of ids) {
    processed++;
    ops.push({ deleteOne: { filter: { _id: id } } });

    if (ops.length >= BULK_DELETE) {
      bulks++;
      if (!DRY_RUN) {
        const res = db.getCollection(collName).bulkWrite(ops, { ordered: false });
        deleted += res.deletedCount || 0;
      }
      ops = [];

      if (LOG_EVERY > 0 && processed % LOG_EVERY === 0) {
        const t = Date.now();
        log(`${label}_PROGRESS: processed=${processed}/${ids.length} bulks=${bulks} deleted=${deleted} | elapsed=${fmtDuration(t - startMs)}`);
      }
    }
  }

  if (ops.length > 0) {
    bulks++;
    if (!DRY_RUN) {
      const res = db.getCollection(collName).bulkWrite(ops, { ordered: false });
      deleted += res.deletedCount || 0;
    }
    ops = [];
  }

  log(`${label}_END: processed=${processed} bulks=${bulks} deleted=${deleted}`);
  return { deleted, bulks, processed };
}

// ============================================================================
// VECCHIE FUNZIONI (conservate) - rinominate solo internamente dove necessario
// ============================================================================
function loadHistoryIdsIfFileExists() {
  return loadIdsIfFileExists(HISTORY_EXPORT_FILE_PATH, HISTORY_EXPORT_CONST_NAME);
}

function deleteEncounterHistoryByIds(ids) {
  // mantiene comportamento precedente (stessa label)
  return bulkDeleteByIds(HISTORY_COLL, ids, "HISTORY_DELETE");
}

// ============================================================================
// MAIN
// ============================================================================
const startMs = Date.now();
log(
  `START: rollback encounter from backup | TARGET_COLL=${TARGET_COLL} | BACKUP_COLL=${BACKUP_COLL} | WS_MODIFIED_BCK_COLL=${WS_MODIFIED_BCK_COLL} | WS_ORIGINAL_BCK_COLL=${WS_ORIGINAL_BCK_COLL} | HISTORY_COLL=${HISTORY_COLL} | DRY_RUN=${DRY_RUN} | REQUIRE_CONFIRMATION=${REQUIRE_CONFIRMATION} | BATCH=${BATCH} | BULK_DELETE=${BULK_DELETE} | BULK_UPSERT=${BULK_UPSERT} | LOG_EVERY=${LOG_EVERY} | HISTORY_EXPORT_FILE_PATH=${HISTORY_EXPORT_FILE_PATH} | WS_HISTORY_EXPORT_FILE_PATH=${WS_HISTORY_EXPORT_FILE_PATH}`
);

try {
  // ========================================================================
  // PRECHECKS
  // ========================================================================
  // NOTE: senza queste collection di backup, questo rollback non può funzionare.
  if (!collectionExists(TARGET_COLL)) throw new Error(`Target collection not found: ${TARGET_COLL}`);

  // vecchio backup: può non esistere in alcuni scenari storici, ma lo script prima richiedeva che esistesse.
  // Manteniamo lo stesso comportamento: se manca => errore.
  if (!collectionExists(BACKUP_COLL)) throw new Error(`Backup collection not found: ${BACKUP_COLL}`);

  // without-status backup (nuovi): richiesti e sempre eseguiti
  if (!collectionExists(WS_MODIFIED_BCK_COLL)) throw new Error(`Collection not found: ${WS_MODIFIED_BCK_COLL}`);
  if (!collectionExists(WS_ORIGINAL_BCK_COLL)) throw new Error(`Collection not found: ${WS_ORIGINAL_BCK_COLL}`);

  // history coll può non esistere; in quel caso i delete history verranno skippati
  const historyExists = collectionExists(HISTORY_COLL);

  const backupCount = db.getCollection(BACKUP_COLL).countDocuments();
  const wsModifiedCount = db.getCollection(WS_MODIFIED_BCK_COLL).countDocuments();
  const wsOriginalCount = db.getCollection(WS_ORIGINAL_BCK_COLL).countDocuments();

  const targetCountBefore = db.getCollection(TARGET_COLL).countDocuments();
  const historyCountBefore = historyExists ? db.getCollection(HISTORY_COLL).countDocuments() : 0;

  log(`COUNTS_BEFORE: backupCount(old)=${backupCount} | ws_modified_bck=${wsModifiedCount} | ws_original_bck=${wsOriginalCount} | target_before=${targetCountBefore} | history_exists=${historyExists} | history_before=${historyCountBefore}`);

  if (backupCount === 0) {
    log(`WARNING: ${BACKUP_COLL} è vuota. Nessun documento da ripristinare nello STEP 1.`);
  }
  if (wsModifiedCount === 0) {
    log(`WARNING: ${WS_MODIFIED_BCK_COLL} è vuota. Nessun documento da cancellare nello STEP A.`);
  }
  if (wsOriginalCount === 0) {
    log(`WARNING: ${WS_ORIGINAL_BCK_COLL} è vuota. Nessun documento da ripristinare nello STEP B.`);
  }

  // ========================================================================
  // CONFIRMATION (unica)
  // ========================================================================
  if (REQUIRE_CONFIRMATION && !DRY_RUN) {
    log("WARNING: Questa operazione eseguira' un ROLLBACK su encounter (e cleanup opzionale su encounter_history).");
    log(`WARNING: STEP 1 restore old backup: ${BACKUP_COLL} -> ${TARGET_COLL} (count=${backupCount})`);
    log(`WARNING: STEP A delete without-status modified: ${WS_MODIFIED_BCK_COLL} -> delete from ${TARGET_COLL} (count=${wsModifiedCount})`);
    log(`WARNING: STEP B restore without-status original: ${WS_ORIGINAL_BCK_COLL} -> ${TARGET_COLL} (count=${wsOriginalCount})`);
    log("WARNING: STEP 2 / STEP C: se presenti file export, verranno cancellati record da encounter_history.");
    log("WARNING: Per procedere digitare esattamente: y  (qualsiasi altra risposta annulla)");

    const ans = askYesNoBlocking("CONFERMI IL ROLLBACK ENCOUNTER (+history cleanup se configurato)? (y/N): ");
    if (String(ans).trim() !== "y") {
      log(`ABORTED: risposta='${ans}' (atteso 'y')`);
      quit(2);
    }
    log("CONFIRMED: procedo con il rollback.");
  } else if (DRY_RUN) {
    log("DRY_RUN: nessuna modifica sara' applicata (skip conferma interattiva).");
  }

  // ========================================================================
  // STEP 1 (VECCHIO - CONSERVATO): rollback encounter da BACKUP_COLL
  // ========================================================================
  log(`STEP_1_START: restore encounter from old backup | from=${BACKUP_COLL} -> to=${TARGET_COLL} | DRY_RUN=${DRY_RUN} | BATCH=${BATCH}`);

  const cursor = db.getCollection(BACKUP_COLL).find({}).sort({ _id: 1 });

  let ops = [];
  let processed = 0;
  let bulks = 0;

  let inserted = 0;
  let upserted = 0;
  let modified = 0;

  while (cursor.hasNext()) {
    const doc = cursor.next();
    processed++;

    if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
      throw new Error(`Documento non valido letto da ${BACKUP_COLL} (processed=${processed})`);
    }
    if (doc._id === undefined) {
      throw new Error(`Documento senza _id letto da ${BACKUP_COLL} (processed=${processed})`);
    }

    ops.push({
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc,
        upsert: true,
      },
    });

    if (ops.length >= BATCH) {
      bulks++;

      if (!DRY_RUN) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(ops, { ordered: false });
        inserted += res.insertedCount || 0;
        upserted += res.upsertedCount || 0;
        modified += res.modifiedCount || 0;
      }

      ops = [];

      if (LOG_EVERY > 0 && processed % LOG_EVERY === 0) {
        const t = Date.now();
        log(`STEP_1_PROGRESS: processed=${processed} bulks=${bulks} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(t - startMs)}`);
      }
    }
  }

  if (ops.length > 0) {
    bulks++;

    if (!DRY_RUN) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(ops, { ordered: false });
      inserted += res.insertedCount || 0;
      upserted += res.upsertedCount || 0;
      modified += res.modifiedCount || 0;
    }

    ops = [];
  }

  log(`STEP_1_END: TOTALS_ENCOUNTER(old_restore): backupCount=${backupCount} processed=${processed} bulks=${bulks} inserted=${inserted} upserted=${upserted} modified=${modified}`);

  // ========================================================================
  // STEP 2 (VECCHIO - OPZIONALE - CONSERVATO): cleanup encounter_history da file export "vecchio"
  // ========================================================================
  const historyIds = loadHistoryIdsIfFileExists();
  if (!historyIds) {
    log(`STEP_2_HISTORY_DELETE_SKIP: file non configurato o non trovato. path=${HISTORY_EXPORT_FILE_PATH}`);
  } else {
    if (!historyExists) {
      log(`STEP_2_HISTORY_DELETE_SKIP: collection history non trovata: ${HISTORY_COLL}`);
    } else {
      log(`STEP_2_START: old history cleanup`);
      deleteEncounterHistoryByIds(historyIds);
      log(`STEP_2_END: old history cleanup done`);
    }
  }

  // ========================================================================
  // STEP A (AGGIUNTA - SEMPRE): delete encounter "modified" inserted by without-status bonifica
  // =========================================================================
  log(`STEP_A_START: delete encounter modified (without-status) from ${TARGET_COLL} using ids from ${WS_MODIFIED_BCK_COLL} | DRY_RUN=${DRY_RUN} | BULK_DELETE=${BULK_DELETE}`);

  const wsModifiedCursor = db.getCollection(WS_MODIFIED_BCK_COLL).find({}, { projection: { _id: 1 } }).sort({ _id: 1 });

  let delOps = [];
  let wsDelProcessed = 0;
  let wsDelBulks = 0;
  let wsDelDeleted = 0;

  while (wsModifiedCursor.hasNext()) {
    const d = wsModifiedCursor.next();
    wsDelProcessed++;

    if (d && d._id !== undefined) {
      delOps.push({ deleteOne: { filter: { _id: d._id } } });
    }

    if (delOps.length >= BULK_DELETE) {
      wsDelBulks++;
      if (!DRY_RUN) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
        wsDelDeleted += res.deletedCount || 0;
      }
      delOps = [];

      if (LOG_EVERY > 0 && wsDelProcessed % LOG_EVERY === 0) {
        const t = Date.now();
        log(`STEP_A_PROGRESS: processed=${wsDelProcessed} bulks=${wsDelBulks} deleted=${wsDelDeleted} | elapsed=${fmtDuration(t - startMs)}`);
      }
    }
  }

  if (delOps.length > 0) {
    wsDelBulks++;
    if (!DRY_RUN) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
      wsDelDeleted += res.deletedCount || 0;
    }
    delOps = [];
  }

  log(`STEP_A_END: deleted_from_target=${wsDelDeleted} | processed_ids=${wsDelProcessed} | bulks=${wsDelBulks} | source=${WS_MODIFIED_BCK_COLL}`);

  // ========================================================================
  // STEP B (AGGIUNTA - SEMPRE): restore encounter original deleted by without-status bonifica
  // =========================================================================
  log(`STEP_B_START: restore encounter original (without-status) | from=${WS_ORIGINAL_BCK_COLL} -> to=${TARGET_COLL} | DRY_RUN=${DRY_RUN} | BULK_UPSERT=${BULK_UPSERT}`);

  const wsOriginalCursor = db.getCollection(WS_ORIGINAL_BCK_COLL).find({}).sort({ _id: 1 });

  let upOps = [];
  let wsUpProcessed = 0;
  let wsUpBulks = 0;
  let wsUpUpserted = 0; // conteggio best-effort (res.upsertedCount)
  let wsUpModified = 0;

  while (wsOriginalCursor.hasNext()) {
    const doc = wsOriginalCursor.next();
    wsUpProcessed++;

    if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
      throw new Error(`Documento non valido letto da ${WS_ORIGINAL_BCK_COLL} (processed=${wsUpProcessed})`);
    }
    if (doc._id === undefined) {
      throw new Error(`Documento senza _id letto da ${WS_ORIGINAL_BCK_COLL} (processed=${wsUpProcessed})`);
    }

    upOps.push({
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc,
        upsert: true,
      },
    });

    if (upOps.length >= BULK_UPSERT) {
      wsUpBulks++;
      if (!DRY_RUN) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
        wsUpUpserted += res.upsertedCount || 0;
        wsUpModified += res.modifiedCount || 0;
      }
      upOps = [];

      if (LOG_EVERY > 0 && wsUpProcessed % LOG_EVERY === 0) {
        const t = Date.now();
        log(`STEP_B_PROGRESS: processed=${wsUpProcessed} bulks=${wsUpBulks} upserted=${wsUpUpserted} modified=${wsUpModified} | elapsed=${fmtDuration(t - startMs)}`);
      }
    }
  }

  if (upOps.length > 0) {
    wsUpBulks++;
    if (!DRY_RUN) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
      wsUpUpserted += res.upsertedCount || 0;
      wsUpModified += res.modifiedCount || 0;
    }
    upOps = [];
  }

  log(`STEP_B_END: processed=${wsUpProcessed} bulks=${wsUpBulks} upserted=${wsUpUpserted} modified=${wsUpModified} | source=${WS_ORIGINAL_BCK_COLL}`);

  // ========================================================================
  // STEP C (AGGIUNTA - OPZIONALE): cleanup encounter_history for without-status bonifica
  // =========================================================================
  const wsHistoryIds = loadIdsIfFileExists(WS_HISTORY_EXPORT_FILE_PATH, WS_HISTORY_EXPORT_CONST_NAME);
  if (!wsHistoryIds) {
    log(`STEP_C_HISTORY_DELETE_SKIP: file non configurato o non trovato. path=${WS_HISTORY_EXPORT_FILE_PATH}`);
  } else {
    if (!historyExists) {
      log(`STEP_C_HISTORY_DELETE_SKIP: collection history non trovata: ${HISTORY_COLL}`);
    } else {
      log(`STEP_C_START: without-status history cleanup | file=${WS_HISTORY_EXPORT_FILE_PATH} | const=${WS_HISTORY_EXPORT_CONST_NAME}`);
      bulkDeleteByIds(HISTORY_COLL, wsHistoryIds, "WS_HISTORY_DELETE");
      log(`STEP_C_END: without-status history cleanup done`);
    }
  }

  // ========================================================================
  // END / COUNTS
  // =========================================================================
  const endMs = Date.now();
  const targetCountAfter = db.getCollection(TARGET_COLL).countDocuments();
  const historyCountAfter = historyExists ? db.getCollection(HISTORY_COLL).countDocuments() : 0;

  log(`COUNTS_AFTER: target_after=${targetCountAfter} | delta=${targetCountAfter - targetCountBefore} | history_after=${historyCountAfter} | history_delta=${historyCountAfter - historyCountBefore}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | DRY_RUN=${DRY_RUN}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}