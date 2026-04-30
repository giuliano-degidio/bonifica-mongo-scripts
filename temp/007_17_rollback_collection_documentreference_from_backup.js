// Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\007_17_rollback_collection_documentreference_from_backup.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\007_17_rollback_collection_documentreference_from_backup.js"
*/

// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/007_17_rollback_collection_documentreference_from_backup.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR_TEST\007_17_rollback_collection_documentreference_from_backup.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/007_17_rollback_collection_documentreference_from_backup.js'


 OUTPUT:
[2026-04-10T15:25:14.899Z] START: rollback documentreference | TARGET_COLL=documentreference | MODIFIED_BCK_COLL=bonifica_documentreference_inserted_bck | ORIGINAL_BCK_COLL=bonifica_documentreference_deleted_bck | HISTORY_COLL=documentreference_history | DRY_RUN=false | REQUIRE_CONFIRMATION=false | BULK_DELETE=1000 | BULK_UPSERT=1000 | HISTORY_EXPORT_FILE_PATH=/data/Mongo_Sh_Script/EXP/documentreference_history_inserted_id.js
[2026-04-10T15:25:17.550Z] COUNTS_BEFORE: modified_bck=32 | original_bck=32 | target_before=44888
[2026-04-10T15:25:17.551Z] STEP_A_START: delete modified docs from documentreference using _id list from bonifica_documentreference_inserted_bck
[2026-04-10T15:25:22.471Z] STEP_A_END: readModifiedIds=32 deleteOpsPrepared=32 deletedTotal=32 delBulks=1 | elapsed=0h 0m 4s
[2026-04-10T15:25:22.471Z] STEP_B_START: restore original docs into documentreference from bonifica_documentreference_deleted_bck (upsert)
[2026-04-10T15:25:22.683Z] STEP_B_END: readOriginalDocs=32 upsertOpsPrepared=32 upBulks=1 inserted=0 upserted=32 modified=0 | elapsed=0h 0m 0s
[2026-04-10T15:25:22.684Z] STEP_C_SKIP: file export non configurato o non trovato. path=/data/Mongo_Sh_Script/EXP/documentreference_history_inserted_id.js
[2026-04-10T15:25:22.908Z] COUNTS_AFTER: target_after=44888 | delta=0
[2026-04-10T15:25:22.908Z] END: elapsed=0h 0m 7s | DRY_RUN=false

*/

/**
 * 007_17_rollback_collection_documentreference_from_backup.js
 *
 * SCOPO
 * -----
 * Rollback della bonifica DocumentReference:
 * - Rimuove da documentreference i documenti "modificati" inseriti dal passo 007_02
 *   (identificati dagli _id presenti nella collezione backup: bonifica_documentreference_inserted_bck)
 * - Re-inserisce (restore) in documentreference i documenti originali cancellati dal passo 007_01
 *   (leggendo i documenti dalla collezione backup: bonifica_documentreference_deleted_bck)
 * - (OPZIONALE) ELIMINA da documentreference_history i record inseriti dal passo
 *   007_01B_insert_documentreference_history_from_impacted_original.js, utilizzando gli _id presenti
 *   nel file export generato da quello script (es. documentreference_history_inserted_id_....js).
 *   Se il file non esiste / non è configurato, questo passo viene SKIPPATO (guardia di esistenza file).
 *
 * PRECONDIZIONI
 * ------------
 * - 007_03 eseguito: documentreference_impacted_original -> bonifica_documentreference_deleted_bck
 * - 007_04 eseguito: documentreference_impacted_modified -> bonifica_documentreference_inserted_bck
 *
 * SICUREZZA
 * ---------
 * - DRY_RUN: se true non modifica nulla, fa solo conteggi e simula i passi.
 * - REQUIRE_CONFIRMATION: se true chiede conferma interattiva (digitare esattamente 'y').
 *
 * MODALITA' TECNICA
 * ----------------
 * STEP A) Delete in documentreference per ogni _id in bonifica_documentreference_inserted_bck (bulkWrite deleteOne)
 * STEP B) Restore in documentreference di tutti i doc in bonifica_documentreference_deleted_bck (bulkWrite replaceOne upsert)
 * STEP C) (Opzionale) Delete in documentreference_history per ogni _id nel file export JS (bulkWrite deleteOne)
 *
 * LOG
 * ---
 * - START/PROGRESS/END con elapsed
 * - stats per ciascuno step
 */

const fs = require("fs");

const MODIFIED_BCK_COLL = "bonifica_documentreference_inserted_bck";
const ORIGINAL_BCK_COLL = "bonifica_documentreference_deleted_bck";
const TARGET_COLL = "documentreference";

// ====== AGGIUNTA: rollback history (opzionale) ======
const HISTORY_COLL = "documentreference_history";
//const HISTORY_EXPORT_FILE_PATH = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\documentreference_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_FILE_PATH = "/data/Mongo_Sh_Script/EXP/documentreference_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_CONST_NAME = "DOCUMENTREFERENCE_HISTORY_INSERTED_ID";

// ====== PARAMETRI ======
const DRY_RUN = false;               // <<< mettere false per eseguire davvero
const REQUIRE_CONFIRMATION = false; // <<< richiede input 'y' per procedere

// tuning
const BULK_DELETE = 1000;   // quante deleteOne per bulk
const BULK_UPSERT = 1000;   // quante replaceOne/upsert per bulk

// log
const LOG_EVERY_DELETES = 5000;
const LOG_EVERY_UPSERTS = 5000;
const LOG_EVERY_HISTORY_DELETES = 5000;

// ====== UTILS ======
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

function loadHistoryIdsIfFileExists() {
  if (!HISTORY_EXPORT_FILE_PATH) return null;
  const p = String(HISTORY_EXPORT_FILE_PATH);
  if (!fs.existsSync(p)) return null;

  load(p); // deve definire la costante HISTORY_EXPORT_CONST_NAME

  const ids = globalThis[HISTORY_EXPORT_CONST_NAME];
  if (!Array.isArray(ids)) {
    throw new Error(`Il file history export esiste ma non definisce un array globale '${HISTORY_EXPORT_CONST_NAME}'. File=${p}`);
  }
  return ids;
}

function deleteHistoryByIds(ids, startMs) {
  if (!ids || ids.length === 0) {
    log("STEP_C_SKIP: nessun id history da cancellare.");
    return { deleted: 0, bulks: 0 };
  }
  if (!collectionExists(HISTORY_COLL)) {
    log(`STEP_C_SKIP: collection history non trovata: ${HISTORY_COLL}`);
    return { deleted: 0, bulks: 0 };
  }

  const stepCStart = Date.now();
  log(`STEP_C_START: delete from ${HISTORY_COLL} using ids from export file | ids=${ids.length} | DRY_RUN=${DRY_RUN}`);

  let ops = [];
  let processed = 0;
  let bulks = 0;
  let deletedTotal = 0;

  for (const id of ids) {
    processed++;
    ops.push({ deleteOne: { filter: { _id: id } } });

    if (ops.length >= BULK_DELETE) {
      bulks++;
      let deletedThisBulk = 0;
      if (!DRY_RUN) {
        const res = db.getCollection(HISTORY_COLL).bulkWrite(ops, { ordered: false });
        deletedThisBulk = res.deletedCount || 0;
        deletedTotal += deletedThisBulk;
      }
      ops = [];

      if (LOG_EVERY_HISTORY_DELETES > 0 && processed % LOG_EVERY_HISTORY_DELETES === 0) {
        const t = Date.now();
        log(`STEP_C_PROGRESS: processed=${processed}/${ids.length} bulks=${bulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(t - stepCStart)}`);
      }
    }
  }

  if (ops.length > 0) {
    bulks++;
    if (!DRY_RUN) {
      const res = db.getCollection(HISTORY_COLL).bulkWrite(ops, { ordered: false });
      deletedTotal += res.deletedCount || 0;
    }
    ops = [];
  }

  const stepCEnd = Date.now();
  log(`STEP_C_END: processed=${processed} bulks=${bulks} deletedTotal=${deletedTotal} | elapsed=${fmtDuration(stepCEnd - stepCStart)}`);
  return { deleted: deletedTotal, bulks };
}

// ====== MAIN ======
const startMs = Date.now();
log(
  `START: rollback documentreference | TARGET_COLL=${TARGET_COLL} | MODIFIED_BCK_COLL=${MODIFIED_BCK_COLL} | ORIGINAL_BCK_COLL=${ORIGINAL_BCK_COLL} | HISTORY_COLL=${HISTORY_COLL} | DRY_RUN=${DRY_RUN} | REQUIRE_CONFIRMATION=${REQUIRE_CONFIRMATION} | BULK_DELETE=${BULK_DELETE} | BULK_UPSERT=${BULK_UPSERT} | HISTORY_EXPORT_FILE_PATH=${HISTORY_EXPORT_FILE_PATH}`
);

try {
  // --- checks ---
  if (!collectionExists(MODIFIED_BCK_COLL)) throw new Error(`Collection not found: ${MODIFIED_BCK_COLL}`);
  if (!collectionExists(ORIGINAL_BCK_COLL)) throw new Error(`Collection not found: ${ORIGINAL_BCK_COLL}`);
  if (!collectionExists(TARGET_COLL)) throw new Error(`Target collection not found: ${TARGET_COLL}`);

  const modifiedCount = db.getCollection(MODIFIED_BCK_COLL).countDocuments();
  const originalCount = db.getCollection(ORIGINAL_BCK_COLL).countDocuments();
  const targetCountBefore = db.getCollection(TARGET_COLL).countDocuments();

  log(`COUNTS_BEFORE: modified_bck=${modifiedCount} | original_bck=${originalCount} | target_before=${targetCountBefore}`);

  // --- confirmation ---
  if (REQUIRE_CONFIRMATION && !DRY_RUN) {
    log("WARNING: Questa operazione eseguira' un ROLLBACK su documentreference.");
    log(`WARNING: Step A: delete da ${TARGET_COLL} per _id presenti in ${MODIFIED_BCK_COLL} (count=${modifiedCount}).`);
    log(`WARNING: Step B: restore (upsert) in ${TARGET_COLL} da ${ORIGINAL_BCK_COLL} (count=${originalCount}).`);
    log("WARNING: Step C (opzionale): delete da documentreference_history per _id in file export (se presente).");
    log("WARNING: Per procedere digitare esattamente: y  (qualsiasi altra risposta annulla)");

    const ans = askYesNoBlocking("CONFERMI IL ROLLBACK? (y/N): ");
    if (String(ans).trim() !== "y") {
      log(`ABORTED: risposta='${ans}' (atteso 'y')`);
      quit(2);
    }
    log("CONFIRMED: procedo con il rollback.");
  } else if (DRY_RUN) {
    log("DRY_RUN: nessuna modifica sara' applicata (skip conferma interattiva).");
  }

  // =========================================================
  // STEP A) DELETE modified from TARGET
  // =========================================================
  const stepAStart = Date.now();
  log(`STEP_A_START: delete modified docs from ${TARGET_COLL} using _id list from ${MODIFIED_BCK_COLL}`);

  const modifiedCursor = db.getCollection(MODIFIED_BCK_COLL)
    .find({}, { projection: { _id: 1 } })
    .sort({ _id: 1 });

  let delOps = [];
  let readModifiedIds = 0;
  let deleteOpsPrepared = 0;
  let deletedTotal = 0;
  let delBulks = 0;

  while (modifiedCursor.hasNext()) {
    const d = modifiedCursor.next();
    readModifiedIds++;

    if (d && d._id !== undefined) {
      delOps.push({ deleteOne: { filter: { _id: d._id } } });
      deleteOpsPrepared++;
    }

    if (delOps.length >= BULK_DELETE) {
      delBulks++;
      let deletedThisBulk = 0;

      if (!DRY_RUN) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
        deletedThisBulk = res.deletedCount || 0;
        deletedTotal += deletedThisBulk;
      }

      if (LOG_EVERY_DELETES > 0 && deleteOpsPrepared % LOG_EVERY_DELETES === 0) {
        const t = Date.now();
        log(`STEP_A_PROGRESS: prepared=${deleteOpsPrepared} deletedTotal=${deletedTotal} delBulks=${delBulks} | elapsed=${fmtDuration(t - stepAStart)}`);
      }

      delOps = [];
    }
  }

  // flush finale delete
  if (delOps.length > 0) {
    delBulks++;
    let deletedThisBulk = 0;

    if (!DRY_RUN) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
      deletedThisBulk = res.deletedCount || 0;
      deletedTotal += deletedThisBulk;
    }

    delOps = [];
  }

  const stepAEnd = Date.now();
  log(`STEP_A_END: readModifiedIds=${readModifiedIds} deleteOpsPrepared=${deleteOpsPrepared} deletedTotal=${deletedTotal} delBulks=${delBulks} | elapsed=${fmtDuration(stepAEnd - stepAStart)}`);

  // =========================================================
  // STEP B) RESTORE original into TARGET (upsert)
  // =========================================================
  const stepBStart = Date.now();
  log(`STEP_B_START: restore original docs into ${TARGET_COLL} from ${ORIGINAL_BCK_COLL} (upsert)`);

  const originalCursor = db.getCollection(ORIGINAL_BCK_COLL)
    .find({})
    .sort({ _id: 1 });

  let upOps = [];
  let readOriginalDocs = 0;
  let upsertOpsPrepared = 0;
  let upBulks = 0;

  let upInserted = 0;
  let upUpserted = 0;
  let upModified = 0;

  while (originalCursor.hasNext()) {
    const doc = originalCursor.next();
    readOriginalDocs++;

    if (doc == null || typeof doc !== "object" || Array.isArray(doc)) throw new Error(`Documento non valido letto da ${ORIGINAL_BCK_COLL} (readOriginalDocs=${readOriginalDocs})`);
    if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${ORIGINAL_BCK_COLL} (readOriginalDocs=${readOriginalDocs})`);

    upOps.push({
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc,
        upsert: true,
      },
    });
    upsertOpsPrepared++;

    if (upOps.length >= BULK_UPSERT) {
      upBulks++;

      if (!DRY_RUN) {
        const res = db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
        upInserted += res.insertedCount || 0;
        upUpserted += res.upsertedCount || 0;
        upModified += res.modifiedCount || 0;
      }

      if (LOG_EVERY_UPSERTS > 0 && upsertOpsPrepared % LOG_EVERY_UPSERTS === 0) {
        const t = Date.now();
        log(`STEP_B_PROGRESS: prepared=${upsertOpsPrepared} readOriginalDocs=${readOriginalDocs} upBulks=${upBulks} inserted=${upInserted} upserted=${upUpserted} modified=${upModified} | elapsed=${fmtDuration(t - stepBStart)}`);
      }

      upOps = [];
    }
  }

  if (upOps.length > 0) {
    upBulks++;
    if (!DRY_RUN) {
      const res = db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
      upInserted += res.insertedCount || 0;
      upUpserted += res.upsertedCount || 0;
      upModified += res.modifiedCount || 0;
    }
    upOps = [];
  }

  const stepBEnd = Date.now();
  log(`STEP_B_END: readOriginalDocs=${readOriginalDocs} upsertOpsPrepared=${upsertOpsPrepared} upBulks=${upBulks} inserted=${upInserted} upserted=${upUpserted} modified=${upModified} | elapsed=${fmtDuration(stepBEnd - stepBStart)}`);

  // =========================================================
  // STEP C) OPTIONAL: delete history docs from export file
  // =========================================================
  const historyIds = loadHistoryIdsIfFileExists();
  if (!historyIds) {
    log(`STEP_C_SKIP: file export non configurato o non trovato. path=${HISTORY_EXPORT_FILE_PATH}`);
  } else {
    deleteHistoryByIds(historyIds, startMs);
  }

  // =========================================================
  // END
  // =========================================================
  const endMs = Date.now();
  const targetCountAfter = db.getCollection(TARGET_COLL).countDocuments();

  log(`COUNTS_AFTER: target_after=${targetCountAfter} | delta=${targetCountAfter - targetCountBefore}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | DRY_RUN=${DRY_RUN}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}