// Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\017_17_rollback_collection_servicerequest_from_backup.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\017_17_rollback_collection_servicerequest_from_backup.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/017_17_rollback_collection_servicerequest_from_backup.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR_TEST\017_17_rollback_collection_servicerequest_from_backup.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/017_17_rollback_collection_servicerequest_from_backup.js'

 OUTPUT:

*/

/**
 * 017_17_rollback_collection_servicerequest_from_backup.js
 *
 * SCOPO
 * -----
 * Rollback della bonifica servicerequest:
 * - Rimuove da servicerequest i documenti "modificati" inseriti dal passo 017_02
 *   (identificati dagli _id presenti nella collezione backup: bonifica_servicerequest_inserted_bck)
 * - Re-inserisce (restore) in servicerequest i documenti originali cancellati dal passo 017_01
 *   (leggendo i documenti dalla collezione backup: bonifica_servicerequest_deleted_bck)
 * - (OPZIONALE) ELIMINA da servicerequest_history i record inseriti dal passo
 *   017_01B_insert_servicerequest_history_from_impacted_original.js, utilizzando gli _id presenti
 *   nel file export generato da quello script (es. servicerequest_history_inserted_id_....js).
 *   Se il file non esiste / non è configurato, questo passo viene SKIPPATO (guardia di esistenza file).
 */

const fs = require("fs");

const MODIFIED_BCK_COLL = "bonifica_servicerequest_inserted_bck";
const ORIGINAL_BCK_COLL = "bonifica_servicerequest_deleted_bck";
const TARGET_COLL = "servicerequest";

// ====== AGGIUNTA: rollback history (opzionale) ======
const HISTORY_COLL = "servicerequest_history";
//const HISTORY_EXPORT_FILE_PATH = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\servicerequest_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_FILE_PATH = "/data/Mongo_Sh_Script/EXP/servicerequest_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_CONST_NAME = "SERVICEREQUEST_HISTORY_INSERTED_ID";

// ====== PARAMETRI ======
const DRY_RUN = true;
const REQUIRE_CONFIRMATION = false;

// tuning
const BULK_DELETE = 1000;
const BULK_UPSERT = 1000;

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

  load(p);
  const ids = globalThis[HISTORY_EXPORT_CONST_NAME];
  if (!Array.isArray(ids)) throw new Error(`Il file history export esiste ma non definisce un array globale '${HISTORY_EXPORT_CONST_NAME}'. File=${p}`);
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
      if (!DRY_RUN) {
        const res = db.getCollection(HISTORY_COLL).bulkWrite(ops, { ordered: false });
        deletedTotal += res.deletedCount || 0;
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
  `START: rollback servicerequest | TARGET_COLL=${TARGET_COLL} | MODIFIED_BCK_COLL=${MODIFIED_BCK_COLL} | ORIGINAL_BCK_COLL=${ORIGINAL_BCK_COLL} | HISTORY_COLL=${HISTORY_COLL} | DRY_RUN=${DRY_RUN} | REQUIRE_CONFIRMATION=${REQUIRE_CONFIRMATION} | BULK_DELETE=${BULK_DELETE} | BULK_UPSERT=${BULK_UPSERT} | HISTORY_EXPORT_FILE_PATH=${HISTORY_EXPORT_FILE_PATH}`
);

try {
  if (!collectionExists(MODIFIED_BCK_COLL)) throw new Error(`Collection not found: ${MODIFIED_BCK_COLL}`);
  if (!collectionExists(ORIGINAL_BCK_COLL)) throw new Error(`Collection not found: ${ORIGINAL_BCK_COLL}`);
  if (!collectionExists(TARGET_COLL)) throw new Error(`Target collection not found: ${TARGET_COLL}`);

  const modifiedCount = db.getCollection(MODIFIED_BCK_COLL).countDocuments();
  const originalCount = db.getCollection(ORIGINAL_BCK_COLL).countDocuments();
  const targetCountBefore = db.getCollection(TARGET_COLL).countDocuments();

  log(`COUNTS_BEFORE: modified_bck=${modifiedCount} | original_bck=${originalCount} | target_before=${targetCountBefore}`);

  if (REQUIRE_CONFIRMATION && !DRY_RUN) {
    log("WARNING: Questa operazione eseguira' un ROLLBACK su servicerequest.");
    log("WARNING: Step C (opzionale): delete da servicerequest_history per _id in file export (se presente).");
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

  // STEP A delete
  const modifiedCursor = db.getCollection(MODIFIED_BCK_COLL).find({}, { projection: { _id: 1 } }).sort({ _id: 1 });
  let delOps = [];
  while (modifiedCursor.hasNext()) {
    const d = modifiedCursor.next();
    if (d && d._id !== undefined) delOps.push({ deleteOne: { filter: { _id: d._id } } });
    if (delOps.length >= BULK_DELETE) {
      if (!DRY_RUN) db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
      delOps = [];
    }
  }
  if (delOps.length > 0) {
    if (!DRY_RUN) db.getCollection(TARGET_COLL).bulkWrite(delOps, { ordered: false });
    delOps = [];
  }

  // STEP B restore
  const originalCursor = db.getCollection(ORIGINAL_BCK_COLL).find({}).sort({ _id: 1 });
  let upOps = [];
  while (originalCursor.hasNext()) {
    const doc = originalCursor.next();
    if (!doc || typeof doc !== "object" || Array.isArray(doc)) throw new Error(`Documento non valido letto da ${ORIGINAL_BCK_COLL}`);
    if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${ORIGINAL_BCK_COLL}`);
    upOps.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    if (upOps.length >= BULK_UPSERT) {
      if (!DRY_RUN) db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
      upOps = [];
    }
  }
  if (upOps.length > 0) {
    if (!DRY_RUN) db.getCollection(TARGET_COLL).bulkWrite(upOps, { ordered: false });
    upOps = [];
  }

  // STEP C optional history cleanup
  const historyIds = loadHistoryIdsIfFileExists();
  if (!historyIds) log(`STEP_C_SKIP: file export non configurato o non trovato. path=${HISTORY_EXPORT_FILE_PATH}`);
  else deleteHistoryByIds(historyIds, startMs);




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