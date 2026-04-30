// Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
  --quiet ^
  --file "C:\Appo09\Mongo_Prod_EXP\067_17_rollback_collection_bundle_from_backup.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\067_17_rollback_collection_bundle_from_backup.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/067_17_rollback_collection_bundle_from_backup.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR_TEST\067_17_rollback_collection_bundle_from_backup.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/067_17_rollback_collection_bundle_from_backup.js'


 OUTPUT:

*/

/**
 * 067_17_rollback_collection_bundle_from_backup.js
 *
 * Rollback della bonifica bundle:
 * - Delete da bundle degli _id presenti in bonifica_bundle_inserted_bck
 * - Restore (upsert) in bundle dei doc in bonifica_bundle_deleted_bck
 * - (OPZIONALE) ELIMINA da bundle_history i record inseriti dal passo
 *   067_01B_insert_bundle_history_from_impacted_original.js, utilizzando gli _id presenti
 *   nel file export generato (es. bundle_history_inserted_id_....js).
 *   Se il file non esiste / non è configurato, questo passo viene SKIPPATO (guardia di esistenza file).
 */

const fs = require("fs");

const MODIFIED_BCK_COLL = "bonifica_bundle_inserted_bck";
const ORIGINAL_BCK_COLL = "bonifica_bundle_deleted_bck";
const TARGET_COLL = "bundle";

// history cleanup
const HISTORY_COLL = "bundle_history";
//const HISTORY_EXPORT_FILE_PATH = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\bundle_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_FILE_PATH = "/data/Mongo_Sh_Script/EXP/bundle_history_inserted_id.js"; // <-- aggiornare al file reale
const HISTORY_EXPORT_CONST_NAME = "BUNDLE_HISTORY_INSERTED_ID";

const DRY_RUN = true;
const BULK_DELETE = 1000;
const BULK_UPSERT = 1000;

function collectionExists(name) {
  return db.getCollectionNames().includes(name) || db.getCollectionInfos({ name }).length > 0;
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

function deleteHistoryByIds(ids) {
  if (!ids || ids.length === 0) {
    print("STEP_C_SKIP: nessun id history da cancellare.");
    return;
  }
  if (!collectionExists(HISTORY_COLL)) {
    print(`STEP_C_SKIP: collection history non trovata: ${HISTORY_COLL}`);
    return;
  }

  print(`STEP_C_START: delete from ${HISTORY_COLL} using export file ids | ids=${ids.length} | DRY_RUN=${DRY_RUN}`);

  const cur = db.getCollection(HISTORY_COLL);
  let ops = [];
  let bulks = 0;

  for (const id of ids) {
    ops.push({ deleteOne: { filter: { _id: id } } });

    if (ops.length >= BULK_DELETE) {
      bulks++;
      if (!DRY_RUN) cur.bulkWrite(ops, { ordered: false });
      ops = [];
    }
  }
  if (ops.length > 0) {
    bulks++;
    if (!DRY_RUN) cur.bulkWrite(ops, { ordered: false });
    ops = [];
  }

  print(`STEP_C_END: deleted bulks=${bulks} (DRY_RUN=${DRY_RUN})`);
}

print(`START: rollback bundle | TARGET_COLL=${TARGET_COLL} | DRY_RUN=${DRY_RUN}`);

try {
  if (!collectionExists(MODIFIED_BCK_COLL)) throw new Error(`Collection not found: ${MODIFIED_BCK_COLL}`);
  if (!collectionExists(ORIGINAL_BCK_COLL)) throw new Error(`Collection not found: ${ORIGINAL_BCK_COLL}`);
  if (!collectionExists(TARGET_COLL)) throw new Error(`Target collection not found: ${TARGET_COLL}`);

  // STEP A: delete modified
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

  // STEP B: restore original
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

  // STEP C: optional history cleanup
  const historyIds = loadHistoryIdsIfFileExists();
  if (!historyIds) print(`STEP_C_SKIP: file export non configurato o non trovato. path=${HISTORY_EXPORT_FILE_PATH}`);
  else deleteHistoryByIds(historyIds);

  print("END: rollback bundle (DRY_RUN=" + DRY_RUN + ")");
} catch (e) {
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}