// Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_02_03_rename_collection_encounter_without_status_original_to_backup.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo10\Mongo_Prod_EXP\099_02_03_rename_collection_encounter_without_status_original_to_backup.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_02_03_rename_collection_encounter_without_status_original_to_backup.js"

Spostare il file da Windows a Ubuntu Kubernetes:
/*
Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\099_02_03_rename_collection_encounter_without_status_original_to_backup.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_02_03_rename_collection_encounter_without_status_original_to_backup.js'
*/
/*
log:
[2026-04-23T13:12:26.886Z] START: SOURCE_COLL=encounter_without_status_original | TARGET_COLL=bonifica_encounter_without_status_original_deleted_bck
[2026-04-23T13:12:27.007Z] RENAME_START: encounter_without_status_original -> bonifica_encounter_without_status_original_deleted_bck
[2026-04-23T13:12:27.088Z] RENAME_END: encounter_without_status_original -> bonifica_encounter_without_status_original_deleted_bck | elapsed=0h 0m 0s
[2026-04-23T13:12:27.256Z] VERIFY: sourceExists=false | targetExists=true
[2026-04-23T13:12:27.257Z] END: elapsed=0h 0m 0s

*/

// Rinomina una collezione.
// Da: encounter_without_status_original
// A : bonifica_encounter_without_status_original_deleted_bck
//
// NOTE:
// - Operazione atomica lato MongoDB (renameCollection).
// - Se la collection target esiste già: verrà droppata (true).

const SOURCE_COLL = "encounter_without_status_original";
const TARGET_COLL = "bonifica_encounter_without_status_original_deleted_bck";

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

const startMs = Date.now();
log(`START: SOURCE_COLL=${SOURCE_COLL} | TARGET_COLL=${TARGET_COLL}`);

try {
  if (!collectionExists(SOURCE_COLL)) throw new Error(`Source collection non trovata: ${SOURCE_COLL}`);

  const renameStart = Date.now();
  log(`RENAME_START: ${SOURCE_COLL} -> ${TARGET_COLL}`);
  db.getCollection(SOURCE_COLL).renameCollection(TARGET_COLL, true);
  const renameEnd = Date.now();
  log(`RENAME_END: ${SOURCE_COLL} -> ${TARGET_COLL} | elapsed=${fmtDuration(renameEnd - renameStart)}`);

  log(`VERIFY: sourceExists=${collectionExists(SOURCE_COLL)} | targetExists=${collectionExists(TARGET_COLL)}`);
  const endMs = Date.now();
  log(`END: elapsed=${fmtDuration(endMs - startMs)}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}