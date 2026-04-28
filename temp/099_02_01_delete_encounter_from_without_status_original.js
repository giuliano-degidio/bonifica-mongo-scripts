//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_02_01_delete_encounter_from_without_status_original.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo10\Mongo_Prod_EXP\099_02_01_delete_encounter_from_without_status_original.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_02_01_delete_encounter_from_without_status_original.js"

Spostare il file da Windows a Ubuntu Kubernetes:
/*
Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\099_02_01_delete_encounter_from_without_status_original.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_02_01_delete_encounter_from_without_status_original.js'
*/
/*
log:
[2026-04-23T13:04:17.222Z] START: SOURCE_COLL=encounter_without_status_original | TARGET_COLL=encounter | DRY_RUN=false | BATCH_IDS=1000 | BULK_DELETE=1000 | LOG_EVERY_DELETES=1000
[2026-04-23T13:04:28.859Z] BULK_END #1: ops=19 deleted=19 | totalDeleted=19 | elapsed=0h 0m 11s
[2026-04-23T13:04:28.860Z] TOTALS: readIds=19 deleteOpsPrepared=19 bulks=1 totalDeleted=19 DRY_RUN=false
[2026-04-23T13:04:28.861Z] END: elapsed=0h 0m 11s | SOURCE_COLL=encounter_without_status_original | TARGET_COLL=encounter
*/

// Legge gli _id dalla collezione encounter_without_status_original
// ed elimina i documenti corrispondenti dalla collezione encounter.
//
// NOTE:
// - Operazione distruttiva: impostare DRY_RUN=true per vedere i numeri senza cancellare.

const SOURCE_COLL = "encounter_without_status_original";
const TARGET_COLL = "encounter";

const BATCH_IDS = 1000;
const BULK_DELETE = 1000;
const LOG_EVERY_DELETES = 1000;

const DRY_RUN = false;

function now() { return new Date().toISOString(); }
function log(msg) { print(`[${now()}] ${msg}`); }
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

const startMs = Date.now();
log(`START: SOURCE_COLL=${SOURCE_COLL} | TARGET_COLL=${TARGET_COLL} | DRY_RUN=${DRY_RUN} | BATCH_IDS=${BATCH_IDS} | BULK_DELETE=${BULK_DELETE} | LOG_EVERY_DELETES=${LOG_EVERY_DELETES}`);

try {
  const source = db.getCollection(SOURCE_COLL);
  const target = db.getCollection(TARGET_COLL);

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

    if (ops.length >= BULK_DELETE) {
      bulks++;
      const bulkStart = Date.now();

      let deletedThisBulk = 0;
      if (!DRY_RUN) {
        const res = target.bulkWrite(ops, { ordered: false });
        deletedThisBulk = res.deletedCount || 0;
        totalDeleted += deletedThisBulk;
      }

      const bulkEnd = Date.now();
      log(`BULK_END #${bulks}: ops=${ops.length} deleted=${deletedThisBulk} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);

      if (LOG_EVERY_DELETES > 0 && totalDeleted > 0 && (totalDeleted % LOG_EVERY_DELETES) < deletedThisBulk) {
        const t = Date.now();
        log(`PROGRESS: readIds=${readIds} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(t - startMs)}`);
      }

      ops = [];
    }
  }

  if (ops.length > 0) {
    bulks++;
    const bulkStart = Date.now();

    let deletedThisBulk = 0;
    if (!DRY_RUN) {
      const res = target.bulkWrite(ops, { ordered: false });
      deletedThisBulk = res.deletedCount || 0;
      totalDeleted += deletedThisBulk;
    }

    const bulkEnd = Date.now();
    log(`BULK_END #${bulks}: ops=${ops.length} deleted=${deletedThisBulk} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
  }

  const endMs = Date.now();
  log(`TOTALS: readIds=${readIds} deleteOpsPrepared=${totalDeletesRequested} bulks=${bulks} totalDeleted=${totalDeleted} DRY_RUN=${DRY_RUN}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | SOURCE_COLL=${SOURCE_COLL} | TARGET_COLL=${TARGET_COLL}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}