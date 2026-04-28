//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_02_02_insert_encounter_without_status_modified.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo10\Mongo_Prod_EXP\099_02_02_insert_encounter_without_status_modified.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_02_02_insert_encounter_without_status_modified.js"

Spostare il file da Windows a Ubuntu Kubernetes:
/*
Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\099_02_02_insert_encounter_without_status_modified.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_02_02_insert_encounter_without_status_modified.js'
*/
/*
log:
[2026-04-23T13:09:13.818Z] START: SOURCE_COLL=encounter_without_status_modified | TARGET_COLL=encounter | MODE=upsert | BATCH=1000 | LOG_EVERY=1000
[2026-04-23T13:09:16.473Z] BULK_END #1: ops=19 inserted=0 upserted=19 modified=0 | elapsed=0h 0m 1s
[2026-04-23T13:09:37.918Z] TOTALS: readDocs=19 bulks=1 opsExecuted=19 inserted=0 upserted=19 modified=0 finalTargetCount=199772
[2026-04-23T13:09:37.919Z] END: elapsed=0h 0m 2s | TARGET_COLL=encounter | finalTargetCount=199772

*/

// Copia (restore) TUTTI i documenti dalla collezione sorgente encounter_without_status_modified
// dentro la collezione target encounter.
//
// Modalità consigliata: upsert (replaceOne + upsert=true) per rendere lo script ri-eseguibile.
// Logga progress ogni 1000 documenti processati.

const SOURCE_COLL = "encounter_without_status_modified";
const TARGET_COLL = "encounter";

const MODE = "upsert";     // "upsert" oppure "insert"
const BATCH = 1000;
const LOG_EVERY = 1000;

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
log(`START: SOURCE_COLL=${SOURCE_COLL} | TARGET_COLL=${TARGET_COLL} | MODE=${MODE} | BATCH=${BATCH} | LOG_EVERY=${LOG_EVERY}`);

try {
  if (!collectionExists(SOURCE_COLL)) throw new Error(`Sorgente non trovata: ${SOURCE_COLL}`);

  const source = db.getCollection(SOURCE_COLL);
  const target = db.getCollection(TARGET_COLL);

  const cursor = source.find({}).sort({ _id: 1 });

  let ops = [];
  let readDocs = 0;

  let bulks = 0;
  let totalOpsExecuted = 0;

  let totalInserted = 0;
  let totalUpserted = 0;
  let totalModified = 0;

  while (cursor.hasNext()) {
    const doc = cursor.next();
    readDocs++;

    if (MODE === "insert") {
      ops.push({ insertOne: { document: doc } });
    } else if (MODE === "upsert") {
      if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${SOURCE_COLL} (readDocs=${readDocs})`);
      ops.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    } else {
      throw new Error(`MODE non valido: ${MODE}`);
    }

    if (ops.length >= BATCH) {
      bulks++;
      const bulkStart = Date.now();
      const res = target.bulkWrite(ops, { ordered: false });

      const inserted = res.insertedCount || 0;
      const upserted = res.upsertedCount || 0;
      const modified = res.modifiedCount || 0;

      totalInserted += inserted;
      totalUpserted += upserted;
      totalModified += modified;
      totalOpsExecuted += ops.length;

      const bulkEnd = Date.now();
      log(`BULK_END #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);

      ops = [];
    }

    if (LOG_EVERY > 0 && readDocs % LOG_EVERY === 0) {
      const t = Date.now();
      log(`PROGRESS: readDocs=${readDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} | elapsed=${fmtDuration(t - startMs)}`);
    }
  }

  if (ops.length > 0) {
    bulks++;
    const bulkStart = Date.now();
    const res = target.bulkWrite(ops, { ordered: false });

    const inserted = res.insertedCount || 0;
    const upserted = res.upsertedCount || 0;
    const modified = res.modifiedCount || 0;

    totalInserted += inserted;
    totalUpserted += upserted;
    totalModified += modified;
    totalOpsExecuted += ops.length;

    const bulkEnd = Date.now();
    log(`BULK_END #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
  }

  const endMs = Date.now();
  const finalCount = target.countDocuments();

  log(`TOTALS: readDocs=${readDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} finalTargetCount=${finalCount}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | TARGET_COLL=${TARGET_COLL} | finalTargetCount=${finalCount}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}