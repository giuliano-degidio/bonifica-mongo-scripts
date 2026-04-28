//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_02_01B_insert_encounter_history_from_without_status_original.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo10\Mongo_Prod_EXP\099_02_01B_insert_encounter_history_from_without_status_original.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_02_01B_insert_encounter_history_from_without_status_original.js"

Spostare il file da Windows a Ubuntu Kubernetes:
/*
Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\099_02_01B_insert_encounter_history_from_without_status_original.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_02_01B_insert_encounter_history_from_without_status_original.js'
*/
/*
log:
[2026-04-23T13:07:41.022Z] START: SOURCE_COLL=encounter_without_status_original | HISTORY_COLL=encounter_history | BULK_WRITE=1000 | LOG_EVERY=1000 | runSuffix=1776949661022 | EXPORT_KEYS=true
[2026-04-23T13:07:43.817Z] BULK_END #1: ops=19 upserted=19 modified=0 inserted=0 | elapsed=0h 0m 1s
[2026-04-23T13:07:47.942Z] TOTALS: readDocs=19 writtenDocs=19 bulks=1 historyCount=29384 exportedIds=19
[2026-04-23T13:07:47.943Z] EXPORT_FILE: C:\Appo10\Mongo_Prod_EXP\EXP/encounter_history_without_status_inserted_id.js
[2026-04-23T13:07:47.943Z] END: elapsed=0h 0m 2s | HISTORY_COLL=encounter_history
*/

// SCOPO
// -----
// Legge tutti i documenti dalla collezione `encounter_without_status_original`
// e li copia nella collezione `encounter_history`, cambiando l'_id in:
//   "<OLD_ID>/_history/<NOW_MS>"
// dove NOW_MS è un suffisso (ms) calcolato lato client (Date.now()).
// e aggiungendo il campo request: "BONIFICA" sui documenti in encounter_history.
//
// Opzionale: esporta su file .js l'elenco degli _id inseriti in history per eventuale rollback.
// Nome file richiesto: encounter_history_without_status_inserted_id.js

const fs = require("fs");

const SOURCE_COLL = "encounter_without_status_original";
const HISTORY_COLL = "encounter_history";

const BULK_WRITE = 1000;
const LOG_EVERY = 1000;

const HISTORY_SUFFIX = null; // se null => Date.now()

const EXPORT_KEYS = true;
//const EXPORT_DIR = "/data/Mongo_Sh_Script/EXP";
const EXPORT_DIR = "C:\\Appo10\\Mongo_Prod_EXP\\EXP";
const EXPORT_CONST_NAME = "ENCOUNTER_HISTORY_WITHOUT_STATUS_INSERTED_ID";
const EXPORT_FILE = `${EXPORT_DIR}/encounter_history_without_status_inserted_id.js`;

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

function makeHistoryId(oldId, msSuffix) {
  return `${oldId}/_history/${msSuffix}`;
}

const startMs = Date.now();
const runSuffix = (HISTORY_SUFFIX && String(HISTORY_SUFFIX).trim()) ? String(HISTORY_SUFFIX).trim() : String(Date.now());

log(`START: SOURCE_COLL=${SOURCE_COLL} | HISTORY_COLL=${HISTORY_COLL} | BULK_WRITE=${BULK_WRITE} | LOG_EVERY=${LOG_EVERY} | runSuffix=${runSuffix} | EXPORT_KEYS=${EXPORT_KEYS}`);

let __exportStream = null;
let __exportedIds = 0;

try {
  if (!collectionExists(SOURCE_COLL)) throw new Error(`Source collection not found: ${SOURCE_COLL}`);
  if (!collectionExists(HISTORY_COLL)) log(`WARNING: history collection ${HISTORY_COLL} not found (will be created on first insert)`);

  if (EXPORT_KEYS) {
    if (!fs.existsSync(EXPORT_DIR)) fs.mkdirSync(EXPORT_DIR, { recursive: true });
    __exportStream = fs.createWriteStream(EXPORT_FILE, { encoding: "utf8", flags: "w" });

    __exportStream.write(`// AUTO-GENERATED FILE\n`);
    __exportStream.write(`// generated_at=${now()}\n`);
    __exportStream.write(`// runSuffix=${runSuffix}\n`);
    __exportStream.write(`// SOURCE_COLL=${SOURCE_COLL} HISTORY_COLL=${HISTORY_COLL}\n\n`);
    __exportStream.write(`const ${EXPORT_CONST_NAME} = [\n`);
  }

  const source = db.getCollection(SOURCE_COLL);
  const history = db.getCollection(HISTORY_COLL);

  const cursor = source.find({}).sort({ _id: 1 });

  let bulkOps = [];
  let bulks = 0;

  let readDocs = 0;
  let writtenDocs = 0;

  while (cursor.hasNext()) {
    const doc = cursor.next();
    readDocs++;

    if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
      throw new Error(`Documento non valido letto da ${SOURCE_COLL} (readDocs=${readDocs})`);
    }
    if (doc._id === undefined) {
      throw new Error(`Documento senza _id letto da ${SOURCE_COLL} (readDocs=${readDocs})`);
    }

    const oldId = doc._id;

    const newDoc = Object.assign({}, doc);
    newDoc._id = makeHistoryId(oldId, runSuffix);
    newDoc.request = "BONIFICA";

    if (__exportStream) {
      __exportStream.write(`  ${JSON.stringify(String(newDoc._id))},\n`);
      __exportedIds++;
    }

    bulkOps.push({
      replaceOne: {
        filter: { _id: newDoc._id },
        replacement: newDoc,
        upsert: true,
      },
    });

    writtenDocs++;

    if (LOG_EVERY > 0 && writtenDocs % LOG_EVERY === 0) {
      const t = Date.now();
      log(`PROGRESS: readDocs=${readDocs} writtenDocs=${writtenDocs} bulks=${bulks} exportedIds=${__exportedIds} | elapsed=${fmtDuration(t - startMs)}`);
    }

    if (bulkOps.length >= BULK_WRITE) {
      bulks++;
      const bulkStart = Date.now();
      const res = history.bulkWrite(bulkOps, { ordered: false });
      const bulkEnd = Date.now();
      log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
      bulkOps = [];
    }
  }

  if (bulkOps.length > 0) {
    bulks++;
    const bulkStart = Date.now();
    const res = history.bulkWrite(bulkOps, { ordered: false });
    const bulkEnd = Date.now();
    log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
    bulkOps = [];
  }

  const endMs = Date.now();
  const historyCount = history.countDocuments();

  log(`TOTALS: readDocs=${readDocs} writtenDocs=${writtenDocs} bulks=${bulks} historyCount=${historyCount} exportedIds=${__exportedIds}`);
  if (__exportStream) log(`EXPORT_FILE: ${EXPORT_FILE}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | HISTORY_COLL=${HISTORY_COLL}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
} finally {
  try { if (__exportStream) __exportStream.write("];\n"); } catch (_) {}
  try { if (__exportStream) __exportStream.end(); } catch (_) {}
}