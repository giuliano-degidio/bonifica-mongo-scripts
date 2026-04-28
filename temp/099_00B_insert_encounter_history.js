//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_00B_insert_encounter_history.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_00B_insert_encounter_history.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_00B_insert_encounter_history.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR\099_00B_insert_encounter_history.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_00B_insert_encounter_history.js'


 OUTPUT:
[2026-04-15T09:34:25.493Z] START: IDS_PATH=/data/Mongo_Sh_Script/EXP/sio_modified_id.js | IDS_PATH_HC40_ADT=/data/Mongo_Sh_Script/EXP/sio_modified_id_hc40_adt.js | SOURCE_COLL=encounter | HISTORY_COLL=encounter_history | BATCH_IDS=1000 | BULK_WRITE=1000 | LOG_EVERY=1000 | runSuffix=1776245665493
[2026-04-15T09:34:25.607Z] IDS_LOADED: SIO_MODIFIED_ID=20 | SIO_MODIFIED_ID_HC40_ADT=16
[2026-04-15T09:34:25.607Z] IDS_MERGED: merged=36 | unique=36
[2026-04-15T09:34:25.609Z] ID_CHUNKS: chunks=1 | chunkSize=1000
[2026-04-15T09:34:25.911Z] BULK_END #1: ops=36 upserted=36 modified=0 inserted=0 | elapsed=0h 0m 0s
[2026-04-15T09:34:27.452Z] TOTALS: ids_unique=36 copiedDocs=36 missingIds_estimate=0 bulks=1 historyCount=29364 exportedIds=36
[2026-04-15T09:34:27.452Z] EXPORT_FILE: C:\Appo09\Mongo_Prod_EXP\EXP\encounter_history_inserted_id_2026-04-15T09-34-25-493Z.js
[2026-04-15T09:34:27.453Z] END: elapsed=0h 0m 0s | HISTORY_COLL=encounter_history | historyCount=29364

[2026-04-15T10:13:04.190Z] IDS_LOADED: SIO_MODIFIED_ID=20 | SIO_MODIFIED_ID_HC40_ADT=16
[2026-04-15T10:13:04.191Z] IDS_MERGED: merged=36 | unique=36
[2026-04-15T10:13:04.192Z] ID_CHUNKS: chunks=1 | chunkSize=1000
[2026-04-15T10:13:04.413Z] BULK_END #1: ops=36 upserted=36 modified=0 inserted=0 | elapsed=0h 0m 0s
[2026-04-15T10:13:04.941Z] TOTALS: ids_unique=36 copiedDocs=36 missingIds_estimate=0 bulks=1 historyCount=29400 exportedIds=36
[2026-04-15T10:13:04.941Z] EXPORT_FILE: /data/Mongo_Sh_Script/EXP/encounter_history_inserted_id_2026-04-15T10-13-04-006Z.js
[2026-04-15T10:13:04.942Z] END: elapsed=0h 0m 0s | HISTORY_COLL=encounter_history | historyCount=29400



*/
// Cosa fa:
// 1) Carica:
//    - C:\Appo09\Mongo_Prod_EXP\EXP\sio_modified_id.js              (const SIO_MODIFIED_ID = [...])
//    - C:\Appo09\Mongo_Prod_EXP\EXP\sio_modified_id_hc40_adt.js     (const SIO_MODIFIED_ID_HC40_ADT = [...])
//    Unisce le due liste senza duplicati in un'unica lista IDS_UNIQUE.
// 3) Legge da encounter tutti i documenti con _id IN IDS_UNIQUE e li copia nella collezione encounter_history
//    cambiando l'_id in "<OLD_ID>/_history/<NOW_MS>"
//    e aggiungendo il campo request: "BONIFICA" sui documenti in encounter_history
// 4) Logga il progresso ogni 1000 documenti copiati.
// 5) esporta le chiavi inserite in encounter_history (per eventuale rollback)
// NOTE:
// - Questo script fa UPSERT su encounter_history usando il NUOVO _id (quindi è ri-eseguibile senza duplicare
// - Non rilanciare per evitare nuove versioni a ogni rilancio

const fs = require("fs");

// ====== INPUT (FILE) ======
//const IDS_PATH = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\sio_modified_id.js"; // const SIO_MODIFIED_ID = ["...", ...]
const IDS_PATH = "/data/Mongo_Sh_Script/EXP/sio_modified_id.js"; // const SIO_MODIFIED_ID = ["...", ...]
//const IDS_PATH_HC40_ADT = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\sio_modified_id_hc40_adt.js"; // const SIO_MODIFIED_ID_HC40_ADT = ["...", ...]
const IDS_PATH_HC40_ADT = "/data/Mongo_Sh_Script/EXP/sio_modified_id_hc40_adt.js"; // const SIO_MODIFIED_ID_HC40_ADT = ["...", ...]



// ====== COLLECTIONS ======
const SOURCE_COLL = "encounter";
const HISTORY_COLL = "encounter_history";

// ====== PARAMETRI ======
const BATCH_IDS = 1000;     // chunk dell'array id per query $in
const BULK_WRITE = 1000;    // batch bulkWrite
const LOG_EVERY = 1000;

// Se si vuole determinare/forzare il suffisso (es. per rerun deterministico), puoi valorizzare:
// const HISTORY_SUFFIX = "1710000000000";  // stringa numerica
// Se vuoto/null => usa Date.now() del client (ms)
const HISTORY_SUFFIX = null;

// ====== EXPORT CHIAVI INSERITE IN HISTORY (per rollback) ======
const EXPORT_KEYS = true;
const EXPORT_DIR = "/data/Mongo_Sh_Script/EXP";
const EXPORT_CONST_NAME = "ENCOUNTER_HISTORY_INSERTED_ID";
const EXPORT_FILE = `${EXPORT_DIR}/encounter_history_inserted_id_${new Date().toISOString().replace(/[:.]/g, "-")}.js`;

function now() { return new Date().toISOString(); }
function log(msg) { print(`[${now()}] ${msg}`); }
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function assertArray(name, arr) {
  if (!Array.isArray(arr) || arr.length === 0) {
    throw new Error(`${name} non è un array valido o è vuoto.`);
  }
}

// Genera il nuovo _id stile FHIR history: "<old>/_history/<ms>"
function makeHistoryId(oldId, msSuffix) {
  return `${oldId}/_history/${msSuffix}`;
}

const startMs = Date.now();
const runSuffix = (HISTORY_SUFFIX && String(HISTORY_SUFFIX).trim()) ? String(HISTORY_SUFFIX).trim() : String(Date.now());

log(`START: IDS_PATH=${IDS_PATH} | IDS_PATH_HC40_ADT=${IDS_PATH_HC40_ADT} | SOURCE_COLL=${SOURCE_COLL} | HISTORY_COLL=${HISTORY_COLL} | BATCH_IDS=${BATCH_IDS} | BULK_WRITE=${BULK_WRITE} | LOG_EVERY=${LOG_EVERY} | runSuffix=${runSuffix}`);

// ====== export stream ======
let __exportStream = null;
let __exportedIds = 0;

try {
  if (EXPORT_KEYS) {
    if (!fs.existsSync(EXPORT_DIR)) fs.mkdirSync(EXPORT_DIR, { recursive: true });
    __exportStream = fs.createWriteStream(EXPORT_FILE, { encoding: "utf8", flags: "w" });

    __exportStream.write(`// AUTO-GENERATED FILE\n`);
    __exportStream.write(`// generated_at=${now()}\n`);
    __exportStream.write(`// runSuffix=${runSuffix}\n`);
    __exportStream.write(`// SOURCE_COLL=${SOURCE_COLL} HISTORY_COLL=${HISTORY_COLL}\n\n`);
    __exportStream.write(`const ${EXPORT_CONST_NAME} = [\n`);
  }

  // 1) load ids (entrambi i file)
  if (!fs.existsSync(IDS_PATH)) throw new Error(`File non trovato: ${IDS_PATH}`);
  if (!fs.existsSync(IDS_PATH_HC40_ADT)) throw new Error(`File non trovato: ${IDS_PATH_HC40_ADT}`);

  load(IDS_PATH);          // defines SIO_MODIFIED_ID
  load(IDS_PATH_HC40_ADT); // defines SIO_MODIFIED_ID_HC40_ADT

  assertArray("SIO_MODIFIED_ID", SIO_MODIFIED_ID);
  assertArray("SIO_MODIFIED_ID_HC40_ADT", SIO_MODIFIED_ID_HC40_ADT);

  const idsMerged = []
    .concat(SIO_MODIFIED_ID)
    .concat(SIO_MODIFIED_ID_HC40_ADT)
    .filter((x) => typeof x === "string" && x.length > 0);

  const idsUnique = Array.from(new Set(idsMerged));

  log(`IDS_LOADED: SIO_MODIFIED_ID=${SIO_MODIFIED_ID.length} | SIO_MODIFIED_ID_HC40_ADT=${SIO_MODIFIED_ID_HC40_ADT.length}`);
  log(`IDS_MERGED: merged=${idsMerged.length} | unique=${idsUnique.length}`);

  const source = db.getCollection(SOURCE_COLL);
  const history = db.getCollection(HISTORY_COLL);

  // 3) copy documents con cambio _id
  const idChunks = chunkArray(idsUnique, BATCH_IDS);
  log(`ID_CHUNKS: chunks=${idChunks.length} | chunkSize=${BATCH_IDS}`);

  let copiedDocs = 0;
  let missingIds = 0;
  let bulkOps = [];
  let bulks = 0;

  for (let ci = 0; ci < idChunks.length; ci++) {
    const ids = idChunks[ci];

    const docs = source.find({ _id: { $in: ids } }).toArray();

    // stima "missing" per chunk (se _id univoci)
    if (docs.length < ids.length) missingIds += (ids.length - docs.length);

    for (const doc of docs) {
      const oldId = doc._id;

      // CLONE + cambio _id
      const newDoc = Object.assign({}, doc);
      newDoc._id = makeHistoryId(oldId, runSuffix);
	  // aggiunta campo per tracciamento bonifica
      newDoc.request = "BONIFICA";

      // export id su file
      if (__exportStream) {
        __exportStream.write(`  ${JSON.stringify(String(newDoc._id))},\n`);
        __exportedIds++;
      }

      bulkOps.push({
        replaceOne: {
          filter: { _id: newDoc._id },      // UPSERT per nuovo id history
          replacement: newDoc,
          upsert: true,
        },
      });

      copiedDocs++;

      if (LOG_EVERY > 0 && copiedDocs % LOG_EVERY === 0) {
        const t = Date.now();
        log(`PROGRESS: copiedDocs=${copiedDocs} bulks=${bulks} currentChunk=${ci + 1}/${idChunks.length} | exportedIds=${__exportedIds} | elapsed=${fmtDuration(t - startMs)}`);
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
  }

  // flush finale
  if (bulkOps.length > 0) {
    bulks++;
    const bulkStart = Date.now();
    const res = history.bulkWrite(bulkOps, { ordered: false });
    const bulkEnd = Date.now();
    log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
    bulkOps = [];
  }

  const endMs = Date.now();
  const finalCount = history.countDocuments();

  log(`TOTALS: ids_unique=${idsUnique.length} copiedDocs=${copiedDocs} missingIds_estimate=${missingIds} bulks=${bulks} historyCount=${finalCount} exportedIds=${__exportedIds}`);
  if (__exportStream) log(`EXPORT_FILE: ${EXPORT_FILE}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | HISTORY_COLL=${HISTORY_COLL} | historyCount=${finalCount}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
} finally {
  try {
    if (__exportStream) {
      __exportStream.write("];\n");
    }
  } catch (_) {}

  try {
    if (__exportStream) __exportStream.end();
  } catch (_) {}
}