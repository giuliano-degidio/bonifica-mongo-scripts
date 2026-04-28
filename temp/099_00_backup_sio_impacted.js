//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_00_backup_sio_impacted.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_00_backup_sio_impacted.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_00_backup_sio_impacted.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR\099_00_backup_sio_impacted.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_00_backup_sio_impacted.js'


 OUTPUT:
[2026-04-15T09:29:43.394Z] START: IDS_PATH=/data/Mongo_Sh_Script/EXP/sio_modified_id.js | IDS_PATH_HC40_ADT=/data/Mongo_Sh_Script/EXP/sio_modified_id_hc40_adt.js | SOURCE_COLL=encounter | BACKUP_COLL=bonifica_encounter_deleted_bck | DROP_BEFORE=true | BATCH_IDS=1000 | BULK_WRITE=1000 | LOG_EVERY=1000
[2026-04-15T09:29:43.577Z] IDS_LOADED: SIO_MODIFIED_ID=20 | SIO_MODIFIED_ID_HC40_ADT=16
[2026-04-15T09:29:43.577Z] IDS_MERGED: merged=36 | unique=36
[2026-04-15T09:29:43.588Z] DROP_SKIP: bonifica_encounter_deleted_bck does not exist | elapsed=0h 0m 0s
[2026-04-15T09:29:43.589Z] ID_CHUNKS: chunks=1 | chunkSize=1000
[2026-04-15T09:29:43.946Z] BULK_END #1: ops=36 upserted=36 modified=0 inserted=0 | elapsed=0h 0m 0s
[2026-04-15T09:29:43.949Z] TOTALS: ids_unique=36 copiedDocs=36 missingIds_estimate=0 bulks=1 backupCount=36
[2026-04-15T09:29:43.949Z] END: elapsed=0h 0m 0s | BACKUP_COLL=bonifica_encounter_deleted_bck | backupCount=36

*/

// Cosa fa:
// 1) Carica:
//    - C:\Appo09\Mongo_Prod_EXP\EXP\sio_modified_id.js              (deve definire const SIO_MODIFIED_ID = [...])
//    - C:\Appo09\Mongo_Prod_EXP\EXP\sio_modified_id_hc40_adt.js     (deve definire const SIO_MODIFIED_ID_HC40_ADT = [...])
//    Unisce le due liste senza duplicati in un'unica lista IDS_UNIQUE.
// 2) Droppa e ricrea la collezione di backup bonifica_encounter_deleted_bck.
// 3) Legge da encounter tutti i documenti con _id IN IDS_UNIQUE e li copia nel backup.
// 4) Logga il progresso ogni 1000 documenti copiati.

const fs = require("fs");

// ====== INPUT (FILE) ======
//const IDS_PATH = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\sio_modified_id.js"; // const SIO_MODIFIED_ID = ["...", ...]
const IDS_PATH = "/data/Mongo_Sh_Script/EXP/sio_modified_id.js"; // const SIO_MODIFIED_ID = ["...", ...]
//const IDS_PATH_HC40_ADT = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\sio_modified_id_hc40_adt.js"; // const SIO_MODIFIED_ID_HC40_ADT = ["...", ...]
const IDS_PATH_HC40_ADT = "/data/Mongo_Sh_Script/EXP/sio_modified_id_hc40_adt.js"; // const SIO_MODIFIED_ID_HC40_ADT = ["...", ...]

// ====== COLLECTIONS ======
const SOURCE_COLL = "encounter";
const BACKUP_COLL = "bonifica_encounter_deleted_bck";

// ====== PARAMETRI ======
const DROP_BEFORE = true;   // ricrea ogni volta
const BATCH_IDS = 1000;     // chunk dell'array id per query $in
const BULK_WRITE = 1000;    // batch bulkWrite
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

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function assertArrayOfStrings(name, arr) {
  if (!Array.isArray(arr) || arr.length === 0) {
    throw new Error(`${name} non è un array valido o è vuoto.`);
  }
  // Non forzo "tutti string" per non essere troppo rigido; filtro dopo.
}

const startMs = Date.now();
log(
  `START: IDS_PATH=${IDS_PATH} | IDS_PATH_HC40_ADT=${IDS_PATH_HC40_ADT} | SOURCE_COLL=${SOURCE_COLL} | BACKUP_COLL=${BACKUP_COLL} | DROP_BEFORE=${DROP_BEFORE} | BATCH_IDS=${BATCH_IDS} | BULK_WRITE=${BULK_WRITE} | LOG_EVERY=${LOG_EVERY}`
);

try {
  // 1) load ids
  if (!fs.existsSync(IDS_PATH)) throw new Error(`File non trovato: ${IDS_PATH}`);
  if (!fs.existsSync(IDS_PATH_HC40_ADT)) throw new Error(`File non trovato: ${IDS_PATH_HC40_ADT}`);

  load(IDS_PATH);           // defines SIO_MODIFIED_ID
  load(IDS_PATH_HC40_ADT);  // defines SIO_MODIFIED_ID_HC40_ADT

  assertArrayOfStrings("SIO_MODIFIED_ID", SIO_MODIFIED_ID);
  assertArrayOfStrings("SIO_MODIFIED_ID_HC40_ADT", SIO_MODIFIED_ID_HC40_ADT);

  const idsMerged = []
    .concat(SIO_MODIFIED_ID)
    .concat(SIO_MODIFIED_ID_HC40_ADT)
    .filter((x) => typeof x === "string" && x.length > 0);

  const idsUnique = Array.from(new Set(idsMerged));

  log(`IDS_LOADED: SIO_MODIFIED_ID=${SIO_MODIFIED_ID.length} | SIO_MODIFIED_ID_HC40_ADT=${SIO_MODIFIED_ID_HC40_ADT.length}`);
  log(`IDS_MERGED: merged=${idsMerged.length} | unique=${idsUnique.length}`);

  const source = db.getCollection(SOURCE_COLL);
  const backup = db.getCollection(BACKUP_COLL);

  // 2) drop + recreate backup
  if (DROP_BEFORE) {
    const dropStart = Date.now();
    if (collectionExists(BACKUP_COLL)) {
      log(`DROP_START: Dropping ${BACKUP_COLL} ...`);
      backup.drop();
      log(`DROP_END: Dropped ${BACKUP_COLL} | elapsed=${fmtDuration(Date.now() - dropStart)}`);
    } else {
      log(`DROP_SKIP: ${BACKUP_COLL} does not exist | elapsed=${fmtDuration(Date.now() - dropStart)}`);
    }
  }

  // 3) copy documents
  const idChunks = chunkArray(idsUnique, BATCH_IDS);
  log(`ID_CHUNKS: chunks=${idChunks.length} | chunkSize=${BATCH_IDS}`);

  let copiedDocs = 0;          // numero documenti letti da encounter e scritti nel backup
  let missingIds = 0;          // id presenti in idsUnique ma non trovati in encounter (stima per chunk)
  let bulkOps = [];
  let bulks = 0;

  for (let ci = 0; ci < idChunks.length; ci++) {
    const ids = idChunks[ci];

    const docs = source.find({ _id: { $in: ids } }).toArray();

    // stima "missing" per chunk (se _id univoci)
    if (docs.length < ids.length) missingIds += (ids.length - docs.length);

    for (const doc of docs) {
      bulkOps.push({
        replaceOne: {
          filter: { _id: doc._id },
          replacement: doc,
          upsert: true,
        },
      });

      copiedDocs++;

      if (LOG_EVERY > 0 && copiedDocs % LOG_EVERY === 0) {
        const t = Date.now();
        log(`PROGRESS: copiedDocs=${copiedDocs} bulks=${bulks} currentChunk=${ci + 1}/${idChunks.length} | elapsed=${fmtDuration(t - startMs)}`);
      }

      if (bulkOps.length >= BULK_WRITE) {
        bulks++;
        const bulkStart = Date.now();
        const res = backup.bulkWrite(bulkOps, { ordered: false });
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
    const res = backup.bulkWrite(bulkOps, { ordered: false });
    const bulkEnd = Date.now();
    log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
    bulkOps = [];
  }

  const endMs = Date.now();
  const finalCount = backup.countDocuments();

  log(`TOTALS: ids_unique=${idsUnique.length} copiedDocs=${copiedDocs} missingIds_estimate=${missingIds} bulks=${bulks} backupCount=${finalCount}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | BACKUP_COLL=${BACKUP_COLL} | backupCount=${finalCount}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}