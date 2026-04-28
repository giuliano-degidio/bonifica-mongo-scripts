//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_01_delete_sio_impacted.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_01_delete_sio_impacted.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_01_delete_sio_impacted.js"

  spostare il file da windows a ubuntu kubernate:
 Get-Content -Raw "C:\Users\giuldegi\OneDrive - Engineering Ingegneria Informatica S.p.A\Desktop\ENG\SANITA\Romagna\CDR-Mongo\Svil\BONIFICA_CDR\099_01_delete_sio_impacted.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_01_delete_sio_impacted.js'


 OUTPUT:
[2026-04-15T11:44:05.883Z] START: IDS_PATH=/data/Mongo_Sh_Script/EXP/sio_modified_id.js | IDS_PATH_HC40_ADT=/data/Mongo_Sh_Script/EXP/sio_modified_id_hc40_adt.js | TARGET_COLL=encounter | BATCH_IDS=1000 | LOG_EVERY_DELETES=1000 | REQUIRE_CONFIRMATION=false
[2026-04-15T11:44:06.001Z] IDS_LOADED: SIO_MODIFIED_ID=20 | SIO_MODIFIED_ID_HC40_ADT=16
[2026-04-15T11:44:06.002Z] IDS_MERGED: merged=36 | unique=36
[2026-04-15T11:44:06.003Z] ID_CHUNKS: chunks=1 | chunkSize=1000
[2026-04-15T11:44:16.570Z] CHUNK_END #1/1: ids=36 deleted=36 | elapsed=0h 0m 10s | totalDeleted=36
[2026-04-15T11:44:16.571Z] TOTALS: ids_unique=36 chunks=1 totalDeleted=36
[2026-04-15T11:44:16.571Z] END: elapsed=0h 0m 10s | TARGET_COLL=encounter
 

*/
// Cosa fa:
// 1) Carica:
//    - C:\Appo09\Mongo_Prod_EXP\EXP\sio_modified_id.js              (const SIO_MODIFIED_ID = [...])
//    - C:\Appo09\Mongo_Prod_EXP\EXP\sio_modified_id_hc40_adt.js     (const SIO_MODIFIED_ID_HC40_ADT = [...])
//    Unisce le due liste senza duplicati in un'unica lista IDS_UNIQUE.
// 2) WARNING BLOCCANTE: chiede conferma (digitare esattamente 'y') se REQUIRE_CONFIRMATION=true.
// 3) Cancella da encounter tutti i documenti con _id IN IDS_UNIQUE.
// 4) Logga progresso ogni 1000 documenti cancellati.

const fs = require("fs");

// ====== INPUT (FILE) ======
//const IDS_PATH = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\sio_modified_id.js"; // const SIO_MODIFIED_ID = ["...", ...]
const IDS_PATH = "/data/Mongo_Sh_Script/EXP/sio_modified_id.js"; // const SIO_MODIFIED_ID = ["...", ...]
//const IDS_PATH_HC40_ADT = "C:\\Appo09\\Mongo_Prod_EXP\\EXP\\sio_modified_id_hc40_adt.js"; // const SIO_MODIFIED_ID_HC40_ADT = ["...", ...]
const IDS_PATH_HC40_ADT = "/data/Mongo_Sh_Script/EXP/sio_modified_id_hc40_adt.js"; // const SIO_MODIFIED_ID_HC40_ADT = ["...", ...]


// ====== COLLECTION ======
const TARGET_COLL = "encounter";

// ====== PARAMETRI ======
const BATCH_IDS = 1000;           // chunk dell'array id per deleteMany $in
const LOG_EVERY_DELETES = 1000;   // progress
const REQUIRE_CONFIRMATION = false;

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

function askYesNoBlocking(question) {
  // mongosh supporta readlineSync (sincrono) per input utente
  if (typeof readlineSync !== "function") {
    throw new Error("readlineSync non disponibile in questo ambiente mongosh: impossibile chiedere conferma interattiva.");
  }
  return readlineSync(question);
}

function assertArray(name, arr) {
  if (!Array.isArray(arr) || arr.length === 0) {
    throw new Error(`${name} non è un array valido o è vuoto.`);
  }
}

const startMs = Date.now();
log(`START: IDS_PATH=${IDS_PATH} | IDS_PATH_HC40_ADT=${IDS_PATH_HC40_ADT} | TARGET_COLL=${TARGET_COLL} | BATCH_IDS=${BATCH_IDS} | LOG_EVERY_DELETES=${LOG_EVERY_DELETES} | REQUIRE_CONFIRMATION=${REQUIRE_CONFIRMATION}`);

try {
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

  // 2) warning bloccante
  if (REQUIRE_CONFIRMATION) {
    log("WARNING: Questa operazione CANCELLERA' DEFINITIVAMENTE documenti dalla collezione encounter.");
    log(`WARNING: Target collection = ${TARGET_COLL}`);
    log(`WARNING: Numero _id in input (unique) = ${idsUnique.length}`);
    log("WARNING: Per procedere digitare esattamente: y  (qualsiasi altra risposta annulla)");

    const ans = askYesNoBlocking("CONFERMI LA CANCELLAZIONE DEFINITIVA? (y/N): ");
    if (String(ans).trim() !== "y") {
      log(`ABORTED: risposta='${ans}' (atteso 'y')`);
      quit(2);
    }
    log("CONFIRMED: procedo con la cancellazione.");
  }

  const target = db.getCollection(TARGET_COLL);

  const idChunks = chunkArray(idsUnique, BATCH_IDS);
  log(`ID_CHUNKS: chunks=${idChunks.length} | chunkSize=${BATCH_IDS}`);

  let totalDeleted = 0;
  let chunksDone = 0;

  for (let ci = 0; ci < idChunks.length; ci++) {
    const ids = idChunks[ci];

    const delStart = Date.now();
    const res = target.deleteMany({ _id: { $in: ids } });
    const delEnd = Date.now();

    const deleted = (res && typeof res.deletedCount === "number") ? res.deletedCount : 0;
    totalDeleted += deleted;
    chunksDone++;

    log(`CHUNK_END #${ci + 1}/${idChunks.length}: ids=${ids.length} deleted=${deleted} | elapsed=${fmtDuration(delEnd - delStart)} | totalDeleted=${totalDeleted}`);

    // progress ogni 1000 cancellati (best effort)
    if (LOG_EVERY_DELETES > 0 && totalDeleted > 0 && (totalDeleted % LOG_EVERY_DELETES) < deleted) {
      const t = Date.now();
      log(`PROGRESS: chunksDone=${chunksDone}/${idChunks.length} | totalDeleted=${totalDeleted} | elapsed=${fmtDuration(t - startMs)}`);
    }
  }

  const endMs = Date.now();
  log(`TOTALS: ids_unique=${idsUnique.length} chunks=${idChunks.length} totalDeleted=${totalDeleted}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | TARGET_COLL=${TARGET_COLL}`);
} catch (e) {
  const errMs = Date.now();
  log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
}