//Lanciare da mongosh
/* SVIL
mongosh "mongodb://giuldegi:bitbros@192.168.248.135:27017/romagna?authSource=admin" ^
--quiet ^
--file "C:\Appo09\Mongo_Prod_EXP\099_01_04B_importazione_encounter_ihub_hc40_adt_without_status_original.js"
*/
/* PRODUZIONE/TEST
mongosh "mongodb://root:password@localhost:47017/hc40-index-bck?authSource=admin&directConnection=true&readPreference=primaryPreferred" ^
--quiet ^
--file "C:\Appo10\Mongo_Prod_EXP\099_01_04B_importazione_encounter_ihub_hc40_adt_without_status_original.js"
*/
// COMANDO DI ESECUZIONE DA MONGOSH TEST UBUNTU
/*

/data/mongosh/bin/mongosh "mongodb://root:password@mongo-rs-1.mongo-rs-svc.mongodb.svc.cluster.local:27017/hc40-index-bonifica?authSource=admin&directConnection=true&readPreference=primary" \
  --quiet \
  --file "/data/Mongo_Sh_Script/099_01_04B_importazione_encounter_ihub_hc40_adt_without_status_original.js"

Spostare il file da Windows a Ubuntu Kubernetes:
/*
Get-Content -Raw "C:\Appo10\Mongo_Prod_EXP\099_01_04B_importazione_encounter_ihub_hc40_adt_without_status_original.js" |
kubectl -n ellipse-index exec -i ubuntu-mongosync-6845564564-zvnn9 -- sh -c 'cat > /data/Mongo_Sh_Script/099_01_04B_importazione_encounter_ihub_hc40_adt_without_status_original.js'
*/

/*
log:
[2026-04-23T12:46:47.674Z] START: IN_DIR=C:\Appo10\Mongo_Prod_EXP\EXP | prefix=encounter_without_status_original_chunk | suffix=.jsonl | MAX_FILES=0 | COLL=encounter_without_status_original | MODE=insert | BATCH=1000 | DROP_BEFORE=true
[2026-04-23T12:46:47.678Z] FILES_FOUND: 1
[2026-04-23T12:46:47.683Z]   - C:\Appo10\Mongo_Prod_EXP\EXP\encounter_without_status_original_chunk0001.jsonl
[2026-04-23T12:46:47.891Z] STEP_DROP_SKIP: encounter_without_status_original does not exist | elapsed=0h 0m 0s
[2026-04-23T12:46:47.892Z] FILE_START [1/1]: C:\Appo10\Mongo_Prod_EXP\EXP\encounter_without_status_original_chunk0001.jsonl
[2026-04-23T12:46:49.694Z] BULK_END file[1/1] #1: ops=19 inserted=19 upserted=0 modified=0 | elapsed=0h 0m 1s
[2026-04-23T12:46:49.694Z] FILE_END [1/1]: lines=19 emptyLines=0 parsedDocs=19 bulks=1 opsExecuted=19 inserted=19 upserted=0 modified=0 | elapsed=0h 0m 1s
[2026-04-23T12:46:49.695Z] STEP_IMPORT_ALL_END: elapsed=0h 0m 1s
[2026-04-23T12:46:49.752Z] TOTALS: files=1 lines=19 emptyLines=0 parsedDocs=19 bulks=1 opsExecuted=19 inserted=19 upserted=0 modified=0 finalCollCount=19
[2026-04-23T12:46:49.753Z] END: elapsed=0h 0m 2s | COLL=encounter_without_status_original | finalCollCount=19
*/

// Importa i file JSONL ORIGINALI prodotti dallo step 099_01_03B
// nella collezione encounter_without_status_original.

const fs = require("fs");
const readline = require("readline");
const path = require("path");

//const IN_DIR = "/data/Mongo_Sh_Script/EXP";
const IN_DIR = "C:\\Appo10\\Mongo_Prod_EXP\\EXP";
const FILE_PREFIX = "encounter_without_status_original_chunk";
const FILE_SUFFIX = ".jsonl";

const MAX_FILES = 0;

const COLL = "encounter_without_status_original";

const MODE = "insert"; // "upsert" consigliato se vuoi ri-eseguibilità senza duplicati
const BATCH = 1000;
const DROP_BEFORE = true;

const LOG_EVERY_LINES = 100000;

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
log(`START: IN_DIR=${IN_DIR} | prefix=${FILE_PREFIX} | suffix=${FILE_SUFFIX} | MAX_FILES=${MAX_FILES} | COLL=${COLL} | MODE=${MODE} | BATCH=${BATCH} | DROP_BEFORE=${DROP_BEFORE}`);

function collectionExists(name) {
  return db.getCollectionNames().includes(name) || db.getCollectionInfos({ name }).length > 0;
}

function listInputFiles() {
  const all = fs.readdirSync(IN_DIR);
  const filtered = all
    .filter((f) => f.startsWith(FILE_PREFIX) && f.endsWith(FILE_SUFFIX))
    .sort((a, b) => a.localeCompare(b, "en"));
  const files = filtered.map((f) => path.join(IN_DIR, f));
  if (MAX_FILES && MAX_FILES > 0) return files.slice(0, MAX_FILES);
  return files;
}

async function importOneFile(FILE, fileIndex, totalFiles) {
  const fileStart = Date.now();
  log(`FILE_START [${fileIndex}/${totalFiles}]: ${FILE}`);

  const rl = readline.createInterface({
    input: fs.createReadStream(FILE, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let ops = [];
  let totalLines = 0;
  let emptyLines = 0;
  let parsedDocs = 0;

  let totalOpsExecuted = 0;
  let bulks = 0;

  let totalInserted = 0;
  let totalUpserted = 0;
  let totalModified = 0;

  for await (const line of rl) {
    totalLines++;
    const s = line.trim();
    if (!s) { emptyLines++; continue; }

    let doc;
    try {
      doc = EJSON.parse(s, { relaxed: false });
    } catch (e) {
      throw new Error(`JSON parse error in file ${FILE} at line ${totalLines}: ${e && e.message ? e.message : e}`);
    }

    parsedDocs++;

    if (MODE === "insert") {
      ops.push({ insertOne: { document: doc } });
    } else if (MODE === "upsert") {
      if (doc._id === undefined) throw new Error(`Documento senza _id in file ${FILE} alla riga ${totalLines}: impossibile fare upsert.`);
      ops.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    } else {
      throw new Error(`MODE non valido: ${MODE}`);
    }

    if (ops.length >= BATCH) {
      bulks++;
      const bulkStart = Date.now();
      const res = db.getCollection(COLL).bulkWrite(ops, { ordered: false });

      const inserted = res.insertedCount || 0;
      const upserted = res.upsertedCount || 0;
      const modified = res.modifiedCount || 0;

      totalInserted += inserted;
      totalUpserted += upserted;
      totalModified += modified;
      totalOpsExecuted += ops.length;

      const bulkEnd = Date.now();
      log(`BULK_END file[${fileIndex}/${totalFiles}] #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
      ops = [];
    }

    if (LOG_EVERY_LINES > 0 && totalLines % LOG_EVERY_LINES === 0) {
      const t = Date.now();
      log(`PROGRESS file[${fileIndex}/${totalFiles}]: lines=${totalLines} parsed=${parsedDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} | elapsed=${fmtDuration(t - fileStart)}`);
    }
  }

  if (ops.length > 0) {
    bulks++;
    const bulkStart = Date.now();
    const res = db.getCollection(COLL).bulkWrite(ops, { ordered: false });

    const inserted = res.insertedCount || 0;
    const upserted = res.upsertedCount || 0;
    const modified = res.modifiedCount || 0;

    totalInserted += inserted;
    totalUpserted += upserted;
    totalModified += modified;
    totalOpsExecuted += ops.length;

    const bulkEnd = Date.now();
    log(`BULK_END file[${fileIndex}/${totalFiles}] #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(bulkEnd - bulkStart)}`);
  }

  const fileEnd = Date.now();
  log(`FILE_END [${fileIndex}/${totalFiles}]: lines=${totalLines} emptyLines=${emptyLines} parsedDocs=${parsedDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} | elapsed=${fmtDuration(fileEnd - fileStart)}`);

  return { totalLines, emptyLines, parsedDocs, bulks, opsExecuted: totalOpsExecuted, inserted: totalInserted, upserted: totalUpserted, modified: totalModified };
}

async function run() {
  const files = listInputFiles();
  if (!files || files.length === 0) {
    throw new Error(`Nessun file trovato in ${IN_DIR} con pattern ${FILE_PREFIX}*${FILE_SUFFIX}`);
  }

  log(`FILES_FOUND: ${files.length}`);
  for (const f of files) log(`  - ${f}`);

  if (DROP_BEFORE) {
    const dropStart = Date.now();
    const exists = collectionExists(COLL);
    if (exists) {
      log(`STEP_DROP_START: Dropping collection ${COLL} ...`);
      db.getCollection(COLL).drop();
      const dropEnd = Date.now();
      log(`STEP_DROP_END: Dropped ${COLL} | elapsed=${fmtDuration(dropEnd - dropStart)}`);
    } else {
      const dropEnd = Date.now();
      log(`STEP_DROP_SKIP: ${COLL} does not exist | elapsed=${fmtDuration(dropEnd - dropStart)}`);
    }
  }

  const step1Start = Date.now();
  let totals = { files: files.length, totalLines: 0, emptyLines: 0, parsedDocs: 0, bulks: 0, opsExecuted: 0, inserted: 0, upserted: 0, modified: 0 };

  for (let i = 0; i < files.length; i++) {
    const stats = await importOneFile(files[i], i + 1, files.length);
    totals.totalLines += stats.totalLines;
    totals.emptyLines += stats.emptyLines;
    totals.parsedDocs += stats.parsedDocs;
    totals.bulks += stats.bulks;
    totals.opsExecuted += stats.opsExecuted;
    totals.inserted += stats.inserted;
    totals.upserted += stats.upserted;
    totals.modified += stats.modified;
  }

  const step1End = Date.now();
  log(`STEP_IMPORT_ALL_END: elapsed=${fmtDuration(step1End - step1Start)}`);

  const endMs = Date.now();
  const finalCount = db.getCollection(COLL).countDocuments();
  log(`TOTALS: files=${totals.files} lines=${totals.totalLines} emptyLines=${totals.emptyLines} parsedDocs=${totals.parsedDocs} bulks=${totals.bulks} opsExecuted=${totals.opsExecuted} inserted=${totals.inserted} upserted=${totals.upserted} modified=${totals.modified} finalCollCount=${finalCount}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | COLL=${COLL} | finalCollCount=${finalCount}`);
}

run().catch((e) => {
  log(`ERROR: ${e && e.stack ? e.stack : e}`);
  quit(1);
});