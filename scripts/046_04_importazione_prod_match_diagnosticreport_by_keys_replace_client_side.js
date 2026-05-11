const fs = require("fs");
const readline = require("readline");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

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
  return (
    db.getCollectionNames().includes(name) ||
    db.getCollectionInfos({ name }).length > 0
  );
}

function listInputFiles(inDir, filePrefix, fileSuffix, maxFiles) {
  const all = fs.readdirSync(inDir);
  const filtered = all
    .filter((f) => f.startsWith(filePrefix) && f.endsWith(fileSuffix))
    .sort((a, b) => a.localeCompare(b, "en"));

  const files = filtered.map((f) => path.join(inDir, f));
  if (maxFiles && maxFiles > 0) return files.slice(0, maxFiles);
  return files;
}

async function importOneFile(COLL, FILE, fileIndex, totalFiles, MODE, BATCH, LOG_EVERY_LINES) {
  const fileStart = Date.now();
  log(`FILE_START [${fileIndex}/${totalFiles}]: ${FILE}`);

  const rl = readline.createInterface({
    input: fs.createReadStream(FILE, { encoding: "utf8" }),
    crlfDelay: Infinity
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
    if (!s) {
      emptyLines++;
      continue;
    }

    let doc;
    try {
      doc = EJSON.parse(s, { relaxed: false });
    } catch (e) {
      throw new Error(
        `JSON parse error in file ${FILE} at line ${totalLines}: ${e && e.message ? e.message : e}`
      );
    }

    parsedDocs++;

    if (MODE === "insert") {
      ops.push({ insertOne: { document: doc } });
    } else if (MODE === "upsert") {
      if (doc._id === undefined) {
        throw new Error(`Documento senza _id in file ${FILE} alla riga ${totalLines}: impossibile fare upsert.`);
      }
      ops.push({
        replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true }
      });
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

  return {
    file: FILE,
    totalLines,
    emptyLines,
    parsedDocs,
    bulks,
    opsExecuted: totalOpsExecuted,
    inserted: totalInserted,
    upserted: totalUpserted,
    modified: totalModified
  };
}

(async () => {
  const { runtime, context } = readRuntimeAndContext();
  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};
  const inSubDir = params.inSubDir || ""; // optional
  const inDir = path.join(expDir, String(runId || "no-runid"), inSubDir);

  const filePrefix = params.filePrefix || "diagnosticreport_impacted_chunk";
  const fileSuffix = params.fileSuffix || ".jsonl";
  const maxFiles = params.maxFiles ?? 0;

  const coll = params.collection || "diagnosticreport_impacted_modified";
  const mode = params.mode || "insert";
  const batch = params.batch ?? 1000;
  const dropBefore = params.dropBefore ?? true;
  const logEveryLines = params.logEveryLines ?? 100000;

  const startMs = Date.now();
  log(`START: IN_DIR=${inDir} | prefix=${filePrefix} | suffix=${fileSuffix} | MAX_FILES=${maxFiles} | COLL=${coll} | MODE=${mode} | BATCH=${batch} | DROP_BEFORE=${dropBefore}`);

  const files = listInputFiles(inDir, filePrefix, fileSuffix, maxFiles);
  if (!files.length) throw new Error(`Nessun file trovato in ${inDir} con pattern ${filePrefix}*${fileSuffix}`);

  log(`FILES_FOUND: ${files.length}`);
  files.forEach((f) => log(`  - ${f}`));

  if (dropBefore) {
    const dropStart = Date.now();
    const exists = collectionExists(coll);

    if (exists) {
      log(`STEP_DROP_START: Dropping collection ${coll} ...`);
      db.getCollection(coll).drop();
      log(`STEP_DROP_END: Dropped ${coll} | elapsed=${fmtDuration(Date.now() - dropStart)}`);
    } else {
      log(`STEP_DROP_SKIP: ${coll} does not exist | elapsed=${fmtDuration(Date.now() - dropStart)}`);
    }
  }

  let totals = {
    files: files.length,
    totalLines: 0,
    emptyLines: 0,
    parsedDocs: 0,
    bulks: 0,
    opsExecuted: 0,
    inserted: 0,
    upserted: 0,
    modified: 0
  };

  for (let i = 0; i < files.length; i++) {
    const s = await importOneFile(coll, files[i], i + 1, files.length, mode, batch, logEveryLines);
    totals.totalLines += s.totalLines;
    totals.emptyLines += s.emptyLines;
    totals.parsedDocs += s.parsedDocs;
    totals.bulks += s.bulks;
    totals.opsExecuted += s.opsExecuted;
    totals.inserted += s.inserted;
    totals.upserted += s.upserted;
    totals.modified += s.modified;
  }

  const endMs = Date.now();
  const finalCount = db.getCollection(coll).countDocuments();

  log(`TOTALS: files=${totals.files} lines=${totals.totalLines} emptyLines=${totals.emptyLines} parsedDocs=${totals.parsedDocs} bulks=${totals.bulks} opsExecuted=${totals.opsExecuted} inserted=${totals.inserted} upserted=${totals.upserted} modified=${totals.modified} finalCollCount=${finalCount}`);
  log(`END: elapsed=${fmtDuration(endMs - startMs)} | COLL=${coll} | finalCollCount=${finalCount}`);

  print(
    JSON.stringify({
      type: "result",
      script: "046_04_importazione_prod_match_diagnosticreport_by_keys_replace_client_side.js",
      runId,
      stepId,
      dbName,
      inDir,
      filePrefix,
      fileSuffix,
      maxFiles,
      collection: coll,
      mode,
      batch,
      dropBefore,
      totals,
      finalCollCount: finalCount,
      ts: new Date().toISOString()
    })
  );
})().catch((e) => {
  log(`ERROR: ${e && e.stack ? e.stack : e}`);
  quit(1);
});