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
  return db.getCollectionNames().includes(name) || db.getCollectionInfos({ name }).length > 0;
}
function listInputFiles(inDir, filePrefix, fileSuffix, maxFiles) {
  const all = fs.readdirSync(inDir);
  const filtered = all.filter((f) => f.startsWith(filePrefix) && f.endsWith(fileSuffix)).sort((a,b)=>a.localeCompare(b,"en"));
  const files = filtered.map((f) => path.join(inDir, f));
  if (maxFiles && maxFiles > 0) return files.slice(0, maxFiles);
  return files;
}

async function importOneFile(COLL, FILE, fileIndex, totalFiles, MODE, BATCH) {
  log(`FILE_START [${fileIndex}/${totalFiles}]: ${FILE}`);

  const rl = readline.createInterface({ input: fs.createReadStream(FILE, { encoding: "utf8" }), crlfDelay: Infinity });

  let ops = [];
  let totalLines = 0, emptyLines = 0, parsedDocs = 0;
  let totalOpsExecuted = 0, bulks = 0;
  let totalInserted = 0, totalUpserted = 0, totalModified = 0;

  for await (const line of rl) {
    totalLines++;
    const s = line.trim();
    if (!s) { emptyLines++; continue; }

    let doc;
    try { doc = EJSON.parse(s, { relaxed: false }); }
    catch (e) { throw new Error(`JSON parse error in file ${FILE} at line ${totalLines}: ${e && e.message ? e.message : e}`); }

    parsedDocs++;

    if (MODE === "insert") ops.push({ insertOne: { document: doc } });
    else if (MODE === "upsert") {
      if (doc._id === undefined) throw new Error(`Documento senza _id in file ${FILE} alla riga ${totalLines}: impossibile fare upsert.`);
      ops.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
    } else throw new Error(`MODE non valido: ${MODE}`);

    if (ops.length >= BATCH) {
      bulks++;
      const res = db.getCollection(COLL).bulkWrite(ops, { ordered: false });
      totalInserted += res.insertedCount || 0;
      totalUpserted += res.upsertedCount || 0;
      totalModified += res.modifiedCount || 0;
      totalOpsExecuted += ops.length;
      ops = [];
    }
  }

  if (ops.length > 0) {
    bulks++;
    const res = db.getCollection(COLL).bulkWrite(ops, { ordered: false });
    totalInserted += res.insertedCount || 0;
    totalUpserted += res.upsertedCount || 0;
    totalModified += res.modifiedCount || 0;
    totalOpsExecuted += ops.length;
    ops = [];
  }

  log(`FILE_END [${fileIndex}/${totalFiles}]: lines=${totalLines} emptyLines=${emptyLines} parsedDocs=${parsedDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted}`);
  return { totalLines, emptyLines, parsedDocs, bulks, opsExecuted: totalOpsExecuted, inserted: totalInserted, upserted: totalUpserted, modified: totalModified };
}

(async () => {
  const { runtime, context } = readRuntimeAndContext();
  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  const inSubDir = params.inSubDir || "";
  const inDir = path.join(expDir, String(runId || "no-runid"), inSubDir);

  const filePrefix = params.filePrefix || "bundle_impacted_original_chunk";
  const fileSuffix = params.fileSuffix || ".jsonl";
  const maxFiles = params.maxFiles ?? 0;

  const coll = params.collection || "bundle_impacted_original";
  const mode = params.mode || "insert";
  const batch = params.batch ?? 1000;
  const dropBefore = params.dropBefore ?? true;

  const startMs = Date.now();
  log(`START: IN_DIR=${inDir} | prefix=${filePrefix} | suffix=${fileSuffix} | MAX_FILES=${maxFiles} | COLL=${coll} | MODE=${mode} | BATCH=${batch} | DROP_BEFORE=${dropBefore}`);

  const files = listInputFiles(inDir, filePrefix, fileSuffix, maxFiles);
  if (!files.length) throw new Error(`Nessun file trovato in ${inDir} con pattern ${filePrefix}*${fileSuffix}`);

  log(`FILES_FOUND: ${files.length}`);
  files.forEach((f) => log(`  - ${f}`));

  if (dropBefore) {
    const exists = collectionExists(coll);
    if (exists) {
      log(`STEP_DROP_START: Dropping collection ${coll} ...`);
      db.getCollection(coll).drop();
      log(`STEP_DROP_END: Dropped ${coll}`);
    }
  }

  let totals = { files: files.length, totalLines: 0, emptyLines: 0, parsedDocs: 0, bulks: 0, opsExecuted: 0, inserted: 0, upserted: 0, modified: 0 };

  for (let i = 0; i < files.length; i++) {
    const s = await importOneFile(coll, files[i], i + 1, files.length, mode, batch);
    totals.totalLines += s.totalLines;
    totals.emptyLines += s.emptyLines;
    totals.parsedDocs += s.parsedDocs;
    totals.bulks += s.bulks;
    totals.opsExecuted += s.opsExecuted;
    totals.inserted += s.inserted;
    totals.upserted += s.upserted;
    totals.modified += s.modified;
  }

  const finalCount = db.getCollection(coll).countDocuments();
  log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | COLL=${coll} | finalCollCount=${finalCount}`);

  print(JSON.stringify({
    type: "result",
    script: "066_04B_importazione_prod_match_bundle_by_keys_original.js",
    runId, stepId, dbName,
    inDir, filePrefix, fileSuffix, maxFiles,
    collection: coll, mode, batch, dropBefore,
    totals, finalCollCount: finalCount,
    ts: new Date().toISOString()
  }));
})().catch((e) => {
  log(`ERROR: ${e && e.stack ? e.stack : e}`);
  quit(1);
});