const fs = require("fs");
const path = require("path");
const readline = require("readline");
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

(async function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  const filePrefix = params.filePrefix || "encounter_without_status_add_chunk";
  const fileSuffix = params.fileSuffix || ".jsonl";
  const maxFiles = params.maxFiles ?? 0;

  const coll = params.collection || "encounter_without_status_modified";
  const mode = params.mode || "insert"; // insert | upsert
  const batch = params.batch ?? 1000;
  const dropBefore = params.dropBefore ?? true;

  const logEveryLines = params.logEveryLines ?? 100000;

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  function listInputFiles() {
    const all = fs.readdirSync(outDir);
    const filtered = all
      .filter((f) => f.startsWith(filePrefix) && f.endsWith(fileSuffix))
      .sort((a, b) => a.localeCompare(b, "en"));
    const files = filtered.map((f) => path.join(outDir, f));
    if (maxFiles && maxFiles > 0) return files.slice(0, maxFiles);
    return files;
  }

  async function importOneFile(FILE, fileIndex, totalFiles) {
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
      if (!s) { emptyLines++; continue; }

      let doc;
      try {
        doc = EJSON.parse(s, { relaxed: false });
      } catch (e) {
        throw new Error(`JSON parse error in file ${FILE} at line ${totalLines}: ${e && e.message ? e.message : e}`);
      }

      parsedDocs++;

      if (mode === "insert") {
        ops.push({ insertOne: { document: doc } });
      } else if (mode === "upsert") {
        if (doc._id === undefined) throw new Error(`Documento senza _id in file ${FILE} alla riga ${totalLines}: impossibile fare upsert.`);
        ops.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
      } else {
        throw new Error(`MODE non valido: ${mode}`);
      }

      if (ops.length >= batch) {
        bulks++;
        const bulkStart = Date.now();
        const res = db.getCollection(coll).bulkWrite(ops, { ordered: false });

        const inserted = res.insertedCount || 0;
        const upserted = res.upsertedCount || 0;
        const modified = res.modifiedCount || 0;

        totalInserted += inserted;
        totalUpserted += upserted;
        totalModified += modified;
        totalOpsExecuted += ops.length;

        log(`BULK_END file[${fileIndex}/${totalFiles}] #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
        ops = [];
      }

      if (logEveryLines > 0 && totalLines % logEveryLines === 0) {
        log(`PROGRESS file[${fileIndex}/${totalFiles}]: lines=${totalLines} parsed=${parsedDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} | elapsed=${fmtDuration(Date.now() - fileStart)}`);
      }
    }

    if (ops.length > 0) {
      bulks++;
      const bulkStart = Date.now();
      const res = db.getCollection(coll).bulkWrite(ops, { ordered: false });

      const inserted = res.insertedCount || 0;
      const upserted = res.upsertedCount || 0;
      const modified = res.modifiedCount || 0;

      totalInserted += inserted;
      totalUpserted += upserted;
      totalModified += modified;
      totalOpsExecuted += ops.length;

      log(`BULK_END file[${fileIndex}/${totalFiles}] #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
    }

    log(`FILE_END [${fileIndex}/${totalFiles}]: lines=${totalLines} emptyLines=${emptyLines} parsedDocs=${parsedDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} | elapsed=${fmtDuration(Date.now() - fileStart)}`);

    return { totalLines, emptyLines, parsedDocs, bulks, opsExecuted: totalOpsExecuted, inserted: totalInserted, upserted: totalUpserted, modified: totalModified };
  }

  const startMs = Date.now();
  log(`START: outDir=${outDir} prefix=${filePrefix} suffix=${fileSuffix} maxFiles=${maxFiles} coll=${coll} mode=${mode} batch=${batch} dropBefore=${dropBefore}`);

  try {
    const files = listInputFiles();
    if (!files || files.length === 0) throw new Error(`Nessun file trovato in ${outDir} con pattern ${filePrefix}*${fileSuffix}`);

    log(`FILES_FOUND: ${files.length}`);
    for (const f of files) log(`  - ${f}`);

    if (dropBefore) {
      const dropStart = Date.now();
      if (collectionExists(coll)) {
        log(`STEP_DROP_START: Dropping collection ${coll} ...`);
        db.getCollection(coll).drop();
        log(`STEP_DROP_END: Dropped ${coll} | elapsed=${fmtDuration(Date.now() - dropStart)}`);
      } else {
        log(`STEP_DROP_SKIP: ${coll} does not exist | elapsed=${fmtDuration(Date.now() - dropStart)}`);
      }
    }

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

    const finalCollCount = db.getCollection(coll).countDocuments();
    log(`TOTALS: files=${totals.files} lines=${totals.totalLines} emptyLines=${totals.emptyLines} parsedDocs=${totals.parsedDocs} bulks=${totals.bulks} opsExecuted=${totals.opsExecuted} inserted=${totals.inserted} upserted=${totals.upserted} modified=${totals.modified} finalCollCount=${finalCollCount}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | coll=${coll}`);

    print(JSON.stringify({
      type: "result",
      script: "099_01_04_importazione_encounter_ihub_hc40_adt_without_status_modified.js",
      runId, stepId, dbName,
      outDir,
      filePrefix, fileSuffix, maxFiles,
      collection: coll,
      mode,
      batch,
      dropBefore,
      totals,
      finalCollCount,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR: ${e && e.stack ? e.stack : e}`);
    quit(1);
  }
})();