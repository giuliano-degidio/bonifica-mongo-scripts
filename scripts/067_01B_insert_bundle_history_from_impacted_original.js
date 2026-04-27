const fs = require("fs");
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
function makeHistoryId(oldId, msSuffix) {
  return `${oldId}/_history/${msSuffix}`;
}

(async () => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  const sourceColl = params.sourceColl || "bundle_impacted_original";
  const historyColl = params.historyColl || "bundle_history";

  const bulkWrite = params.bulkWrite ?? 1000;
  const logEvery = params.logEvery ?? 1000;

  const historySuffix = params.historySuffix ?? null;
  const exportKeys = params.exportKeys ?? true;
  const exportFileName = params.exportFileName || "bundle_history_inserted_id.js";
  const exportConstName = params.exportConstName || "BUNDLE_HISTORY_INSERTED_ID";

  const runOutDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(runOutDir, { recursive: true });
  const exportFile = path.join(runOutDir, exportFileName);

  const startMs = Date.now();
  const runSuffix =
    (historySuffix && String(historySuffix).trim())
      ? String(historySuffix).trim()
      : String(Date.now());

  log(`START: SOURCE_COLL=${sourceColl} | HISTORY_COLL=${historyColl} | BULK_WRITE=${bulkWrite} | LOG_EVERY=${logEvery} | runSuffix=${runSuffix} | EXPORT_KEYS=${exportKeys}`);

  let exportStream = null;
  let exportedIds = 0;

  try {
    if (!collectionExists(sourceColl)) throw new Error(`Source collection not found: ${sourceColl}`);
    if (!collectionExists(historyColl)) log(`WARNING: history collection ${historyColl} not found (will be created on first insert)`);

    if (exportKeys) {
      exportStream = fs.createWriteStream(exportFile, { encoding: "utf8", flags: "w" });
      exportStream.write(`// AUTO-GENERATED FILE\n`);
      exportStream.write(`// generated_at=${now()}\n`);
      exportStream.write(`// runId=${runId}\n`);
      exportStream.write(`// runSuffix=${runSuffix}\n`);
      exportStream.write(`// SOURCE_COLL=${sourceColl} HISTORY_COLL=${historyColl}\n\n`);
      exportStream.write(`const ${exportConstName} = [\n`);
    }

    const source = db.getCollection(sourceColl);
    const history = db.getCollection(historyColl);

    const cursor = source.find({}).sort({ _id: 1 });

    let bulkOps = [];
    let bulks = 0;

    let readDocs = 0;
    let writtenDocs = 0;

    while (cursor.hasNext()) {
      const doc = cursor.next();
      readDocs++;

      if (doc == null || typeof doc !== "object" || Array.isArray(doc)) throw new Error(`Documento non valido letto da ${sourceColl} (readDocs=${readDocs})`);
      if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${sourceColl} (readDocs=${readDocs})`);

      const newDoc = Object.assign({}, doc);
      newDoc._id = makeHistoryId(doc._id, runSuffix);
      newDoc.request = "BONIFICA";

      if (exportStream) {
        exportStream.write(`  ${JSON.stringify(String(newDoc._id))},\n`);
        exportedIds++;
      }

      bulkOps.push({ replaceOne: { filter: { _id: newDoc._id }, replacement: newDoc, upsert: true } });
      writtenDocs++;

      if (logEvery > 0 && writtenDocs % logEvery === 0) {
        log(`PROGRESS: readDocs=${readDocs} writtenDocs=${writtenDocs} bulks=${bulks} exportedIds=${exportedIds} | elapsed=${fmtDuration(Date.now() - startMs)}`);
      }

      if (bulkOps.length >= bulkWrite) {
        bulks++;
        const bulkStart = Date.now();
        const res = history.bulkWrite(bulkOps, { ordered: false });
        log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
        bulkOps = [];
      }
    }

    if (bulkOps.length > 0) {
      bulks++;
      const bulkStart = Date.now();
      const res = history.bulkWrite(bulkOps, { ordered: false });
      log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
      bulkOps = [];
    }

    const historyCount = history.countDocuments();
    log(`TOTALS: readDocs=${readDocs} writtenDocs=${writtenDocs} bulks=${bulks} historyCount=${historyCount} exportedIds=${exportedIds}`);
    if (exportStream) log(`EXPORT_FILE: ${exportFile}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | HISTORY_COLL=${historyColl}`);

    print(JSON.stringify({
      type: "result",
      script: "067_01B_insert_bundle_history_from_impacted_original.js",
      runId, stepId, dbName,
      sourceColl, historyColl,
      bulkWrite, logEvery,
      runSuffix,
      exportKeys,
      exportFile,
      exportConstName,
      exportedIds,
      historyCount,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR: ${e && e.stack ? e.stack : e}`);
    quit(1);
  } finally {
    try { if (exportStream) exportStream.write("];\n"); } catch (_) {}
    try { if (exportStream) exportStream.end(); } catch (_) {}
  }
})();