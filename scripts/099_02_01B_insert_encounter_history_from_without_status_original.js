const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

function now() { return new Date().toISOString(); }
function log(msg) { print(`[${now()}] ${msg}`); }
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(ms / 3600);
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

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";
  const params = context?.params || {};

  const sourceCollection = params.sourceCollection || "encounter_without_status_original";
  const historyCollection = params.historyCollection || "encounter_history";

  const bulkWrite = params.bulkWrite ?? 1000;
  const logEvery = params.logEvery ?? 1000;

  const historySuffixParam = params.historySuffix ?? null;
  const runSuffix = (historySuffixParam && String(historySuffixParam).trim())
    ? String(historySuffixParam).trim()
    : String(Date.now());

  const exportKeys = params.exportKeys ?? true;
  const exportConstName = params.exportConstName || "ENCOUNTER_HISTORY_WITHOUT_STATUS_INSERTED_ID";
  const exportFileName = params.exportFileName || "encounter_history_without_status_inserted_id.js";

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const exportFile = path.join(outDir, exportFileName);

  const startMs = Date.now();
  log(`START: sourceCollection=${sourceCollection} historyCollection=${historyCollection} bulkWrite=${bulkWrite} logEvery=${logEvery} runSuffix=${runSuffix} exportKeys=${exportKeys}`);

  let exportStream = null;
  let exportedIds = 0;

  try {
    // ATTENZIONE: se la collezione di origine non esiste --> SKIP
    if (!collectionExists(sourceCollection)) {
      log(`SKIP: sourceCollection not found: ${sourceCollection} (nessuna operazione eseguita)`);
      log(`END: status=SKIP elapsed=${fmtDuration(Date.now() - startMs)}`);
      quit(0);
    }

    if (!collectionExists(historyCollection)) log(`WARNING: history collection ${historyCollection} not found (will be created on first insert)`);

    if (exportKeys) {
      exportStream = fs.createWriteStream(exportFile, { encoding: "utf8", flags: "w" });
      exportStream.write(`// AUTO-GENERATED FILE\n`);
      exportStream.write(`// generated_at=${now()}\n`);
      exportStream.write(`// runSuffix=${runSuffix}\n`);
      exportStream.write(`// SOURCE_COLL=${sourceCollection} HISTORY_COLL=${historyCollection}\n\n`);
      exportStream.write(`const ${exportConstName} = [\n`);
    }

    const source = db.getCollection(sourceCollection);
    const history = db.getCollection(historyCollection);

    const cursor = source.find({}).sort({ _id: 1 });

    let bulkOps = [];
    let bulks = 0;

    let readDocs = 0;
    let writtenDocs = 0;

    while (cursor.hasNext()) {
      const doc = cursor.next();
      readDocs++;

      if (doc == null || typeof doc !== "object" || Array.isArray(doc)) throw new Error(`Documento non valido letto da ${sourceCollection} (readDocs=${readDocs})`);
      if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${sourceCollection} (readDocs=${readDocs})`);

      const newDoc = Object.assign({}, doc);
      newDoc._id = makeHistoryId(doc._id, runSuffix);
      newDoc.request = "BONIFICA RunID: " + (runId ?? "ND");

      if (exportStream) {
        exportStream.write(`  ${JSON.stringify(String(newDoc._id))},\n`);
        exportedIds++;
      }

      bulkOps.push({
        replaceOne: {
          filter: { _id: newDoc._id },
          replacement: newDoc,
          upsert: true
        }
      });

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
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | historyCollection=${historyCollection}`);

    print(JSON.stringify({
      type: "result",
      script: "099_02_01B_insert_encounter_history_from_without_status_original.js",
      runId, stepId, dbName,
      outDir,
      sourceCollection,
      historyCollection,
      runSuffix,
      bulkWrite, logEvery,
      exportKeys,
      exportConstName,
      exportFile: exportKeys ? exportFile : null,
      exportedIds,
      readDocs,
      writtenDocs,
      bulks,
      historyCount,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
    log("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  } finally {
    try { if (exportStream) exportStream.write("];\n"); } catch (_) {}
    try { if (exportStream) exportStream.end(); } catch (_) {}
  }
})();