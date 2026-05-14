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

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const sourceCollection = params.sourceCollection || "encounter_without_status_modified";
  const targetCollection = params.targetCollection || "encounter";

  const mode = params.mode || "upsert"; // upsert | insert
  const batch = params.batch ?? 1000;
  const logEvery = params.logEvery ?? 1000;

  const startMs = Date.now();
  log(`START: sourceCollection=${sourceCollection} targetCollection=${targetCollection} mode=${mode} batch=${batch} logEvery=${logEvery}`);

  try {
    // ATTENZIONE: se la sorgente non esiste => SKIP & exit(0)
    if (!collectionExists(sourceCollection)) {
      log(`SKIP: sourceCollection not found: ${sourceCollection} (nessuna operazione eseguita)`);
      log(`END: status=SKIP elapsed=${fmtDuration(Date.now() - startMs)}`);
      quit(0);
    }

    const source = db.getCollection(sourceCollection);
    const target = db.getCollection(targetCollection);

    const cursor = source.find({}).sort({ _id: 1 });

    let ops = [];
    let readDocs = 0;

    let bulks = 0;
    let totalOpsExecuted = 0;

    let totalInserted = 0;
    let totalUpserted = 0;
    let totalModified = 0;

    while (cursor.hasNext()) {
      const doc = cursor.next();
      readDocs++;

      if (mode === "insert") {
        ops.push({ insertOne: { document: doc } });
      } else if (mode === "upsert") {
        if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${sourceCollection} (readDocs=${readDocs})`);
        ops.push({ replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true } });
      } else {
        throw new Error(`MODE non valido: ${mode}`);
      }

      if (ops.length >= batch) {
        bulks++;
        const bulkStart = Date.now();
        const res = target.bulkWrite(ops, { ordered: false });

        const inserted = res.insertedCount || 0;
        const upserted = res.upsertedCount || 0;
        const modified = res.modifiedCount || 0;

        totalInserted += inserted;
        totalUpserted += upserted;
        totalModified += modified;
        totalOpsExecuted += ops.length;

        log(`BULK_END #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
        ops = [];
      }

      if (logEvery > 0 && readDocs % logEvery === 0) {
        log(`PROGRESS: readDocs=${readDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} | elapsed=${fmtDuration(Date.now() - startMs)}`);
      }
    }

    if (ops.length > 0) {
      bulks++;
      const bulkStart = Date.now();
      const res = target.bulkWrite(ops, { ordered: false });

      const inserted = res.insertedCount || 0;
      const upserted = res.upsertedCount || 0;
      const modified = res.modifiedCount || 0;

      totalInserted += inserted;
      totalUpserted += upserted;
      totalModified += modified;
      totalOpsExecuted += ops.length;

      log(`BULK_END #${bulks}: ops=${ops.length} inserted=${inserted} upserted=${upserted} modified=${modified} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
    }

    const finalTargetCount = target.countDocuments();

    log(`TOTALS: readDocs=${readDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} finalTargetCount=${finalTargetCount}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | targetCollection=${targetCollection}`);

    print(JSON.stringify({
      type: "result",
      script: "099_02_02_insert_encounter_without_status_modified.js",
      runId, stepId, dbName,
      sourceCollection,
      targetCollection,
      mode, batch, logEvery,
      readDocs,
      bulks,
      opsExecuted: totalOpsExecuted,
      inserted: totalInserted,
      upserted: totalUpserted,
      modified: totalModified,
      finalTargetCount,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
    log("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();