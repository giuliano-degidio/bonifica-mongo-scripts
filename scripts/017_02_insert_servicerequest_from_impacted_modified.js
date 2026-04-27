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

(async () => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const sourceColl = params.sourceColl || "servicerequest_impacted_modified";
  const targetColl = params.targetColl || "servicerequest";
  const mode = params.mode || "upsert";
  const batch = params.batch ?? 1000;
  const logEvery = params.logEvery ?? 1000;

  const startMs = Date.now();
  log(`START: SOURCE_COLL=${sourceColl} | TARGET_COLL=${targetColl} | MODE=${mode} | BATCH=${batch} | LOG_EVERY=${logEvery}`);

  try {
    if (!collectionExists(sourceColl)) throw new Error(`Sorgente non trovata: ${sourceColl}`);

    const source = db.getCollection(sourceColl);
    const target = db.getCollection(targetColl);

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
        if (doc._id === undefined) throw new Error(`Documento senza _id letto da ${sourceColl} (readDocs=${readDocs})`);
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

    const finalCount = target.countDocuments();
    log(`TOTALS: readDocs=${readDocs} bulks=${bulks} opsExecuted=${totalOpsExecuted} inserted=${totalInserted} upserted=${totalUpserted} modified=${totalModified} finalTargetCount=${finalCount}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | TARGET_COLL=${targetColl} | finalTargetCount=${finalCount}`);

    print(JSON.stringify({
      type: "result",
      script: "017_02_insert_servicerequest_from_impacted_modified.js",
      runId, stepId, dbName,
      sourceColl, targetColl,
      mode, batch, logEvery,
      readDocs, bulks, opsExecuted: totalOpsExecuted,
      inserted: totalInserted, upserted: totalUpserted, modified: totalModified,
      finalTargetCount: finalCount,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR: ${e && e.stack ? e.stack : e}`);
    quit(1);
  }
})();