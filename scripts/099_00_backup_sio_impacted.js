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

function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

function assertArray(name, arr) {
  if (!Array.isArray(arr) || arr.length === 0) throw new Error(`${name} non è un array valido o è vuoto.`);
}

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};

  const idsFileName = params.idsFileName || "sio_modified_id.js";
  const idsFileNameHc40Adt = params.idsFileNameHc40Adt || "sio_modified_id_hc40_adt.js";

  const sourceCollection = params.sourceCollection || "encounter";
  const backupCollection = params.backupCollection || "bonifica_encounter_deleted_bck";

  const dropBefore = params.dropBefore ?? true;
  const batchIds = params.batchIds ?? 1000;
  const bulkWrite = params.bulkWrite ?? 1000;
  const logEvery = params.logEvery ?? 1000;

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const idsPath = path.join(outDir, idsFileName);
  const idsPathHc40 = path.join(outDir, idsFileNameHc40Adt);

  const startMs = Date.now();
  log(
    `START: idsPath=${idsPath} idsPathHc40=${idsPathHc40} sourceCollection=${sourceCollection} backupCollection=${backupCollection} dropBefore=${dropBefore} batchIds=${batchIds} bulkWrite=${bulkWrite} logEvery=${logEvery}`
  );

  try {
    if (!fs.existsSync(idsPath)) throw new Error(`File non trovato: ${idsPath}`);
    if (!fs.existsSync(idsPathHc40)) throw new Error(`File non trovato: ${idsPathHc40}`);

    load(idsPath);      // defines SIO_MODIFIED_ID
    load(idsPathHc40);  // defines SIO_MODIFIED_ID_HC40_ADT

    assertArray("SIO_MODIFIED_ID", SIO_MODIFIED_ID);
    assertArray("SIO_MODIFIED_ID_HC40_ADT", SIO_MODIFIED_ID_HC40_ADT);

    const idsMerged = []
      .concat(SIO_MODIFIED_ID)
      .concat(SIO_MODIFIED_ID_HC40_ADT)
      .filter((x) => typeof x === "string" && x.length > 0);

    const idsUnique = Array.from(new Set(idsMerged));

    log(`IDS_LOADED: SIO_MODIFIED_ID=${SIO_MODIFIED_ID.length} SIO_MODIFIED_ID_HC40_ADT=${SIO_MODIFIED_ID_HC40_ADT.length}`);
    log(`IDS_MERGED: merged=${idsMerged.length} unique=${idsUnique.length}`);

    const source = db.getCollection(sourceCollection);
    const backup = db.getCollection(backupCollection);

    if (dropBefore) {
      const dropStart = Date.now();
      if (collectionExists(backupCollection)) {
        log(`DROP_START: Dropping ${backupCollection} ...`);
        backup.drop();
        log(`DROP_END: Dropped ${backupCollection} | elapsed=${fmtDuration(Date.now() - dropStart)}`);
      } else {
        log(`DROP_SKIP: ${backupCollection} does not exist | elapsed=${fmtDuration(Date.now() - dropStart)}`);
      }
    }

    const idChunks = chunkArray(idsUnique, batchIds);
    log(`ID_CHUNKS: chunks=${idChunks.length} chunkSize=${batchIds}`);

    let copiedDocs = 0;
    let missingIds = 0;
    let bulkOps = [];
    let bulks = 0;

    for (let ci = 0; ci < idChunks.length; ci++) {
      const ids = idChunks[ci];
      const docs = source.find({ _id: { $in: ids } }).toArray();
      if (docs.length < ids.length) missingIds += (ids.length - docs.length);

      for (const doc of docs) {
        bulkOps.push({
          replaceOne: {
            filter: { _id: doc._id },
            replacement: doc,
            upsert: true
          }
        });

        copiedDocs++;

        if (logEvery > 0 && copiedDocs % logEvery === 0) {
          log(`PROGRESS: copiedDocs=${copiedDocs} bulks=${bulks} chunk=${ci + 1}/${idChunks.length} | elapsed=${fmtDuration(Date.now() - startMs)}`);
        }

        if (bulkOps.length >= bulkWrite) {
          bulks++;
          const bulkStart = Date.now();
          const res = backup.bulkWrite(bulkOps, { ordered: false });
          log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
          bulkOps = [];
        }
      }
    }

    if (bulkOps.length > 0) {
      bulks++;
      const bulkStart = Date.now();
      const res = backup.bulkWrite(bulkOps, { ordered: false });
      log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
      bulkOps = [];
    }

    const finalCount = backup.countDocuments();
    log(`TOTALS: ids_unique=${idsUnique.length} copiedDocs=${copiedDocs} missingIds_estimate=${missingIds} bulks=${bulks} backupCount=${finalCount}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | backupCollection=${backupCollection} backupCount=${finalCount}`);

    print(JSON.stringify({
      type: "result",
      script: "099_00_backup_sio_impacted.js",
      runId, stepId, dbName,
      outDir, idsPath, idsPathHc40,
      sourceCollection, backupCollection,
      dropBefore, batchIds, bulkWrite, logEvery,
      idsUnique: idsUnique.length,
      copiedDocs, missingIdsEstimate: missingIds,
      bulks,
      backupCount: finalCount,
      ts: new Date().toISOString()
    }));
  } catch (e) {
    log(`ERROR_TIME: elapsed=${fmtDuration(Date.now() - startMs)}`);
    log("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();