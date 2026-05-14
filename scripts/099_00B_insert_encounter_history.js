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
function chunkArray(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}
function assertArray(name, arr) {
  if (!Array.isArray(arr) || arr.length === 0) throw new Error(`${name} non è un array valido o è vuoto.`);
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

  // MODIFICA: Usare solo il file con SIO_IHUB_HC40_ADT_KEYS
  // Path fisso come da tua richiesta (puoi parametrizzare se serve)
  const idsFileNameMap = params.idsFileNameMap || "sio_ihub_hc40_adt_keys_map.js";
  const idsPathMap = path.join(expDir, String(runId || "no-runid"), idsFileNameMap);

  const sourceCollection = params.sourceCollection || "encounter";
  const historyCollection = params.historyCollection || "encounter_history";

  const batchIds = params.batchIds ?? 1000;
  const bulkWrite = params.bulkWrite ?? 1000;
  const logEvery = params.logEvery ?? 1000;

  const historySuffixParam = params.historySuffix ?? null;
  const runSuffix = (historySuffixParam && String(historySuffixParam).trim())
    ? String(historySuffixParam).trim()
    : String(Date.now());

  const exportKeys = params.exportKeys ?? true;
  const exportConstName = params.exportConstName || "ENCOUNTER_HISTORY_INSERTED_ID";
  const exportFilePrefix = (params.exportFilePrefix || "encounter_history_inserted_id").replace(/_+$/, "");

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  // Solo come info, non realmente più usati:
  // const idsPath = path.join(outDir, idsFileName);
  // const idsPathHc40 = path.join(outDir, idsFileNameHc40Adt);

  const exportFile = path.join(outDir, `${exportFilePrefix}.js`);

  const startMs = Date.now();
  log(`START: idsPathMap=${idsPathMap} sourceCollection=${sourceCollection} historyCollection=${historyCollection} batchIds=${batchIds} bulkWrite=${bulkWrite} logEvery=${logEvery} runSuffix=${runSuffix} exportKeys=${exportKeys}`);

  let exportStream = null;
  let exportedIds = 0;

  try {
    if (!fs.existsSync(idsPathMap)) throw new Error(`File non trovato: ${idsPathMap}`);

    if (exportKeys) {
      exportStream = fs.createWriteStream(exportFile, { encoding: "utf8", flags: "w" });
      exportStream.write(`// AUTO-GENERATED FILE\n`);
      exportStream.write(`// generated_at=${now()}\n`);
      exportStream.write(`// runSuffix=${runSuffix}\n`);
      exportStream.write(`// SOURCE_COLL=${sourceCollection} HISTORY_COLL=${historyCollection}\n\n`);
      exportStream.write(`const ${exportConstName} = [\n`);
    }

    // Carica solo il file con la mappa
    load(idsPathMap); // Definisce SIO_IHUB_HC40_ADT_KEYS



    assertArray("SIO_IHUB_HC40_ADT_KEYS", SIO_IHUB_HC40_ADT_KEYS);

    // Estrarre solo la parte prima di #
    const idsClean = SIO_IHUB_HC40_ADT_KEYS
      .map(x => typeof x === "string" && x.includes("#") ? x.split("#")[0].trim() : null)
      .filter(x => !!x);
    const idsUnique = Array.from(new Set(idsClean));

    log(`IDS_LOADED: SIO_IHUB_HC40_ADT_KEYS=${SIO_IHUB_HC40_ADT_KEYS.length}`);
    log(`IDS_CLEANED: cleaned=${idsClean.length} unique=${idsUnique.length}`);


    const source = db.getCollection(sourceCollection);
    const history = db.getCollection(historyCollection);

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
        const oldId = doc._id;
        const newDoc = Object.assign({}, doc);
        newDoc._id = makeHistoryId(oldId, runSuffix);
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

        copiedDocs++;

        if (logEvery > 0 && copiedDocs % logEvery === 0) {
          log(`PROGRESS: copiedDocs=${copiedDocs} bulks=${bulks} chunk=${ci + 1}/${idChunks.length} exportedIds=${exportedIds} | elapsed=${fmtDuration(Date.now() - startMs)}`);
        }

        if (bulkOps.length >= bulkWrite) {
          bulks++;
          const bulkStart = Date.now();
          const res = history.bulkWrite(bulkOps, { ordered: false });
          log(`BULK_END #${bulks}: ops=${bulkOps.length} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} inserted=${res.insertedCount || 0} | elapsed=${fmtDuration(Date.now() - bulkStart)}`);
          bulkOps = [];
        }
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
    log(`TOTALS: ids_unique=${idsUnique.length} copiedDocs=${copiedDocs} missingIds_estimate=${missingIds} bulks=${bulks} historyCount=${historyCount} exportedIds=${exportedIds}`);
    if (exportStream) log(`EXPORT_FILE: ${exportFile}`);
    log(`END: elapsed=${fmtDuration(Date.now() - startMs)} | historyCollection=${historyCollection}`);

    print(JSON.stringify({
      type: "result",
      script: "099_00B_insert_encounter_history.js",
      runId, stepId, dbName,
      outDir, idsPathMap,
      sourceCollection, historyCollection,
      runSuffix,
      batchIds, bulkWrite, logEvery,
      exportKeys,
      exportConstName,
      exportFile: exportKeys ? exportFile : null,
      exportedIds,
      idsUnique: idsUnique.length,
      copiedDocs, missingIdsEstimate: missingIds,
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