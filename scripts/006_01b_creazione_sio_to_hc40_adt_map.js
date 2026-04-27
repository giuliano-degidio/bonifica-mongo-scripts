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

(async () => {
  const { runtime, context } = readRuntimeAndContext();
  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const mapColl = params.mapColl || "sio_to_hc40_adt_map";
  const srcColl = params.srcColl || "sio_encounter_duplicate_hc40_adt";

  const dryRun = params.dryRun ?? false;
  const limitMap = params.limitMap ?? 0;

  const startMs = Date.now();
  log(`START: building ${mapColl} from ${srcColl} | DRY_RUN=${dryRun} | LIMIT_MAP=${limitMap}`);

  try {
    const step1Start = Date.now();
    log(`STEP1_START: Building mapping collection ${mapColl} from ${srcColl} ...`);

    const mapExists =
      db.getCollectionNames().includes(mapColl) ||
      db.getCollectionInfos({ name: mapColl }).length > 0;

    if (mapExists) {
      db.getCollection(mapColl).drop();
      log(`STEP1_DROP: dropped existing ${mapColl}`);
    } else {
      log(`STEP1_DROP: ${mapColl} does not exist, skipping drop`);
    }

    const pipeline = [
      {
        $project: {
          _id: 0,
          chiaveKO: 1,
          newEncounterRef: { $first: "$encounter_timestamp.id_encounter" }
        }
      },
      { $match: { chiaveKO: { $type: "string" }, newEncounterRef: { $type: "string" } } },
      {
        $group: {
          _id: "$chiaveKO",
          chiaveKO: { $first: "$chiaveKO" },
          newEncounterRef: { $first: "$newEncounterRef" }
        }
      },
      { $project: { _id: 0, chiaveKO: 1, newEncounterRef: 1 } }
    ];

    if (limitMap && limitMap > 0) pipeline.push({ $limit: limitMap });

    pipeline.push({ $out: mapColl });

    db.getCollection(srcColl).aggregate(pipeline, { allowDiskUse: true });

    log(`STEP1_INDEX_START: Creating index on ${mapColl}.chiaveKO (unique) ...`);
    db.getCollection(mapColl).createIndex({ chiaveKO: 1 }, { unique: true });
    log(`STEP1_INDEX_END: Index created.`);

    const mapCount = db.getCollection(mapColl).countDocuments();
    const step1End = Date.now();
    log(`STEP1_TOTALS: ${mapColl} docs = ${mapCount}`);
    log(`STEP1_END: elapsed=${fmtDuration(step1End - step1Start)}`);

    const endMs = Date.now();
    log(`TOTALS: ${mapColl}=${mapCount}`);
    log(`END: elapsed=${fmtDuration(endMs - startMs)}`);

    print(
      JSON.stringify({
        type: "result",
        script: "006_01b_creazione_sio_to_hc40_adt_map.js",
        runId,
        stepId,
        dbName,
        srcColl,
        mapColl,
        dryRun,
        limitMap,
        mapCount,
        ts: new Date().toISOString()
      })
    );
  } catch (e) {
    const errMs = Date.now();
    log(`ERROR_TIME: elapsed=${fmtDuration(errMs - startMs)}`);
    print("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();