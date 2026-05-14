const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

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

  const srcIhubLatestAll = params.srcIhubLatestAll || "encounter_IHUB_latest_all";
  const srcSioAll = params.srcSioAll || "encounter_SIO_all";

  const ihubFlat = params.ihubFlat || "encounter_IHUB_latest_flat";
  const out = params.out || "sio_encounter_duplicate";
  const dropBefore = params.dropBefore ?? true;

  const startMs = Date.now();
  print(`START: ${new Date(startMs).toISOString()} | IHUB_FLAT=${ihubFlat} | OUT=${out} | DROP_BEFORE=${dropBefore}`);

  try {
    // STEP 1: Build IHUB_FLAT
    const step1Start = Date.now();
    print(`STEP1_START: ${new Date(step1Start).toISOString()} | building ${ihubFlat} from ${srcIhubLatestAll}`);

    if (dropBefore) {
      db.getCollection(ihubFlat).drop();
    }

    db.getCollection(srcIhubLatestAll).aggregate(
      [
        { $unwind: "$value" },
        {
          $project: {
            _id: 0,
            joinKey: { $concat: ["$paziente", "|", "$value"] },
            paziente: 1,
            value: 1,
            id_encounter: "$_id",
            timestamp: 1
          }
        },
        {
          $group: {
            _id: { joinKey: "$joinKey", id_encounter: "$id_encounter" },
            joinKey: { $first: "$joinKey" },
            paziente: { $first: "$paziente" },
            value: { $first: "$value" },
            id_encounter: { $first: "$id_encounter" },
            timestamp: { $first: "$timestamp" }
          }
        },
        { $out: ihubFlat }
      ],
      { allowDiskUse: true }
    );

    const step1AfterOut = Date.now();
    const ihubFlatCount = db.getCollection(ihubFlat).countDocuments();
    print(`STEP1_AFTER_OUT: ${new Date(step1AfterOut).toISOString()} | ${ihubFlat}_count=${ihubFlatCount} | elapsed=${fmtDuration(step1AfterOut - step1Start)}`);

    const idxStart = Date.now();
    print(`STEP1_INDEX_START: ${new Date(idxStart).toISOString()} | creating index on ${ihubFlat}.joinKey`);
    db.getCollection(ihubFlat).createIndex({ joinKey: 1 });
    const idxEnd = Date.now();
    print(`STEP1_INDEX_END: ${new Date(idxEnd).toISOString()} | elapsed=${fmtDuration(idxEnd - idxStart)}`);

    // STEP 2: Build OUT
    const step2Start = Date.now();
    print(`STEP2_START: ${new Date(step2Start).toISOString()} | building ${out} from ${srcSioAll} + ${ihubFlat}`);

    if (dropBefore) {
      const exists =
        db.getCollectionNames().includes(out) ||
        db.getCollectionInfos({ name: out }).length > 0;

      if (exists) {
        db.getCollection(out).drop();
      }
    }

    db.getCollection(srcSioAll).aggregate(
      [
        { $unwind: "$value" },
        {
          $project: {
            chiaveKO: "$_id",
            chiaveKO_timestamp: "$timestamp",
            paziente: 1,
            value: 1,
            joinKey: { $concat: ["$paziente", "|", "$value"] }
          }
        },
        {
          $lookup: {
            from: ihubFlat,
            localField: "joinKey",
            foreignField: "joinKey",
            as: "ihub"
          }
        },
        { $match: { "ihub.0": { $exists: true } } },
        {
          $project: {
            _id: { value: "$value", paziente: "$paziente", chiaveKO: "$chiaveKO" },
            value: 1,
            paziente: 1,
            chiaveKO: 1,
            chiaveKO_timestamp: 1,
            encounter_timestamp: {
              $map: {
                input: "$ihub",
                as: "m",
                in: { id_encounter: "$$m.id_encounter", timestamp: "$$m.timestamp" }
              }
            }
          }
        },
        {
          $project: {
            _id: 1,
            value: 1,
            paziente: 1,
            chiaveKO: 1,
            chiaveKO_timestamp: 1,
            encounter_timestamp: { $setUnion: ["$encounter_timestamp", []] }
          }
        },
        { $out: out }
      ],
      { allowDiskUse: true }
    );

    const step2End = Date.now();
    const outCount = db.getCollection(out).countDocuments();
    print(`STEP2_END: ${new Date(step2End).toISOString()} | ${out}_count=${outCount} | elapsed=${fmtDuration(step2End - step2Start)}`);

    const endMs = Date.now();
    print(`TOTALS: ${ihubFlat}_count=${ihubFlatCount} | ${out}_count=${outCount}`);
    print(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)}`);

    print(
      JSON.stringify({
        type: "result",
        script: "005_create_sio_encounter_duplicate.js",
        runId,
        stepId,
        dbName,
        srcIhubLatestAll,
        srcSioAll,
        ihubFlat,
        out,
        dropBefore,
        ihubFlatCount,
        outCount,
        ts: new Date().toISOString()
      })
    );
  } catch (e) {
    const errMs = Date.now();
    print(`ERROR_TIME: ${new Date(errMs).toISOString()} | elapsed=${fmtDuration(errMs - startMs)}`);
    print("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();