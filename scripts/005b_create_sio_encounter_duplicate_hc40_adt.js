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

  const srcHc40AdtLatestAll = params.srcHc40AdtLatestAll || "encounter_HC40_ADT_latest_all";
  const srcSioAll = params.srcSioAll || "encounter_SIO_all";

  const hc40AdtFlat = params.hc40AdtFlat || "encounter_HC40_ADT_latest_flat";
  const out = params.out || "sio_encounter_duplicate_hc40_adt";
  const dropBefore = params.dropBefore ?? true;

  const startMs = Date.now();
  print(`START: ${new Date(startMs).toISOString()} | HC40_ADT_FLAT=${hc40AdtFlat} | OUT=${out} | DROP_BEFORE=${dropBefore}`);

  try {
    // STEP 1: Build HC40_ADT_FLAT
    const step1Start = Date.now();
    print(`STEP1_START: ${new Date(step1Start).toISOString()} | building ${hc40AdtFlat} from ${srcHc40AdtLatestAll}`);

    if (dropBefore) {
      db.getCollection(hc40AdtFlat).drop();
    }

    db.getCollection(srcHc40AdtLatestAll).aggregate(
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
        { $out: hc40AdtFlat }
      ],
      { allowDiskUse: true }
    );

    const step1AfterOut = Date.now();
    const flatCount = db.getCollection(hc40AdtFlat).countDocuments();
    print(`STEP1_AFTER_OUT: ${new Date(step1AfterOut).toISOString()} | ${hc40AdtFlat}_count=${flatCount} | elapsed=${fmtDuration(step1AfterOut - step1Start)}`);

    const idxStart = Date.now();
    print(`STEP1_INDEX_START: ${new Date(idxStart).toISOString()} | creating index on ${hc40AdtFlat}.joinKey`);
    db.getCollection(hc40AdtFlat).createIndex({ joinKey: 1 });
    const idxEnd = Date.now();
    print(`STEP1_INDEX_END: ${new Date(idxEnd).toISOString()} | elapsed=${fmtDuration(idxEnd - idxStart)}`);

    // STEP 2: Build OUT
    const step2Start = Date.now();
    print(`STEP2_START: ${new Date(step2Start).toISOString()} | building ${out} from ${srcSioAll} + ${hc40AdtFlat}`);

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
            from: hc40AdtFlat,
            localField: "joinKey",
            foreignField: "joinKey",
            as: "hc40_adt"
          }
        },
        { $match: { "hc40_adt.0": { $exists: true } } },
        {
          $project: {
            _id: { value: "$value", paziente: "$paziente", chiaveKO: "$chiaveKO" },
            value: 1,
            paziente: 1,
            chiaveKO: 1,
            chiaveKO_timestamp: 1,
            encounter_timestamp: {
              $map: {
                input: "$hc40_adt",
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
    print(`TOTALS: ${hc40AdtFlat}_count=${flatCount} | ${out}_count=${outCount}`);
    print(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)}`);

    print(
      JSON.stringify({
        type: "result",
        script: "005b_create_sio_encounter_duplicate_hc40_adt.js",
        runId,
        stepId,
        dbName,
        srcHc40AdtLatestAll,
        srcSioAll,
        hc40AdtFlat,
        out,
        dropBefore,
        flatCount,
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