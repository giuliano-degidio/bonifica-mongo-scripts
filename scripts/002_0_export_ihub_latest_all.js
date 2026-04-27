const fs = require("fs");
const path = require("path");
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
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};
  const coll = params.collection || "encounter";

  const idPrefixStart = params.idPrefixStart || "Encounter/IHUB.";
  const idPrefixEnd = params.idPrefixEnd || "Encounter/IHUB/";

  const outFileName = params.outFileName || "002_encounter_IHUB_latest_all.jsonl";
  const docLimit = params.docLimit ?? 0;
  const logEvery = params.logEvery ?? 10000;

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, outFileName);

  const pipeline = [
    { $match: { _id: { $gte: idPrefixStart, $lt: idPrefixEnd } } },
    { $sort: { timestamp: -1, _id: -1 } },
    {
      $group: {
        _id: {
          value: "$resource.identifier.value",
          paziente: "$resource.subject.reference",
        },
        encounterId: { $first: "$_id" },
        timestamp: { $first: "$timestamp" },
      },
    },
    {
      $project: {
        _id: "$encounterId",
        timestamp: 1,
        timestamp_data: { $toDate: "$timestamp" },
        value: "$_id.value",
        paziente: "$_id.paziente",
      },
    },
    { $sort: { timestamp: -1, _id: -1 } },
  ];

  const startMs = Date.now();
  print(
    `START: ${new Date(startMs).toISOString()} | outPath=${outPath} | LOG_EVERY=${logEvery} | DOC_LIMIT=${docLimit}`
  );

  const out = fs.createWriteStream(outPath, { encoding: "utf8" });

  let count = 0;
  try {
    const cursor = db.getCollection(coll).aggregate(pipeline, { allowDiskUse: true });

    while (cursor.hasNext()) {
      const doc = cursor.next();

      out.write(EJSON.stringify(doc, { relaxed: true }) + "\n");
      count++;

      if (docLimit > 0 && count >= docLimit) break;

      if (logEvery > 0 && count % logEvery === 0) {
        const nowMs = Date.now();
        print(`PROGRESS: ${new Date(nowMs).toISOString()} | exported=${count} | elapsed=${fmtDuration(nowMs - startMs)}`);
      }
    }

    out.end();

    const endMs = Date.now();
    print(`END: ${new Date(endMs).toISOString()} | exported=${count} | elapsed=${fmtDuration(endMs - startMs)} | outPath=${outPath}`);

    print(
      JSON.stringify({
        type: "result",
        script: "002_0_export_ihub_latest_all.js",
        runId,
        stepId,
        dbName,
        collection: coll,
        idPrefixStart,
        idPrefixEnd,
        outPath,
        exported: count,
        ts: new Date().toISOString(),
      })
    );
  } catch (e) {
    try { out.end(); } catch (_) {}
    const errMs = Date.now();
    print(`ERROR_TIME: ${new Date(errMs).toISOString()} | exported=${count} | elapsed=${fmtDuration(errMs - startMs)}`);
    print("ERROR: " + (e && e.stack ? e.stack : e));
    quit(1);
  }
})();