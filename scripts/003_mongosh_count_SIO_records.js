const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

(async () => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  const params = context?.params || {};
  const coll = params.collection || "encounter";
  const idPrefixStart = params.idPrefixStart || "Encounter/SIO.";
  const idPrefixEnd = params.idPrefixEnd || "Encounter/SIO/";

  const filter = { _id: { $gte: idPrefixStart, $lt: idPrefixEnd } };

  const n = await db.getCollection(coll).countDocuments(filter);

  print(`countDocuments ${idPrefixStart}: ${n}`);

  print(
    JSON.stringify({
      type: "result",
      script: "003_mongosh_count_SIO_records.js",
      runId,
      stepId,
      dbName,
      collection: coll,
      filter,
      count: n,
      ts: new Date().toISOString(),
    })
  );
})().catch((err) => {
  print("ERROR: " + (err && err.stack ? err.stack : err));
  quit(1);
});