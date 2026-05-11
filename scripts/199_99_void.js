const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

(() => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  //const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;

  //const params = context?.params || {};
  //const coll = params.collection || "encounter_SIO_all";
  //const filter = params.filter || {};

 // const n = db.getCollection(coll).countDocuments(filter);

 // print(`${coll} total documents (_id count): ${n}`);

  print(
    JSON.stringify({
      type: "result",
      script: "199_99_void.js",
      runId,
      stepId,
     // dbName,
     // collection: coll,
     // filter,
    //  count: n,
      ts: new Date().toISOString()
    })
  );
})();