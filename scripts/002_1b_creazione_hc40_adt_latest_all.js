const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

function now() {
  return new Date().toISOString();
}
function log(msg) {
  print(`[${now()}] ${msg}`);
}
function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

function buildOp(doc, mode) {
  if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
    throw new Error("Riga non valida (atteso documento JSON).");
  }
  if (doc._id === undefined) {
    throw new Error("Documento senza _id: impossibile fare upsert.");
  }

  if (mode === "insert") return { insertOne: { document: doc } };
  if (mode === "upsert") {
    return {
      replaceOne: { filter: { _id: doc._id }, replacement: doc, upsert: true },
    };
  }
  throw new Error(`MODE non valido: ${mode}`);
}

(async () => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};
  const inFileName = params.inFileName || "002_encounter_HC40_ADT_latest_all.jsonl";
  const coll = params.collection || "encounter_HC40_ADT_latest_all";
  const mode = params.mode || "upsert";
  const batch = params.batch ?? 1000;
  const logEvery = params.logEvery ?? 10000;
  const dropBefore = params.dropBefore ?? true;

  const runOutDir = path.join(expDir, String(runId || "no-runid"));
  const filePath = path.join(runOutDir, inFileName);

  const startMs = Date.now();
  log(`START import | file=${filePath} | coll=${coll} | mode=${mode} | batch=${batch} | logEvery=${logEvery} | dropBefore=${dropBefore}`);

  if (dropBefore) {
    log(`Dropping collection ${coll} ...`);
    db.getCollection(coll).drop();
  }

  log(`Opening stream: ${filePath}`);
  const stream = fs.createReadStream(filePath, { encoding: "utf8" });

  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let ops = [];
  let bulks = 0;
  let imported = 0;
  let lines = 0;
  let skippedEmpty = 0;

  for await (const line of rl) {
    lines++;
    const trimmed = line.trim();
    if (!trimmed) {
      skippedEmpty++;
      continue;
    }

    let doc;
    try {
      doc = EJSON.parse(trimmed);
    } catch (e) {
      throw new Error(`Errore parsing EJSON alla riga ${lines}: ${e}`);
    }

    ops.push(buildOp(doc, mode));

    if (ops.length >= batch) {
      bulks++;
      const res = db.getCollection(coll).bulkWrite(ops, { ordered: false });
      imported += ops.length;

      if (logEvery > 0 && imported % logEvery === 0) {
        const nowMs = Date.now();
        log(`PROGRESS | bulk=${bulks} | imported=${imported} | elapsed=${fmtDuration(nowMs - startMs)} | inserted=${res.insertedCount || 0} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0}`);
      }
      ops = [];
    }
  }

  if (ops.length > 0) {
    bulks++;
    const res = db.getCollection(coll).bulkWrite(ops, { ordered: false });
    imported += ops.length;
    log(`FINAL_BULK | bulk=${bulks} | imported=${imported} | inserted=${res.insertedCount || 0} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0}`);
  }

  const endMs = Date.now();
  log(`TOTAL_IMPORTED: ${imported}`);
  log(`LINES_READ: ${lines} | SKIPPED_EMPTY: ${skippedEmpty}`);
  log(`END import | elapsed=${fmtDuration(endMs - startMs)} | coll=${coll}`);
   if (imported === 0) {
     // crea collection vuota scrivendo e subito eliminando un record fittizio
     db.getCollection(coll).insertOne({_dummy: true});
     db.getCollection(coll).deleteMany({_dummy: true});
     log(`Collezione ${coll} creata vuota (prima non esisteva).`);
   }
  print(
    JSON.stringify({
      type: "result",
      script: "002_1b_creazione_hc40_adt_latest_all.js",
      runId,
      stepId,
      dbName,
      collection: coll,
      filePath,
      mode,
      batch,
      imported,
      linesRead: lines,
      skippedEmpty,
      ts: new Date().toISOString(),
    })
  );
})().catch((e) => {
  log(`ERROR: ${e && e.stack ? e.stack : e}`);
  quit(1);
});