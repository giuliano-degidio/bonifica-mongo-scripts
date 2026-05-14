const fs = require("fs");
const path = require("path");
const readline = require("readline");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

function buildOp(doc, mode) {
  if (doc == null || typeof doc !== "object" || Array.isArray(doc)) {
    throw new Error("Riga non valida: atteso documento JSON/EJSON.");
  }
  if (doc._id === undefined) {
    throw new Error("Documento senza _id: impossibile importare.");
  }

  if (mode === "insert") return { insertOne: { document: doc } };
  if (mode === "upsert") {
    return {
      replaceOne: {
        filter: { _id: doc._id },
        replacement: doc,
        upsert: true,
      },
    };
  }
  throw new Error(`MODE non valido: ${mode}`);
}

async function importOneFile(filePath, opts, state) {
  const fileStartMs = Date.now();
  print(
    `FILE_START: ${new Date(fileStartMs).toISOString()} | file=${filePath} | importedSoFar=${state.totalImported} | elapsedTotal=${fmtDuration(
      fileStartMs - state.startMs
    )}`
  );

  const rl = readline.createInterface({
    input: fs.createReadStream(filePath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  let ops = [];
  let bulks = 0;
  let linesRead = 0;
  let skippedEmpty = 0;
  let badLines = 0;
  let processed = 0;

  for await (const line of rl) {
    linesRead++;

    const s = line.trim();
    if (!s) {
      skippedEmpty++;
      continue;
    }

    let doc;
    try {
      doc = EJSON.parse(s);
    } catch (e) {
      badLines++;
      const msg = `Bad EJSON line | file=${filePath} | line=${linesRead} | err=${e}`;
      if (opts.failOnBadLine) throw new Error(msg);
      print(`WARN: ${msg}`);
      continue;
    }

    ops.push(buildOp(doc, opts.mode));
    processed++;
    state.totalImported++;

    if (ops.length >= opts.batch) {
      bulks++;
      const res = db.getCollection(opts.collection).bulkWrite(ops, { ordered: false });
      ops = [];

      print(
        `BULK: ${new Date(Date.now()).toISOString()} | file=${path.basename(
          filePath
        )} | bulk#=${bulks} | ops=${opts.batch} | inserted=${res.insertedCount || 0} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} | totalImported=${state.totalImported}`
      );
    }

    if (opts.progressEvery > 0 && state.totalImported % opts.progressEvery === 0) {
      const nowMs = Date.now();
      print(
        `PROGRESS: ${new Date(nowMs).toISOString()} | imported=${state.totalImported} | elapsed=${fmtDuration(
          nowMs - state.startMs
        )}`
      );
    }
  }

  if (ops.length > 0) {
    bulks++;
    const res = db.getCollection(opts.collection).bulkWrite(ops, { ordered: false });
    print(
      `BULK_FINAL: ${new Date(Date.now()).toISOString()} | file=${path.basename(
        filePath
      )} | bulk#=${bulks} | ops=${ops.length} | inserted=${res.insertedCount || 0} upserted=${res.upsertedCount || 0} modified=${res.modifiedCount || 0} | totalImported=${state.totalImported}`
    );
    ops = [];
  }

  const fileEndMs = Date.now();
  print(
    `FILE_END: ${new Date(fileEndMs).toISOString()} | file=${filePath} | processed=${processed} | linesRead=${linesRead} | skippedEmpty=${skippedEmpty} | badLines=${badLines} | bulks=${bulks} | fileElapsed=${fmtDuration(
      fileEndMs - fileStartMs
    )} | totalImported=${state.totalImported} | totalElapsed=${fmtDuration(fileEndMs - state.startMs)}`
  );

  return { processed, linesRead, skippedEmpty, badLines, bulks };
}

(async () => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};

  const baseName = params.baseName || "encounter_SIO";
  const fileSuffix = params.fileSuffix || ".ejsonl";

  const collection = params.collection || "encounter_SIO_all";
  const mode = params.mode || "upsert";
  const dropBefore = params.dropBefore ?? true;

  const batch = params.batch ?? 100000;
  const progressEvery = params.progressEvery ?? 500000;
  const failOnBadLine = params.failOnBadLine ?? true;

  const runDir = path.join(expDir, String(runId || "no-runid"));
  const startMs = Date.now();

  print(
    `START: ${new Date(startMs).toISOString()} | runDir=${runDir} | pattern=${baseName}_*${fileSuffix} | coll=${collection} | MODE=${mode} | BATCH=${batch} | PROGRESS_EVERY=${progressEvery} | DROP_BEFORE=${dropBefore} | FAIL_ON_BAD_LINE=${failOnBadLine}`
  );

  if (dropBefore) {
    print(`Dropping collection ${collection} ...`);
    db.getCollection(collection).drop();
  }

  const all = fs.readdirSync(runDir);
  const files = all
    .filter((f) => f.startsWith(`${baseName}_`) && f.endsWith(fileSuffix))
    .sort()
    .map((f) => path.join(runDir, f));

  if (files.length === 0) {
    const endMs = Date.now();
    print(`NO_FILES: ${new Date(endMs).toISOString()} | runDir=${runDir} | elapsed=${fmtDuration(endMs - startMs)}`);

    print(
      JSON.stringify({
        type: "result",
        script: "004_1_import_encounter_sio_all.js",
        runId,
        stepId,
        dbName,
        collection,
        runDir,
        filesFound: 0,
        imported: 0,
        ts: new Date().toISOString(),
      })
    );
    return;
  }

  print(`FILES_FOUND: ${files.length}`);
  files.forEach((f, i) => print(`  [${i + 1}/${files.length}] ${f}`));

  const state = { startMs, totalImported: 0 };

  const opts = { collection, mode, batch, progressEvery, failOnBadLine };
  for (let i = 0; i < files.length; i++) {
    const fp = files[i];
    print(`RUN_FILE: ${i + 1}/${files.length} | ${fp}`);
    await importOneFile(fp, opts, state);
  }

  const endMs = Date.now();
  print(`TOTAL_IMPORTED: ${state.totalImported}`);
  print(`END: ${new Date(endMs).toISOString()} | imported=${state.totalImported} | elapsed=${fmtDuration(endMs - startMs)} | coll=${collection}`);

  print(
    JSON.stringify({
      type: "result",
      script: "004_1_import_encounter_sio_all.js",
      runId,
      stepId,
      dbName,
      collection,
      runDir,
      filesFound: files.length,
      imported: state.totalImported,
      ts: new Date().toISOString(),
    })
  );
})().catch((e) => {
  const errMs = Date.now();
  print(`ERROR_TIME: ${new Date(errMs).toISOString()}`);
  print("ERROR: " + (e && e.stack ? e.stack : e));
  quit(1);
});