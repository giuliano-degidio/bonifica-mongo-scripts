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

(() => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};
  const srcColl = params.srcColl || "sio_to_ihub_map";
  const limit = params.limit ?? 0;
  const outFileName = params.outFileName || "sio_ihub_keys.js";
  const constName = params.constName || "SIO_IHUB_KEYS";

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, outFileName);

  const startMs = Date.now();
  print(`START: ${new Date(startMs).toISOString()} | outPath=${outPath} | LIMIT=${limit} | srcColl=${srcColl}`);

  try {
    let cursor = db.getCollection(srcColl).find({}, { _id: 0, chiaveKO: 1, newEncounterRef: 1 });
    if (limit && limit > 0) cursor = cursor.limit(limit);

    const rows = cursor.toArray();
    const keys = rows
      .map((x) => {
        const k = x && x.chiaveKO;
        const v = x && x.newEncounterRef;
        if (typeof k !== "string") return null;
        if (typeof v !== "string") return null;
        return `${k}#${v}`;
      })
      .filter((x) => typeof x === "string");

    fs.writeFileSync(outPath, `const ${constName} = ` + JSON.stringify(keys, null, 2) + ";\n", "utf8");

    const endMs = Date.now();
    print(`TOTAL_KEYS_EXPORTED: ${keys.length}`);
    print(`Wrote ${keys.length} keys to ${outPath}`);
    print(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)} | outPath=${outPath}`);

    print(
      JSON.stringify({
        type: "result",
        script: "006_02_esportazione_su_file_sio_to_ihub_map.js",
        runId,
        stepId,
        dbName,
        srcColl,
        limit,
        outPath,
        constName,
        exported: keys.length,
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