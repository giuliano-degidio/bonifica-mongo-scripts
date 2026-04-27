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
  const srcColl = params.srcColl || "sio_to_hc40_adt_map";
  const limit = params.limit ?? 0;
  const outFileName = params.outFileName || "sio_modified_id_hc40_adt.js";
  const constName = params.constName || "SIO_MODIFIED_ID_HC40_ADT";

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, outFileName);

  const startMs = Date.now();
  print(`START: ${new Date(startMs).toISOString()} | outPath=${outPath} | LIMIT=${limit} | srcColl=${srcColl}`);

  try {
    let cursor = db.getCollection(srcColl).find({}, { chiaveKO: 1, _id: 0 });
    if (limit && limit > 0) cursor = cursor.limit(limit);

    const rows = cursor.toArray();
    const ids = rows
      .map((x) => (x && x.chiaveKO !== undefined ? String(x.chiaveKO) : null))
      .filter((x) => typeof x === "string" && x.length > 0);

    fs.writeFileSync(outPath, `const ${constName} = ` + JSON.stringify(ids, null, 2) + ";\n", "utf8");

    const endMs = Date.now();
    print(`TOTAL_ID_EXPORTED: ${ids.length}`);
    print(`Wrote ${ids.length} chiaveKO values to ${outPath}`);
    print(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)} | outPath=${outPath}`);

    print(
      JSON.stringify({
        type: "result",
        script: "006_02b2_esportazione_su_file_sio_modified_id_hc40_adt_map.js",
        runId,
        stepId,
        dbName,
        srcColl,
        limit,
        outPath,
        constName,
        exported: ids.length,
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