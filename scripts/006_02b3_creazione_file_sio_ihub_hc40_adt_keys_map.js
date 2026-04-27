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

function isRefHC40Adt(ref) {
  return typeof ref === "string" && ref.includes("HC40-ADT.");
}
function isRefIhub(ref) {
  return typeof ref === "string" && ref.includes("IHUB.");
}

(() => {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};

  const ihubColl = params.ihubColl || "sio_to_ihub_map";
  const hc40AdtColl = params.hc40AdtColl || "sio_to_hc40_adt_map";

  const limitIhub = params.limitIhub ?? 0;
  const limitHc40Adt = params.limitHc40Adt ?? 0;

  const outFileName = params.outFileName || "sio_ihub_hc40_adt_keys_map.js";
  const constName = params.constName || "SIO_IHUB_HC40_ADT_KEYS";

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(outDir, outFileName);

  // ====== dichiarate FUORI dal try per evitare problemi di scope ======
  const stats = {
    TOTAL_IHUB_ROWS_READ: 0,
    TOTAL_HC40_ADT_ROWS_READ: 0,
    TOTAL_VALID_IHUB_PAIRS: 0,
    TOTAL_VALID_HC40_ADT_PAIRS: 0,
    SKIPPED_NOT_STRING: 0,
    COLLISIONS_FOUND: 0,
    COLLISIONS_RESOLVED_TO_HC40: 0,
    COLLISIONS_RESOLVED_TO_IHUB: 0
  };

  const chosenRightByLeft = Object.create(null);

  /**
   * Applica una singola coppia (left,right) alla mappa di output:
   * - se left non esiste => set
   * - se left esiste => collisione
   *    - HC40-ADT vince su IHUB (sempre)
   *    - altrimenti mantengo il valore esistente (stable)
   */
  function putWithPriority(map, left, right) {
    const prev = map[left];
    if (prev === undefined) {
      map[left] = right;
      return;
    }

    stats.COLLISIONS_FOUND++;

    const prevIsH = isRefHC40Adt(prev);
    const newIsH = isRefHC40Adt(right);

    if (newIsH && !prevIsH) {
      map[left] = right;
      stats.COLLISIONS_RESOLVED_TO_HC40++;
    } else {
      stats.COLLISIONS_RESOLVED_TO_IHUB++;
    }
  }

  function readPairsIntoMap(collName, limit) {
    let cursor = db.getCollection(collName).find(
      {},
      { _id: 0, chiaveKO: 1, newEncounterRef: 1 }
    );

    if (limit && limit > 0) cursor = cursor.limit(limit);

    const rows = cursor.toArray();

    for (const x of rows) {
      const k = x && x.chiaveKO;
      const v = x && x.newEncounterRef;

      if (typeof k !== "string" || typeof v !== "string") {
        stats.SKIPPED_NOT_STRING++;
        continue;
      }

      putWithPriority(chosenRightByLeft, k, v);

      if (collName === ihubColl) stats.TOTAL_VALID_IHUB_PAIRS++;
      else if (collName === hc40AdtColl) stats.TOTAL_VALID_HC40_ADT_PAIRS++;
    }

    if (collName === ihubColl) stats.TOTAL_IHUB_ROWS_READ += rows.length;
    else if (collName === hc40AdtColl) stats.TOTAL_HC40_ADT_ROWS_READ += rows.length;
  }

  const startMs = Date.now();
  print(
    `START: ${new Date(startMs).toISOString()} | outPath=${outPath} | LIMIT_IHUB=${limitIhub} | LIMIT_HC40_ADT=${limitHc40Adt} | ihubColl=${ihubColl} | hc40AdtColl=${hc40AdtColl}`
  );

  try {
    readPairsIntoMap(ihubColl, limitIhub);
    readPairsIntoMap(hc40AdtColl, limitHc40Adt);

    const keys = Object.keys(chosenRightByLeft)
      .sort((a, b) => a.localeCompare(b, "en"))
      .map((left) => `${left}#${chosenRightByLeft[left]}`);

    fs.writeFileSync(
      outPath,
      `const ${constName} = ` + JSON.stringify(keys, null, 2) + ";\n",
      "utf8"
    );

    let finalHC40 = 0;
    let finalIHUB = 0;
    for (const left of Object.keys(chosenRightByLeft)) {
      const r = chosenRightByLeft[left];
      if (isRefHC40Adt(r)) finalHC40++;
      else if (isRefIhub(r)) finalIHUB++;
    }

    const endMs = Date.now();
    print(`TOTAL_IHUB_ROWS_READ: ${stats.TOTAL_IHUB_ROWS_READ}`);
    print(`TOTAL_HC40_ADT_ROWS_READ: ${stats.TOTAL_HC40_ADT_ROWS_READ}`);
    print(`TOTAL_VALID_IHUB_PAIRS: ${stats.TOTAL_VALID_IHUB_PAIRS}`);
    print(`TOTAL_VALID_HC40_ADT_PAIRS: ${stats.TOTAL_VALID_HC40_ADT_PAIRS}`);
    print(`SKIPPED_NOT_STRING: ${stats.SKIPPED_NOT_STRING}`);

    print(`COLLISIONS_FOUND: ${stats.COLLISIONS_FOUND}`);
    print(`COLLISIONS_RESOLVED_TO_HC40: ${stats.COLLISIONS_RESOLVED_TO_HC40}`);
    print(`COLLISIONS_RESOLVED_TO_IHUB: ${stats.COLLISIONS_RESOLVED_TO_IHUB}`);

    print(`TOTAL_FINAL_KEYS_EXPORTED: ${keys.length}`);
    print(`FINAL_TARGETS: HC40-ADT=${finalHC40} | IHUB=${finalIHUB}`);

    print(`Wrote ${keys.length} keys to ${outPath}`);
    print(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)} | outPath=${outPath}`);

    print(
      JSON.stringify({
        type: "result",
        script: "006_02b3_creazione_file_sio_ihub_hc40_adt_keys_map.js",
        runId,
        stepId,
        dbName,
        ihubColl,
        hc40AdtColl,
        limitIhub,
        limitHc40Adt,
        outPath,
        constName,
        stats,
        exported: keys.length,
        finalTargets: { hc40Adt: finalHC40, ihub: finalIHUB },
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