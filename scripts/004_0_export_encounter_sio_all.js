const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

function pad4(n) { return String(n).padStart(4, "0"); }

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

  const idPrefixStart = params.idPrefixStart || "Encounter/SIO.";
  const idPrefixEnd = params.idPrefixEnd || "Encounter/SIO/";

  const baseName = params.baseName || "encounter_SIO";
  const chunkSize = params.chunkSize ?? 500000;
  const pageSize = params.pageSize ?? 50000;
  const logEvery = params.logEvery ?? 100000;

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const statePath = path.join(outDir, `${baseName}_state.json`);

  function loadState() {
    try {
      return JSON.parse(fs.readFileSync(statePath, "utf8"));
    } catch {
      return { lastId: null, fileIndex: 1, writtenInCurrent: 0, totalWritten: 0 };
    }
  }

  function saveState(st) {
    fs.writeFileSync(statePath, JSON.stringify(st, null, 2), "utf8");
  }

  function openStream(fileIndex) {
    const filePath = path.join(outDir, `${baseName}_${pad4(fileIndex)}.ejsonl`);
    print(`Writing to ${filePath}`);
    return fs.createWriteStream(filePath, { encoding: "utf8", flags: "a" });
  }

  const startMs = Date.now();
  print(
    `START: ${new Date(startMs).toISOString()} | outDir=${outDir} | baseName=${baseName} | chunkSize=${chunkSize} | pageSize=${pageSize} | LOG_EVERY=${logEvery} | statePath=${statePath}`
  );

  let state = loadState();
  print(
    `STATE_LOADED: fileIndex=${state.fileIndex} | writtenInCurrent=${state.writtenInCurrent} | totalWritten=${state.totalWritten} | lastId=${state.lastId}`
  );

  let out = openStream(state.fileIndex);
  let pages = 0;

  try {
    while (true) {
      const match = { _id: { $gte: idPrefixStart, $lt: idPrefixEnd } };
      if (state.lastId) match._id.$gt = state.lastId;

      const pageStartMs = Date.now();

      const batch = db.getCollection(coll)
        .aggregate(
          [
            { $match: match },
            { $sort: { _id: 1 } },
            { $limit: pageSize },
            {
              $project: {
                _id: 1,
                timestamp: 1,
                timestamp_data: { $toDate: "$timestamp" },
                value: "$resource.identifier.value",
                paziente: "$resource.subject.reference",
              },
            },
          ],
          { allowDiskUse: true }
        )
        .toArray();

      pages++;

      if (batch.length === 0) {
        print(`NO_MORE_DATA: pages=${pages} | lastId=${state.lastId}`);
        break;
      }

      print(
        `PAGE: ${pages} | fetched=${batch.length} | fromLastId=${state.lastId || "<START>"} | pageElapsed=${fmtDuration(
          Date.now() - pageStartMs
        )}`
      );

      for (const doc of batch) {
        out.write(EJSON.stringify(doc, { relaxed: true }) + "\n");

        state.lastId = doc._id;
        state.writtenInCurrent++;
        state.totalWritten++;

        if (state.writtenInCurrent >= chunkSize) {
          out.end();
          state.fileIndex++;
          state.writtenInCurrent = 0;
          out = openStream(state.fileIndex);
        }

        if (logEvery > 0 && state.totalWritten % logEvery === 0) {
          const nowMs = Date.now();
          print(
            `PROGRESS: ${new Date(nowMs).toISOString()} | exported=${state.totalWritten} | elapsed=${fmtDuration(
              nowMs - startMs
            )} | fileIndex=${state.fileIndex} | writtenInCurrent=${state.writtenInCurrent} | lastId=${state.lastId}`
          );
          saveState(state);
        }
      }

      saveState(state);
    }

    out.end();
    saveState(state);

    const endMs = Date.now();
    print(`TOTAL_EXPORTED: ${state.totalWritten}`);
    print(
      `END: ${new Date(endMs).toISOString()} | exported=${state.totalWritten} | pages=${pages} | lastFileIndex=${state.fileIndex} | elapsed=${fmtDuration(
        endMs - startMs
      )} | statePath=${statePath}`
    );

    print(
      JSON.stringify({
        type: "result",
        script: "004_0_export_encounter_sio_all.js",
        runId,
        stepId,
        dbName,
        collection: coll,
        idPrefixStart,
        idPrefixEnd,
        outDir,
        baseName,
        chunkSize,
        pageSize,
        logEvery,
        statePath,
        exported: state.totalWritten,
        pages,
        ts: new Date().toISOString(),
      })
    );
  } catch (e) {
    try { out.end(); } catch (_) {}
    saveState(state);

    const errMs = Date.now();
    print(
      `ERROR_TIME: ${new Date(errMs).toISOString()} | exported=${state.totalWritten} | pages=${pages} | fileIndex=${state.fileIndex} | lastId=${state.lastId} | elapsed=${fmtDuration(
        errMs - startMs
      )}`
    );
    print("ERROR: " + (e && e.stack ? e.stack : e));
    print(`State saved in: ${statePath}`);
    quit(1);
  }
})();