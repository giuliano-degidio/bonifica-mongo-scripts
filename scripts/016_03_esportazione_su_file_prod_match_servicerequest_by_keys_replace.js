const fs = require("fs");
const path = require("path");
const { readRuntimeAndContext } = require("/data/Mongo_Sh_Script/lib/read_context.js");

// ====== DEBUG ======
const DEBUG = false;
const DEBUG_DOC_ID = ""; // es: "ServiceRequest/DNLAB.1-40936527-202404072159-4-0"
const DEBUG_NO_WRITE = false;
const DEBUG_PRINT_MAP_FOR_KEY = "";

// ====== DEFAULTS ======
const DEFAULT_MAP_CHUNK_SIZE = 1000;
const DEFAULT_BATCH_SIZE = 2000;
const DEFAULT_LOG_EVERY = 1000;

// ServiceRequest: match name specifico (tuo update)
const DEFAULT_MATCH_NAME = "encounter:encounter.reference";
const DEFAULT_HINT_NAME = "attribute_search.name_1_attribute_search.value_1__id_1";

function fmtDuration(ms) {
  const s = Math.floor(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = s % 60;
  return `${h}h ${m}m ${ss}s`;
}

function readJsonIfExists(p, fallback) {
  try {
    if (!fs.existsSync(p)) return fallback;
    const raw = fs.readFileSync(p, "utf8");
    return JSON.parse(raw);
  } catch {
    return fallback;
  }
}

function writeJson(p, obj) {
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
}

function afterPrefixOrSelf(str, prefix) {
  if (typeof str !== "string") return "";
  return str.startsWith(prefix) ? str.slice(prefix.length) : str;
}

function pad4(n) {
  return String(n).padStart(4, "0");
}

function isRightHC40(right) {
  if (typeof right !== "string") return false;
  return right.includes("Encounter/HC40-ADT.") || right.includes("HC40-ADT.");
}

function isRightIHUB(right) {
  if (typeof right !== "string") return false;
  return right.includes("Encounter/IHUB.") || right.includes("IHUB.");
}

/**
 * Dedup/resolve collisions on input pairs:
 * - group by LEFT (full left string)
 * - choose ONE right per left
 * - priority: HC40-ADT over IHUB
 */
function resolvePairsCollisions(pairs) {
  const chosenRightByLeft = Object.create(null);

  let badRows = 0;
  let collisionsFound = 0;

  for (const row of pairs) {
    if (typeof row !== "string") { badRows++; continue; }
    const idx = row.indexOf("#");
    if (idx < 1) { badRows++; continue; }

    const left = row.slice(0, idx);
    const right = row.slice(idx + 1);
    if (!left || !right) { badRows++; continue; }

    const prev = chosenRightByLeft[left];
    if (prev === undefined) {
      chosenRightByLeft[left] = right;
      continue;
    }

    collisionsFound++;

    const prevIsH = isRightHC40(prev);
    const newIsH = isRightHC40(right);

    if (newIsH && !prevIsH) {
      chosenRightByLeft[left] = right;
    }
  }

  const resolvedPairs = [];
  let resolvedToHc40 = 0;
  let resolvedToIhub = 0;

  for (const left of Object.keys(chosenRightByLeft)) {
    const right = chosenRightByLeft[left];
    resolvedPairs.push(`${left}#${right}`);

    if (isRightHC40(right)) resolvedToHc40++;
    else if (isRightIHUB(right)) resolvedToIhub++;
  }

  resolvedPairs.sort((a, b) => {
    const la = a.split("#")[0];
    const lb = b.split("#")[0];
    return la.localeCompare(lb, "en");
  });

  return {
    resolvedPairs,
    stats: { badRows, collisionsFound, resolvedToHc40, resolvedToIhub }
  };
}

// ---------- build maps ----------
function buildMapsFromPairs(pairs) {
  const mapFullUpper = Object.create(null);
  const mapFullLower = Object.create(null);

  const mapAfterUpper = Object.create(null);
  const mapAfterLower = Object.create(null);

  const mapTokenUpper = Object.create(null);
  const mapTokenLower = Object.create(null);

  const sioKeysFullUpper = [];

  let bad = 0;

  for (const row of pairs) {
    if (typeof row !== "string") { bad++; continue; }
    const idx = row.indexOf("#");
    if (idx < 1) { bad++; continue; }

    const left = row.slice(0, idx);
    const right = row.slice(idx + 1);
    if (!left || !right) { bad++; continue; }

    sioKeysFullUpper.push(left);

    mapFullUpper[left] = right;
    mapFullLower[left.toLowerCase()] = right.toLowerCase();

    const leftAfter = afterPrefixOrSelf(left, "Encounter/");
    const rightAfter = afterPrefixOrSelf(right, "Encounter/");

    mapAfterUpper[leftAfter] = rightAfter;
    mapAfterLower[leftAfter.toLowerCase()] = rightAfter.toLowerCase();

    // token map
    const idxS = leftAfter.indexOf("SIO.");
    const idxSL = leftAfter.toLowerCase().indexOf("sio.");

    const idxI = rightAfter.indexOf("IHUB.");
    const idxIL = rightAfter.toLowerCase().indexOf("ihub.");

    const idxH = rightAfter.indexOf("HC40-ADT.");
    const idxHL = rightAfter.toLowerCase().indexOf("hc40-adt.");

    if (idxS >= 0 && (idxI >= 0 || idxH >= 0)) {
      const leftTok = leftAfter.slice(idxS + 4);

      let rightTok = "";
      if (idxH >= 0) rightTok = rightAfter.slice(idxH + 9); // HC40 priority
      else rightTok = rightAfter.slice(idxI + 5);

      if (leftTok) {
        mapTokenUpper[leftTok] = rightTok;
        mapTokenLower[leftTok.toLowerCase()] = rightTok;
      }
    } else if (idxSL >= 0 && (idxIL >= 0 || idxHL >= 0)) {
      const leftTok = leftAfter.slice(idxSL + 4);

      let rightTok = "";
      if (idxHL >= 0) rightTok = rightAfter.slice(idxHL + 9);
      else rightTok = rightAfter.slice(idxIL + 5);

      if (leftTok) mapTokenLower[leftTok.toLowerCase()] = rightTok;
    }
  }

  return {
    maps: { mapFullUpper, mapFullLower, mapAfterUpper, mapAfterLower, mapTokenUpper, mapTokenLower },
    sioKeysFullUpper,
    badRows: bad
  };
}

// ---------- replacement (exact) ----------
function makeReplaceExactFn(maps) {
  const { mapFullUpper, mapFullLower, mapAfterUpper, mapAfterLower, mapTokenUpper, mapTokenLower } = maps;

  return function replaceExact(s) {
    if (typeof s !== "string") return { value: s, mode: "none" };

    if (mapFullUpper[s]) return { value: mapFullUpper[s], mode: "direct" };

    const sLower = s.toLowerCase();
    if (s === sLower && mapFullLower[sLower]) return { value: mapFullLower[sLower], mode: "direct" };

    if (mapAfterUpper[s]) return { value: mapAfterUpper[s], mode: "direct" };
    if (s === sLower && mapAfterLower[sLower]) return { value: mapAfterLower[sLower], mode: "direct" };

    if (mapTokenUpper[s]) return { value: mapTokenUpper[s], mode: "direct" };
    if (s === sLower && mapTokenLower[sLower]) return { value: mapTokenLower[sLower], mode: "direct" };

    return { value: s, mode: "none" };
  };
}

// ---------- replacement exact + pipeSuffix ----------
function makeReplaceFnWithPipeSuffix(replaceExactFn) {
  return function replaceExactOrPipeSuffix(s) {
    if (typeof s !== "string") return { value: s, mode: "none" };

    const direct = replaceExactFn(s);
    if (direct.mode !== "none" && direct.value !== s) return direct;

    const pipe = s.lastIndexOf("|");
    if (pipe === -1) return { value: s, mode: "none" };

    const prefix = s.slice(0, pipe + 1);
    const suffix = s.slice(pipe + 1);

    const r2 = replaceExactFn(suffix);
    if (r2.mode === "none" || r2.value === suffix) return { value: s, mode: "none" };

    return { value: prefix + r2.value, mode: "pipeSuffix" };
  };
}

// ---------- apply replacements ----------
function applyReplacementsToDocument(doc, replaceFn, debugCollector) {
  let replDirect = 0;
  let replPipe = 0;

  function rep(obj, key, pth) {
    if (!obj || typeof obj !== "object") return;
    const v = obj[key];
    if (typeof v !== "string") return;

    const r = replaceFn(v);
    if (r.value !== v) {
      obj[key] = r.value;
      if (r.mode === "pipeSuffix") replPipe++;
      else replDirect++;
      if (debugCollector) debugCollector.push({ path: pth, before: v, after: r.value, mode: r.mode });
    }
  }

  // ServiceRequest: resource.encounter reference (object OR array)
  try {
    const enc = doc?.resource?.encounter;

    if (enc && typeof enc === "object" && !Array.isArray(enc)) {
      rep(enc, "reference", "resource.encounter.reference");
    }

    if (Array.isArray(enc)) {
      for (let i = 0; i < enc.length; i++) {
        const e = enc[i];
        if (e && typeof e === "object") rep(e, "reference", `resource.encounter[${i}].reference`);
      }
    }
  } catch (_) {}

  // attribute_search + join + join.attribute_search + join._id
  try {
    const as = doc?.attribute_search;
    if (Array.isArray(as)) {
      for (let i = 0; i < as.length; i++) {
        const a = as[i];
        if (!a || typeof a !== "object") continue;

        rep(a, "value", `attribute_search[${i}].value`);
        rep(a, "value_lower", `attribute_search[${i}].value_lower`);

        const joins = a.join;
        if (Array.isArray(joins)) {
          for (let j = 0; j < joins.length; j++) {
            const enc = joins[j];
            if (!enc || typeof enc !== "object") continue;

            rep(enc, "_id", `attribute_search[${i}].join[${j}]._id`);

            const jas = enc.attribute_search;
            if (Array.isArray(jas)) {
              for (let k = 0; k < jas.length; k++) {
                const ja = jas[k];
                if (!ja || typeof ja !== "object") continue;

                rep(ja, "value", `attribute_search[${i}].join[${j}].attribute_search[${k}].value`);
                rep(ja, "value_lower", `attribute_search[${i}].join[${j}].attribute_search[${k}].value_lower`);
              }
            }
          }
        }
      }
    }
  } catch (_) {}

  // attribute_search_sort.* (tutte stringhe)
  try {
    const ass = doc?.attribute_search_sort;
    if (ass && typeof ass === "object" && !Array.isArray(ass)) {
      for (const k of Object.keys(ass)) {
        const v = ass[k];
        if (typeof v !== "string") continue;

        const r = replaceFn(v);
        if (r.value !== v) {
          ass[k] = r.value;
          if (r.mode === "pipeSuffix") replPipe++;
          else replDirect++;
          if (debugCollector) debugCollector.push({ path: `attribute_search_sort["${k}"]`, before: v, after: r.value, mode: r.mode });
        }
      }
    }
  } catch (_) {}

  return { repl_direct: replDirect, repl_pipeSuffix: replPipe };
}

(function main() {
  const { runtime, context } = readRuntimeAndContext();

  const runId = runtime?.runId || context?.runId || null;
  const stepId = context?.step?.id ?? null;
  const dbName = runtime?.mongo?.dbName || context?.mongo?.dbName || null;
  const expDir = context?.paths?.expDir || runtime?.paths?.expDir || "/data/Mongo_Sh_Script/EXP";

  const params = context?.params || {};

  const outDir = path.join(expDir, String(runId || "no-runid"));
  fs.mkdirSync(outDir, { recursive: true });

  const outBase = params.outBase || "servicerequest_impacted";
  const outExt = params.outExt || ".jsonl";

  const mapFileName = params.mapFileName || "sio_ihub_hc40_adt_keys_map.js";
  const mapConstName = params.mapConstName || "SIO_IHUB_HC40_ADT_KEYS";
  const mapPath = params.mapPath || path.join(outDir, mapFileName);

  const mapChunkSize = params.mapChunkSize ?? DEFAULT_MAP_CHUNK_SIZE;
  const batchSize = params.batchSize ?? DEFAULT_BATCH_SIZE;
  const logEvery = params.logEvery ?? DEFAULT_LOG_EVERY;

  const matchName = params.matchName || DEFAULT_MATCH_NAME;
  const hintName = params.hintName || DEFAULT_HINT_NAME;

  const logToFile = params.logToFile ?? true;
  const logToConsole = params.logToConsole ?? true;
  const logFileFlags = params.logFileFlags || "a";
  const logPath = params.logPath || path.join(outDir, `${outBase}.log`);

  const globalStatePath = params.globalStatePath || path.join(outDir, `${outBase}.chunks.state.json`);

  function chunkOutPath(chunkNo1Based) {
    return `${outDir}/${outBase}_chunk${pad4(chunkNo1Based)}${outExt}`;
  }
  function chunkStatePath(chunkNo1Based) {
    return `${outDir}/${outBase}_chunk${pad4(chunkNo1Based)}.state.json`;
  }

  const __logStream = logToFile ? fs.createWriteStream(logPath, { encoding: "utf8", flags: logFileFlags }) : null;

  function logLine(s) {
    const line = String(s);
    if (logToConsole) print(line);
    if (__logStream) __logStream.write(line + "\n");
  }

  let summary = {
    totalPairs: 0,
    chunkSize: 0,
    chunkCount: 0,
    completedChunksBefore: [],
    completedChunksAfter: [],
    totalExportedDocs: 0,
    totalReplDirect: 0,
    totalReplPipeSuffix: 0,
    chunks: []
  };

  const startMs = Date.now();
  logLine(`START: ${new Date(startMs).toISOString()} | DEBUG=${DEBUG} | DEBUG_DOC_ID=${DEBUG_DOC_ID || "(none)"} | MAP_PATH=${mapPath}`);
  logLine(`LOG: logPath=${logPath} | LOG_TO_CONSOLE=${logToConsole} | LOG_FILE_FLAGS=${logFileFlags}`);
  logLine(`CONFIG: MAP_CHUNK_SIZE=${mapChunkSize} | BATCH_SIZE=${batchSize} | LOG_EVERY=${logEvery} | DEBUG_NO_WRITE=${DEBUG_NO_WRITE}`);
  logLine(`STATE: GLOBAL_STATE_PATH=${globalStatePath}`);

  // --- continue in part 2/3 ---
    try {
    load(mapPath);
    const pairsAll = globalThis[mapConstName];

    if (!Array.isArray(pairsAll) || pairsAll.length === 0) {
      throw new Error(`${mapConstName} non è un array valido o è vuoto. File=${mapPath}`);
    }

    const totalPairs = pairsAll.length;
    const chunkSize = (mapChunkSize && mapChunkSize > 0) ? mapChunkSize : totalPairs;
    const chunkCount = Math.ceil(totalPairs / chunkSize);

    summary.totalPairs = totalPairs;
    summary.chunkSize = chunkSize;
    summary.chunkCount = chunkCount;

    logLine(`MAP_TOTAL: pairs=${totalPairs} | chunkSize=${chunkSize} | chunkCount=${chunkCount}`);

    const globalState = readJsonIfExists(globalStatePath, { completedChunks: [], updated_at: null });
    const completedSet = new Set(Array.isArray(globalState.completedChunks) ? globalState.completedChunks : []);
    summary.completedChunksBefore = Array.from(completedSet).sort((a, b) => a - b);

    logLine(`GLOBAL_STATE: completedChunks=${summary.completedChunksBefore.join(",") || "(none)"}`);

    for (let chunkIndex = 0; chunkIndex < chunkCount; chunkIndex++) {
      const chunkNo = chunkIndex + 1;
      const from = chunkIndex * chunkSize;
      const to = Math.min(totalPairs, from + chunkSize);

      const chunkPairsOriginal = pairsAll.slice(from, to);
      const outPathChunk = chunkOutPath(chunkNo);
      const statePathChunk = chunkStatePath(chunkNo);

      if (completedSet.has(chunkNo) && !DEBUG) {
        logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (global state). outPath=${outPathChunk}`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairsOriginal.length, outPath: outPathChunk, statePath: statePathChunk, exported: 0, replDirect: 0, replPipeSuffix: 0, completed: true, skipped: true });
        continue;
      }

      logLine(`CHUNK_START: #${chunkNo}/${chunkCount} | map_from=${from} | map_to=${to - 1} | map_rows=${chunkPairsOriginal.length} | outPath=${outPathChunk} | statePath=${statePathChunk}`);

      const resolved = resolvePairsCollisions(chunkPairsOriginal);
      const chunkPairs = resolved.resolvedPairs;
      const { badRows, collisionsFound, resolvedToHc40, resolvedToIhub } = resolved.stats;

      logLine(`CHUNK_COLLISIONS: #${chunkNo} | COLLISIONS_FOUND=${collisionsFound} | COLLISIONS_RESOLVED_TO_HC40=${resolvedToHc40} | COLLISIONS_RESOLVED_TO_IHUB=${resolvedToIhub} | BAD_ROWS=${badRows} | originalPairs=${chunkPairsOriginal.length} | resolvedPairs=${chunkPairs.length}`);

      const { maps, sioKeysFullUpper, badRows: badRowsAfterBuild } = buildMapsFromPairs(chunkPairs);
      const replaceExactFn = makeReplaceExactFn(maps);
      const replaceFn = makeReplaceFnWithPipeSuffix(replaceExactFn);

      logLine(`CHUNK_MAP: #${chunkNo} | pairs=${chunkPairs.length} | matchKeys=${sioKeysFullUpper.length} | badRowsAfterBuild=${badRowsAfterBuild}`);

      if (DEBUG && DEBUG_PRINT_MAP_FOR_KEY) {
        const k = DEBUG_PRINT_MAP_FOR_KEY;
        const kAfter = afterPrefixOrSelf(k, "Encounter/");
        const tokUpper = (() => {
          const i = kAfter.indexOf("SIO.");
          return i >= 0 ? kAfter.slice(i + 4) : "";
        })();

        logLine(`CHUNK_DEBUG_MAP_CHECK: #${chunkNo} key="${k}"`);
        logLine(`  mapFullUpper: ${maps.mapFullUpper[k] ? maps.mapFullUpper[k] : "(missing)"}`);
        logLine(`  mapAfterUpper("${kAfter}"): ${maps.mapAfterUpper[kAfter] ? maps.mapAfterUpper[kAfter] : "(missing)"}`);
        logLine(`  mapTokenUpper("${tokUpper}"): ${tokUpper && maps.mapTokenUpper[tokUpper] ? maps.mapTokenUpper[tokUpper] : "(missing)"}`);
      }

      const chunkState = readJsonIfExists(statePathChunk, { lastId: null, exported: 0, completed: false });
      let lastId = (typeof chunkState.lastId === "string") ? chunkState.lastId : null;
      let exported = (typeof chunkState.exported === "number") ? chunkState.exported : 0;

      if (chunkState.completed && !DEBUG) {
        completedSet.add(chunkNo);
        globalState.completedChunks = Array.from(completedSet).sort((a, b) => a - b);
        globalState.updated_at = new Date().toISOString();
        writeJson(globalStatePath, globalState);

        logLine(`CHUNK_SKIP: #${chunkNo}/${chunkCount} già completato (chunk state). outPath=${outPathChunk}`);
        summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairsOriginal.length, outPath: outPathChunk, statePath: statePathChunk, exported, replDirect: 0, replPipeSuffix: 0, completed: true, skipped: true });
        continue;
      }

      const outFlags = (!DEBUG && exported > 0 && fs.existsSync(outPathChunk)) ? "a" : "w";
      const out = (DEBUG && DEBUG_NO_WRITE) ? null : fs.createWriteStream(outPathChunk, { encoding: "utf8", flags: outFlags });

      logLine(`CHUNK_RESUME: #${chunkNo} | lastId=${lastId ? lastId : "(null)"} | exported=${exported} | outFlags=${outFlags}`);

      const baseQuery = {
        attribute_search: {
          $elemMatch: {
            name: matchName,
            value: { $in: sioKeysFullUpper }
          }
        }
      };
      if (DEBUG_DOC_ID) baseQuery._id = DEBUG_DOC_ID;

      let totalDirect = 0;
      let totalPipe = 0;
      let batchNo = 0;

      while (true) {
        batchNo++;

        const q = Object.assign({}, baseQuery);
        if (!DEBUG_DOC_ID && lastId) q._id = { $gt: lastId };

        const cursor = db.servicerequest.find(q).sort({ _id: 1 }).limit(batchSize);
        if (!DEBUG) {
          try { cursor.hint(hintName); } catch (_) {}
        }

        const docs = cursor.toArray();
        if (docs.length === 0) break;

        let batchDirect = 0;
        let batchPipe = 0;

        for (const doc of docs) {
          const debugChanges = DEBUG ? [] : null;

          const c = applyReplacementsToDocument(doc, replaceFn, debugChanges);
          batchDirect += c.repl_direct;
          batchPipe += c.repl_pipeSuffix;

          if (DEBUG) {
            logLine(`DEBUG_DOC: chunk#${chunkNo} _id=${doc._id} | repl_direct=${c.repl_direct} | repl_pipeSuffix=${c.repl_pipeSuffix}`);
            for (const ch of debugChanges) {
              logLine(`  - [${ch.mode}] ${ch.path}: "${ch.before}" -> "${ch.after}"`);
            }
          }

          if (out) out.write(EJSON.stringify(doc, { relaxed: true }) + "\n");

          exported++;
          lastId = doc._id;

          if (logEvery > 0 && exported % logEvery === 0) {
            const nowMs = Date.now();
            logLine(`PROGRESS: chunk#${chunkNo} ${new Date(nowMs).toISOString()} | exported=${exported} | lastId=${lastId} | repl_direct=${totalDirect + batchDirect} | repl_pipeSuffix=${totalPipe + batchPipe} | elapsed=${fmtDuration(nowMs - startMs)}`);
          }
        }

        totalDirect += batchDirect;
        totalPipe += batchPipe;

        if (!DEBUG) {
          writeJson(statePathChunk, { lastId, exported, completed: false, updated_at: new Date().toISOString() });
        }

        logLine(`BATCH_DONE: chunk#${chunkNo} batch#${batchNo} | docs=${docs.length} | repl_direct=${batchDirect} | repl_pipeSuffix=${batchPipe} | lastId=${lastId} | chunkExported=${exported}`);

        if (DEBUG) break;
      }

      if (out) out.end();

      if (!DEBUG) {
        writeJson(statePathChunk, { lastId, exported, completed: true, completed_at: new Date().toISOString() });

        completedSet.add(chunkNo);
        globalState.completedChunks = Array.from(completedSet).sort((a, b) => a - b);
        globalState.updated_at = new Date().toISOString();
        writeJson(globalStatePath, globalState);
      }

      logLine(`CHUNK_END: #${chunkNo}/${chunkCount} | exported=${exported} | totalReplDirect=${totalDirect} | totalReplPipeSuffix=${totalPipe} | outPath=${outPathChunk} | completed=${!DEBUG}`);

      summary.totalExportedDocs += exported;
      summary.totalReplDirect += totalDirect;
      summary.totalReplPipeSuffix += totalPipe;

      summary.chunks.push({ chunkNo, from, to: to - 1, mapRows: chunkPairsOriginal.length, outPath: outPathChunk, statePath: statePathChunk, exported, replDirect: totalDirect, replPipeSuffix: totalPipe, completed: !DEBUG, skipped: false });

      if (DEBUG) {
        logLine("DEBUG: stop after first processed chunk (DEBUG=true).");
        break;
      }
    }

    summary.completedChunksAfter = Array.from(
      new Set(readJsonIfExists(globalStatePath, { completedChunks: [] }).completedChunks || [])
    ).sort((a, b) => a - b);

    const endMs = Date.now();
    logLine(`END: ${new Date(endMs).toISOString()} | elapsed=${fmtDuration(endMs - startMs)}`);

    // Part 3/3 below
  } catch (e) {
    logLine("ERROR: " + (e && e.stack ? e.stack : e));
    try { if (__logStream) __logStream.end(); } catch (_) {}
    quit(1);
  }
    try { if (__logStream) __logStream.end(); } catch (_) {}

  print(
    JSON.stringify({
      type: "result",
      script: "016_03_esportazione_su_file_prod_match_servicerequest_by_keys_replace.js",
      runId,
      stepId,
      dbName,

      expDir,
      outDir,
      outBase,
      outExt,
      logPath,
      mapPath,
      mapConstName,
      globalStatePath,

      debug: DEBUG,
      debugDocId: DEBUG_DOC_ID || null,
      debugNoWrite: DEBUG_NO_WRITE,

      mapChunkSize,
      batchSize,
      logEvery,
      matchName,
      hintName,

      summary,

      ts: new Date().toISOString()
    })
  );
})();