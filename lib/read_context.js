const fs = require("fs");
const path = require("path");

function tryReadJsonFile(p) {
  try {
    if (!p) return null;
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function readRuntimeAndContext() {
  // Allow tests or callers to inject directly
  if (globalThis.__RUNTIME__ || globalThis.__CONTEXT__) {
    return {
      runtime: globalThis.__RUNTIME__ || {},
      context: globalThis.__CONTEXT__ || {},
    };
  }

  // ----------------------------
  // Primary source (pipeline orchestrator):
  // run_pipeline.sh exports:
  //   - RUNTIME_PATH (runtime.json)
  //   - STEP_CONTEXT_PATH (context_step_XXX.json)
  // ----------------------------
  const runtimeFromFile = tryReadJsonFile(process.env.RUNTIME_PATH) || null;
  const contextFromFile = tryReadJsonFile(process.env.STEP_CONTEXT_PATH) || null;

  // ----------------------------
  // Fallback source (manual execution):
  // Old behaviour: use RUN_ID/EXP_DIR and a context.json in EXP/<runId>/context.json if present
  // ----------------------------
  const fallbackRunId = process.env.RUN_ID || process.env.runId || String(Date.now());
  const fallbackExpDir = process.env.EXP_DIR || process.env.expDir || "/data/Mongo_Sh_Script/EXP";
  const fallbackDbName = process.env.MONGO_DB_NAME || process.env.dbName || null;

  const legacyCtxPath = path.join(fallbackExpDir, fallbackRunId, "context.json");
  const legacyCtx = tryReadJsonFile(legacyCtxPath) || {};

  // Compose runtime/context with precedence:
  // 1) runtimeFromFile/contextFromFile (pipeline)
  // 2) legacyCtx.runtime/legacyCtx.context (manual legacy)
  // 3) environment defaults
  const runtime = Object.assign(
    {
      runId: fallbackRunId,
      mongo: { dbName: fallbackDbName },
      paths: { expDir: fallbackExpDir },
    },
    (legacyCtx && legacyCtx.runtime) || {},
    runtimeFromFile || {}
  );

  const context = Object.assign(
    {
      runId: fallbackRunId,
      mongo: { dbName: fallbackDbName },
      paths: { expDir: fallbackExpDir },
      params: {},
      step: { id: null },
    },
    (legacyCtx && legacyCtx.context) || {},
    contextFromFile || {}
  );

  // Ensure runId and expDir are consistent if present at top-level or nested
  const resolvedRunId =
    (context && context.runId) ||
    (runtime && runtime.runId) ||
    legacyCtx.runId ||
    fallbackRunId;

  runtime.runId = resolvedRunId;
  context.runId = resolvedRunId;

  // expDir can come from: context.paths.expDir, runtime.paths.expDir, legacyCtx.paths.expDir, fallbackExpDir
  const resolvedExpDir =
    (context && context.paths && context.paths.expDir) ||
    (runtime && runtime.paths && runtime.paths.expDir) ||
    (legacyCtx.paths && legacyCtx.paths.expDir) ||
    fallbackExpDir;

  runtime.paths = runtime.paths || {};
  context.paths = context.paths || {};
  runtime.paths.expDir = resolvedExpDir;
  context.paths.expDir = resolvedExpDir;

  return { runtime, context };
}

module.exports = { readRuntimeAndContext };
