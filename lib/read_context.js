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
  if (globalThis.__RUNTIME__ || globalThis.__CONTEXT__) {
    return {
      runtime: globalThis.__RUNTIME__ || {},
      context: globalThis.__CONTEXT__ || {}
    };
  }

  const runId = process.env.RUN_ID || process.env.runId || String(Date.now());
  const expDir = process.env.EXP_DIR || process.env.expDir || "/data/Mongo_Sh_Script/EXP";
  const dbName = process.env.MONGO_DB_NAME || process.env.dbName || null;

  const ctxPath = path.join(expDir, runId, "context.json");
  const fileCtx = tryReadJsonFile(ctxPath) || {};

  const runtime = Object.assign(
    {
      runId,
      mongo: { dbName },
      paths: { expDir }
    },
    fileCtx.runtime || {}
  );

  const context = Object.assign(
    {
      runId,
      mongo: { dbName },
      paths: { expDir },
      params: {},
      step: { id: null }
    },
    fileCtx.context || {}
  );

  if (fileCtx.runId) {
    runtime.runId = fileCtx.runId;
    context.runId = fileCtx.runId;
  }
  if (fileCtx.paths?.expDir) {
    runtime.paths = runtime.paths || {};
    context.paths = context.paths || {};
    runtime.paths.expDir = fileCtx.paths.expDir;
    context.paths.expDir = fileCtx.paths.expDir;
  }

  return { runtime, context };
}

module.exports = { readRuntimeAndContext };