import {
  clampInt,
  json,
} from "./http-helpers.js";
import { diffDays, isIsoDate, isoDate, isoDateBerlin } from "./date-utils.js";
import {
  formatEventDistance,
  getLifeEventCategoryLabel,
  getLifeEventEffect,
  inferRaceDistanceLabel,
  isARaceCategory,
  isARaceEvent,
  isLifeEventActiveOnDay,
  isLifeEventCategory,
  normalizeEventCategory,
  parseLifeEventBoundary,
} from "./event-utils.js";
import {
  avg,
  bucketLoadsByDay,
  clamp,
  countBy,
  isMondayIso,
  median,
  pearsonCorrelation,
  round,
  safeRound,
  std,
  sum,
  uniq,
} from "./stats-utils.js";
import {
  handleSyncRequest,
  handleWatchfaceRequest,
  handleWeeklyMailTestRequest,
  isWatchfacePath,
  isWeeklyMailTestPath,
  withWorkerErrorBoundary,
} from "./request-handlers.js";
import chunk1 from "./index-chunks/chunk1.js";
import chunk2 from "./index-chunks/chunk2.js";
import chunk3 from "./index-chunks/chunk3.js";
import chunk4 from "./index-chunks/chunk4.js";
import chunk5 from "./index-chunks/chunk5.js";
import chunk6 from "./index-chunks/chunk6.js";
import chunk7 from "./index-chunks/chunk7.js";

const source = [chunk1, chunk2, chunk3, chunk4, chunk5, chunk6, chunk7].join("");

const runtime = new Function(
  "clampInt",
  "json",
  "diffDays",
  "isIsoDate",
  "isoDate",
  "isoDateBerlin",
  "formatEventDistance",
  "getLifeEventCategoryLabel",
  "getLifeEventEffect",
  "inferRaceDistanceLabel",
  "isARaceCategory",
  "isARaceEvent",
  "isLifeEventActiveOnDay",
  "isLifeEventCategory",
  "normalizeEventCategory",
  "parseLifeEventBoundary",
  "avg",
  "bucketLoadsByDay",
  "clamp",
  "countBy",
  "isMondayIso",
  "median",
  "pearsonCorrelation",
  "round",
  "safeRound",
  "std",
  "sum",
  "uniq",
  "handleSyncRequest",
  "handleWatchfaceRequest",
  "handleWeeklyMailTestRequest",
  "isWatchfacePath",
  "isWeeklyMailTestPath",
  "withWorkerErrorBoundary",
  `${source}
return { defaultExport: __default_export__, __test, __internalTestHooksForRepoTestsOnly };`
);

const exportsFromChunks = runtime(
  clampInt,
  json,
  diffDays,
  isIsoDate,
  isoDate,
  isoDateBerlin,
  formatEventDistance,
  getLifeEventCategoryLabel,
  getLifeEventEffect,
  inferRaceDistanceLabel,
  isARaceCategory,
  isARaceEvent,
  isLifeEventActiveOnDay,
  isLifeEventCategory,
  normalizeEventCategory,
  parseLifeEventBoundary,
  avg,
  bucketLoadsByDay,
  clamp,
  countBy,
  isMondayIso,
  median,
  pearsonCorrelation,
  round,
  safeRound,
  std,
  sum,
  uniq,
  handleSyncRequest,
  handleWatchfaceRequest,
  handleWeeklyMailTestRequest,
  isWatchfacePath,
  isWeeklyMailTestPath,
  withWorkerErrorBoundary
);

export default exportsFromChunks.defaultExport;
export const __test = exportsFromChunks.__test;
export const __internalTestHooksForRepoTestsOnly = exportsFromChunks.__internalTestHooksForRepoTestsOnly;
