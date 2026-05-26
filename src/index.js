import {
  clampInt,
  json,
} from "./http-helpers.js";
import { diffDays, isIsoDate, isoDate, isoDateBerlin } from "./date-utils.js";
import {
  formatEventDistance,
  getLifeEventCategoryLabel,
  getLifeEventEffect,
  getTriathlonDistanceTargets,
  inferRaceDistanceLabel,
  inferTriathlonDistanceLabel,
  isARaceCategory,
  isARaceEvent,
  isLifeEventActiveOnDay,
  isLifeEventCategory,
  isTriathlonEvent,
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
import runtimeConfigAndHelpers from "./index-runtime/runtime-config-and-helpers.js";
import runtimeBlockLogicCore from "./index-runtime/runtime-block-logic-core.js";
import runtimeBlockLogicPolicies from "./index-runtime/runtime-block-logic-policies.js";
import runtimeSyncEngine from "./index-runtime/runtime-sync-engine.js";
import runtimeCommentaryAndAnalysis from "./index-runtime/runtime-commentary-and-analysis.js";
import runtimeEffectivenessAnalysis from "./index-runtime/runtime-effectiveness-analysis.js";
import runtimeRecoveryLearning from "./index-runtime/runtime-recovery-learning.js";
import runtimeIntegrationsAndHooks from "./index-runtime/runtime-integrations-and-hooks.js";
import runtimeVdotZones from "./index-runtime/runtime-vdot-zones.js";

const source = [
  runtimeConfigAndHelpers,
  runtimeVdotZones,
  runtimeBlockLogicCore,
  runtimeBlockLogicPolicies,
  runtimeSyncEngine,
  runtimeEffectivenessAnalysis,
  runtimeRecoveryLearning,
  runtimeCommentaryAndAnalysis,
  runtimeIntegrationsAndHooks,
].join("");

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
  "getTriathlonDistanceTargets",
  "inferRaceDistanceLabel",
  "inferTriathlonDistanceLabel",
  "isARaceCategory",
  "isARaceEvent",
  "isLifeEventActiveOnDay",
  "isLifeEventCategory",
  "isTriathlonEvent",
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

const exportsFromRuntime = runtime(
  clampInt,
  json,
  diffDays,
  isIsoDate,
  isoDate,
  isoDateBerlin,
  formatEventDistance,
  getLifeEventCategoryLabel,
  getLifeEventEffect,
  getTriathlonDistanceTargets,
  inferRaceDistanceLabel,
  inferTriathlonDistanceLabel,
  isARaceCategory,
  isARaceEvent,
  isLifeEventActiveOnDay,
  isLifeEventCategory,
  isTriathlonEvent,
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

export default exportsFromRuntime.defaultExport;
export const __test = exportsFromRuntime.__test;
export const __internalTestHooksForRepoTestsOnly = exportsFromRuntime.__internalTestHooksForRepoTestsOnly;
