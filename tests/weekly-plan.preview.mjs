import { selectWeeklyPlan, shouldSelectBaseKeyByQuota } from "../src/index.js";

const contexts = [
  {
    id: "5k_BUILD_40d",
    distance: "5k",
    phase: "BUILD",
    dayIso: "2026-03-10",
    daysToRace: 40,
    runfloorGap: false,
    deloadActive: false,
    lastKeyDate: "2026-03-06",
    lastKeyTypes: ["vo2_touch"],
    history: [{ typeKey: "vo2_touch", workload: 24 }],
  },
  {
    id: "10k_RACE_18d",
    distance: "10k",
    phase: "RACE",
    dayIso: "2026-05-05",
    daysToRace: 18,
    runfloorGap: false,
    deloadActive: false,
    lastKeyDate: "2026-04-30",
    lastKeyTypes: ["racepace"],
    history: [{ typeKey: "racepace", workload: 30 }],
  },
  {
    id: "hm_RACE_9d",
    distance: "hm",
    phase: "RACE",
    dayIso: "2026-09-01",
    daysToRace: 9,
    runfloorGap: false,
    deloadActive: false,
    lastKeyDate: "2026-08-29",
    lastKeyTypes: ["racepace"],
    history: [{ typeKey: "racepace", workload: 40 }],
  },
  {
    id: "m_BUILD_40d_runfloor",
    distance: "m",
    phase: "BUILD",
    dayIso: "2026-02-12",
    daysToRace: 40,
    runfloorGap: true,
    deloadActive: false,
    lastKeyDate: "2026-02-10",
    lastKeyTypes: ["racepace"],
    history: [{ typeKey: "racepace", workload: 60 }],
  },
  {
    id: "10k_BUILD_18d_deload",
    distance: "10k",
    phase: "BUILD",
    dayIso: "2026-06-02",
    daysToRace: 18,
    runfloorGap: false,
    deloadActive: true,
    lastKeyDate: "2026-05-29",
    lastKeyTypes: ["schwelle"],
    history: [{ typeKey: "schwelle", workload: 32 }],
  },
  {
    id: "5k_RACE_9d_signals",
    distance: "5k",
    phase: "RACE",
    dayIso: "2026-07-07",
    daysToRace: 9,
    runfloorGap: false,
    deloadActive: false,
    driftWarning: true,
    negativeSignals: ["sleep_low"],
    lastKeyDate: "2026-07-04",
    lastKeyTypes: ["racepace"],
    history: [{ typeKey: "racepace", workload: 20 }],
  },
];

for (const context of contexts) {
  const result = selectWeeklyPlan(context);
  console.log(`\n=== ${context.id} ===`);
  console.log(JSON.stringify({
    selected: result.selected.map((w) => ({ id: w.id, name: w.name, typeKey: w.typeKey, isKey: w.isKey, source: w.source })),
    rationale: result.rationale,
    taperApplied: result.taperApplied,
    deloadApplied: result.deloadApplied,
    runfloorBlocked: result.runfloorBlocked,
  }, null, 2));
}

console.log("\n=== BASE quota deterministic check (0.5) ===");
const weeks = ["2026-01-06", "2026-01-13", "2026-01-20", "2026-01-27"];
for (const dayIso of weeks) {
  console.log(`${dayIso}: ${shouldSelectBaseKeyByQuota(0.5, dayIso)}`);
}
