import cron from "node-cron";
import { config } from "./config.js";
import { runCsvSync } from "./services/sync-service.js";

let isRunning = false;

export function startScheduler() {
  cron.schedule(config.SYNC_CRON, async () => {
    if (isRunning) {
      return;
    }

    isRunning = true;
    try {
      const result = await runCsvSync();
      console.log("Scheduled CSV sync finished", result);
    } catch (error) {
      console.error("Scheduled CSV sync failed", error);
    } finally {
      isRunning = false;
    }
  });
}
