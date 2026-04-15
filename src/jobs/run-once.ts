import { pool } from "../db/pool.js";
import { runCsvSync } from "../services/sync-service.js";

runCsvSync()
  .then((result) => {
    console.log(result);
  })
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
