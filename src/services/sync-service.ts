import { config } from "../config.js";
import { downloadFile } from "../csv/download.js";
import { parseAndNormalizeCsv, parseAndNormalizeXlsx } from "../csv/normalize.js";
import { pool } from "../db/pool.js";
import { upsertRecords } from "../repositories/records.js";
import { fetchDataset, selectResources, type DatosGobArResource } from "../sources/datos-gobar.js";

function parseResource(body: Buffer, resource: DatosGobArResource) {
  const format = resource.format.trim().toUpperCase();

  if (format === "CSV") {
    return parseAndNormalizeCsv(body, resource.id);
  }

  if (format === "XLSX") {
    return parseAndNormalizeXlsx(body, resource.id);
  }

  throw new Error(`Unsupported resource format ${resource.format} for ${resource.id}`);
}

export async function runCsvSync() {
  const client = await pool.connect();

  const runResult = await client.query<{ id: string }>(
    `INSERT INTO sync_runs (status) VALUES ('running') RETURNING id`
  );
  const runId = Number(runResult.rows[0].id);

  try {
    let syncedResources = 0;
    let skippedResources = 0;
    let failedResources = 0;
    let upsertedRecords = 0;
    const failures: string[] = [];

    for (const datasetRef of config.datasetIds) {
      let dataset;
      let resources;

      try {
        dataset = await fetchDataset(datasetRef);
        resources = selectResources(dataset, config.resourceFormats);
      } catch (error) {
        failedResources += 1;
        const reason = error instanceof Error ? error.message : "Unknown dataset sync error";
        failures.push(`${datasetRef}: ${reason}`);
        continue;
      }

      for (const resource of resources) {
        let transactionStarted = false;

        try {
          const downloaded = await downloadFile(resource.url);
          const expectedHash = config.expectedResourceHashes.get(resource.id) || resource.hash;

          if (expectedHash && downloaded.sha256 !== expectedHash) {
            throw new Error(`SHA256 mismatch for ${resource.id}. Expected ${expectedHash}, got ${downloaded.sha256}`);
          }

          const existing = await client.query(
            `SELECT id FROM source_files WHERE resource_id = $1 AND sha256 = $2`,
            [resource.id, downloaded.sha256]
          );

          if (existing.rowCount) {
            skippedResources += 1;
            continue;
          }

          const records = parseResource(downloaded.body, resource);

          await client.query("BEGIN");
          transactionStarted = true;
          const sourceFile = await client.query<{ id: string }>(
            `INSERT INTO source_files (
               dataset_id, dataset_title, resource_id, resource_name,
               url, sha256, etag, last_modified, row_count
             )
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             RETURNING id`,
            [
              dataset.id,
              dataset.title,
              resource.id,
              resource.name,
              resource.url,
              downloaded.sha256,
              downloaded.etag ?? null,
              downloaded.lastModified ?? (resource.last_modified ? new Date(resource.last_modified) : null),
              records.length
            ]
          );
          const sourceFileId = Number(sourceFile.rows[0].id);

          await upsertRecords(client, records, sourceFileId, {
            datasetId: dataset.id,
            datasetTitle: dataset.title,
            resourceId: resource.id,
            resourceName: resource.name
          });
          await client.query("COMMIT");

          syncedResources += 1;
          upsertedRecords += records.length;
        } catch (error) {
          if (transactionStarted) {
            await client.query("ROLLBACK").catch(() => undefined);
          }

          failedResources += 1;
          const reason = error instanceof Error ? error.message : "Unknown resource sync error";
          failures.push(`${resource.id}: ${reason}`);
        }
      }
    }

    const finalStatus = failedResources > 0
      ? "failed"
      : syncedResources === 0
        ? "skipped"
        : "success";
    const failureSummary = failures.length ? ` Failures: ${failures.slice(0, 3).join(" | ")}` : "";

    await client.query(
      `UPDATE sync_runs
       SET status = $1, reason = $2, finished_at = now(), source_file_id = $3
       WHERE id = $4`,
      [
        finalStatus,
        `Synced ${syncedResources} resources, skipped ${skippedResources}, failed ${failedResources}, upserted ${upsertedRecords} records.${failureSummary}`,
        null,
        runId
      ]
    );

    return {
      runId,
      status: finalStatus,
      syncedResources,
      skippedResources,
      failedResources,
      records: upsertedRecords
    };
  } catch (error) {
    await client.query("ROLLBACK").catch(() => undefined);
    const reason = error instanceof Error ? error.message : "Unknown sync error";
    await client.query(
      `UPDATE sync_runs SET status = 'failed', reason = $1, finished_at = now() WHERE id = $2`,
      [reason, runId]
    );
    throw error;
  } finally {
    client.release();
  }
}
