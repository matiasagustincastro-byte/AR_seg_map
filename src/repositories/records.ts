import type pg from "pg";
import type { NormalizedRecord } from "../csv/normalize.js";

export async function upsertRecords(
  client: pg.PoolClient,
  records: NormalizedRecord[],
  sourceFileId: number,
  metadata: {
    datasetId: string;
    datasetTitle: string;
    resourceId: string;
    resourceName: string;
  }
) {
  await client.query(`DELETE FROM official_records WHERE resource_id = $1`, [metadata.resourceId]);

  for (const record of records) {
    await client.query(
      `INSERT INTO official_records (
         external_id, dataset_id, dataset_title, resource_id, resource_name,
         data, normalized_text, source_file_id, updated_at
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
       ON CONFLICT (external_id) DO UPDATE SET
         dataset_id = EXCLUDED.dataset_id,
         dataset_title = EXCLUDED.dataset_title,
         resource_id = EXCLUDED.resource_id,
         resource_name = EXCLUDED.resource_name,
         data = EXCLUDED.data,
         normalized_text = EXCLUDED.normalized_text,
         source_file_id = EXCLUDED.source_file_id,
         updated_at = now()`,
      [
        record.externalId,
        metadata.datasetId,
        metadata.datasetTitle,
        metadata.resourceId,
        metadata.resourceName,
        record.data,
        record.normalizedText,
        sourceFileId
      ]
    );
  }
}

export async function listRecords(
  client: pg.Pool | pg.PoolClient,
  params: {
    q?: string;
    datasetId?: string;
    resourceId?: string;
    year?: string;
    province?: string;
    crime?: string;
    sortBy?: "anio" | "provincia_nombre" | "codigo_delito_snic_nombre" | "cantidad_hechos" | "cantidad_victimas" | "tasa_hechos";
    sortDir?: "asc" | "desc";
    page: number;
    pageSize: number;
  }
) {
  const offset = (params.page - 1) * params.pageSize;
  const values: unknown[] = [];
  const where: string[] = [];

  if (params.q) {
    values.push(`%${params.q.normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase()}%`);
    where.push(`normalized_text ILIKE $${values.length}`);
  }

  if (params.datasetId) {
    values.push(params.datasetId);
    where.push(`dataset_id = $${values.length}`);
  }

  if (params.resourceId) {
    values.push(params.resourceId);
    where.push(`resource_id = $${values.length}`);
  }

  if (params.year) {
    values.push(params.year);
    where.push(`data->>'anio' = $${values.length}`);
  }

  if (params.province) {
    values.push(params.province);
    where.push(`data->>'provincia_nombre' = $${values.length}`);
  }

  if (params.crime) {
    values.push(params.crime);
    where.push(`data->>'codigo_delito_snic_nombre' = $${values.length}`);
  }

  values.push(params.pageSize, offset);
  const limitParam = values.length - 1;
  const offsetParam = values.length;
  const sortBy = params.sortBy ?? "anio";
  const sortDir = params.sortDir ?? "desc";
  const numericSorts = new Set(["cantidad_hechos", "cantidad_victimas", "tasa_hechos"]);
  const orderExpr = numericSorts.has(sortBy)
    ? `NULLIF(replace(data->>'${sortBy}', ',', '.'), '')::numeric ${sortDir.toUpperCase()} NULLS LAST`
    : `data->>'${sortBy}' ${sortDir.toUpperCase()} NULLS LAST`;

  const dataQuery = client.query(
    `SELECT external_id, dataset_id, dataset_title, resource_id, resource_name, data, updated_at
     FROM official_records
     ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
     ORDER BY ${orderExpr}, external_id ASC
     LIMIT $${limitParam} OFFSET $${offsetParam}`,
    values
  );
  const countQuery = client.query<{ total: string }>(
    `SELECT count(*) AS total
     FROM official_records
     ${where.length ? `WHERE ${where.join(" AND ")}` : ""}`,
    values.slice(0, -2)
  );
  const [dataResult, countResult] = await Promise.all([dataQuery, countQuery]);

  return {
    data: dataResult.rows,
    total: Number(countResult.rows[0]?.total ?? 0)
  };
}

export async function getRecordByExternalId(client: pg.Pool, externalId: string) {
  const { rows } = await client.query(
    `SELECT external_id, dataset_id, dataset_title, resource_id, resource_name, data, updated_at
     FROM official_records
     WHERE external_id = $1`,
    [externalId]
  );

  return rows[0] ?? null;
}
