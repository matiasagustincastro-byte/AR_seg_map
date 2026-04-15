import { pool } from "./pool.js";

const statements = [
  `CREATE EXTENSION IF NOT EXISTS pg_trgm`,
  `CREATE EXTENSION IF NOT EXISTS unaccent`,
  `CREATE TABLE IF NOT EXISTS source_files (
    id bigserial PRIMARY KEY,
    dataset_id text NOT NULL DEFAULT '',
    dataset_title text NOT NULL DEFAULT '',
    resource_id text NOT NULL DEFAULT '',
    resource_name text NOT NULL DEFAULT '',
    url text NOT NULL,
    sha256 text NOT NULL,
    etag text,
    last_modified timestamptz,
    downloaded_at timestamptz NOT NULL DEFAULT now(),
    row_count integer NOT NULL DEFAULT 0,
    UNIQUE (resource_id, sha256)
  )`,
  `CREATE TABLE IF NOT EXISTS sync_runs (
    id bigserial PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('running', 'skipped', 'success', 'failed')),
    reason text,
    source_file_id bigint REFERENCES source_files(id),
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz
  )`,
  `CREATE TABLE IF NOT EXISTS official_records (
    external_id text PRIMARY KEY,
    dataset_id text NOT NULL DEFAULT '',
    dataset_title text NOT NULL DEFAULT '',
    resource_id text NOT NULL DEFAULT '',
    resource_name text NOT NULL DEFAULT '',
    data jsonb NOT NULL,
    normalized_text text NOT NULL,
    source_file_id bigint NOT NULL REFERENCES source_files(id),
    updated_at timestamptz NOT NULL DEFAULT now()
  )`,
  `CREATE INDEX IF NOT EXISTS official_records_dataset_id_idx
    ON official_records (dataset_id)`,
  `CREATE INDEX IF NOT EXISTS official_records_resource_id_idx
    ON official_records (resource_id)`,
  `CREATE INDEX IF NOT EXISTS official_records_data_gin_idx
    ON official_records USING gin (data)`,
  `CREATE INDEX IF NOT EXISTS official_records_search_trgm_idx
    ON official_records USING gin (normalized_text gin_trgm_ops)`,
  `CREATE INDEX IF NOT EXISTS sync_runs_started_at_idx
    ON sync_runs (started_at DESC)`,
  `CREATE TABLE IF NOT EXISTS province_dimensions (
    province_id text PRIMARY KEY,
    province_name text NOT NULL,
    normalized_name text NOT NULL,
    region text NOT NULL,
    macroregion text NOT NULL,
    centroid_lat numeric NOT NULL,
    centroid_lon numeric NOT NULL
  )`,
  `CREATE TABLE IF NOT EXISTS province_population (
    province_id text NOT NULL REFERENCES province_dimensions(province_id),
    year integer NOT NULL,
    population_total integer NOT NULL,
    population_male integer,
    population_female integer,
    source text NOT NULL,
    source_url text,
    source_retrieved_at date,
    source_note text,
    PRIMARY KEY (province_id, year)
  )`,
  `ALTER TABLE province_population ADD COLUMN IF NOT EXISTS source_url text`,
  `ALTER TABLE province_population ADD COLUMN IF NOT EXISTS source_retrieved_at date`,
  `ALTER TABLE province_population ADD COLUMN IF NOT EXISTS source_note text`,
  `CREATE INDEX IF NOT EXISTS province_dimensions_normalized_name_idx
    ON province_dimensions (normalized_name)`,
  `CREATE OR REPLACE VIEW enriched_crime_records AS
    WITH normalized_records AS (
      SELECT
        r.*,
        CASE
          WHEN unaccent(lower(r.data->>'provincia_nombre')) IN ('caba', 'ciudad autonoma de buenos aires')
            THEN 'ciudad autonoma de buenos aires'
          WHEN unaccent(lower(r.data->>'provincia_nombre')) LIKE 'tierra del fuego%'
            THEN 'tierra del fuego'
          ELSE unaccent(lower(r.data->>'provincia_nombre'))
        END AS normalized_province_name
      FROM official_records r
    )
    SELECT
      r.external_id,
      r.dataset_id,
      r.dataset_title,
      r.resource_id,
      r.resource_name,
      r.data,
      r.updated_at,
      NULLIF(r.data->>'anio', '')::integer AS year,
      d.province_id,
      d.province_name,
      d.region,
      d.macroregion,
      d.centroid_lat,
      d.centroid_lon,
      p.population_total,
      NULLIF(replace(r.data->>'cantidad_hechos', ',', '.'), '')::numeric AS cantidad_hechos,
      NULLIF(replace(r.data->>'cantidad_victimas', ',', '.'), '')::numeric AS cantidad_victimas,
      CASE
        WHEN p.population_total > 0 AND r.data ? 'cantidad_hechos'
        THEN NULLIF(replace(r.data->>'cantidad_hechos', ',', '.'), '')::numeric / p.population_total * 100000
      END AS tasa_hechos_100k_calculada,
      CASE
        WHEN p.population_total > 0 AND r.data ? 'cantidad_victimas'
        THEN NULLIF(replace(r.data->>'cantidad_victimas', ',', '.'), '')::numeric / p.population_total * 100000
      END AS tasa_victimas_100k_calculada
    FROM normalized_records r
    LEFT JOIN province_dimensions d
      ON CASE
        WHEN d.normalized_name LIKE 'tierra del fuego%' THEN 'tierra del fuego'
        ELSE d.normalized_name
      END = r.normalized_province_name
    LEFT JOIN province_population p
      ON p.province_id = d.province_id
      AND p.year = 2022
    WHERE r.data ? 'provincia_nombre'`
];

async function migrate() {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const statement of statements) {
      await client.query(statement);
    }
    await client.query("COMMIT");
    console.log("Database migrated");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

migrate().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
