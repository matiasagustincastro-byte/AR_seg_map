import { pool } from "../db/pool.js";
import { population2022, provinceDimensions } from "./province-data.js";

async function seedEnrichment() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    for (const province of provinceDimensions) {
      await client.query(
        `INSERT INTO province_dimensions (
           province_id, province_name, normalized_name, region, macroregion, centroid_lat, centroid_lon
         )
         VALUES ($1, $2, unaccent(lower($2)), $3, $4, $5, $6)
         ON CONFLICT (province_id) DO UPDATE SET
           province_name = EXCLUDED.province_name,
           normalized_name = EXCLUDED.normalized_name,
           region = EXCLUDED.region,
           macroregion = EXCLUDED.macroregion,
           centroid_lat = EXCLUDED.centroid_lat,
           centroid_lon = EXCLUDED.centroid_lon`,
        [
          province.provinceId,
          province.provinceName,
          province.region,
          province.macroregion,
          province.centroidLat,
          province.centroidLon
        ]
      );
    }

    for (const population of population2022) {
      await client.query(
        `INSERT INTO province_population (province_id, year, population_total, source, source_url, source_retrieved_at, source_note)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (province_id, year) DO UPDATE SET
           population_total = EXCLUDED.population_total,
           source = EXCLUDED.source,
           source_url = EXCLUDED.source_url,
           source_retrieved_at = EXCLUDED.source_retrieved_at,
           source_note = EXCLUDED.source_note`,
        [
          population.provinceId,
          population.year,
          population.populationTotal,
          population.source,
          population.sourceUrl,
          population.sourceRetrievedAt,
          population.sourceNote
        ]
      );
    }

    await client.query("COMMIT");
    console.log("Enrichment seed loaded");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

seedEnrichment().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
