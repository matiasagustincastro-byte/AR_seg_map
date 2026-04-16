import type { FastifyInstance, FastifyReply } from "fastify";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import { config } from "../config.js";
import { pool } from "../db/pool.js";
import { getRecordByExternalId, listRecords } from "../repositories/records.js";
import { runCsvSync } from "../services/sync-service.js";

const projectRoot = dirname(dirname(dirname(fileURLToPath(import.meta.url))));
const publicRoot = join(projectRoot, "public");
const metricSchema = z.enum(["cantidad_hechos", "cantidad_victimas", "tasa_hechos", "tasa_victimas", "tasa_hechos_100k_calculada", "tasa_victimas_100k_calculada"]);

type Metric = z.infer<typeof metricSchema>;

const metricLabels: Record<Metric, string> = {
  cantidad_hechos: "cantidad de hechos",
  cantidad_victimas: "cantidad de victimas",
  tasa_hechos: "tasa de hechos",
  tasa_victimas: "tasa de victimas",
  tasa_hechos_100k_calculada: "tasa propia de hechos 100k",
  tasa_victimas_100k_calculada: "tasa propia de victimas 100k"
};

const spfDatasetId = "6d0e08b3-041c-40c1-9ea6-962db3747677";
const spfFieldSchema = z.enum([
  "unidad",
  "situacion_procesal",
  "delito",
  "edad",
  "nacionalidad",
  "genero",
  "jurisdiccion",
  "estado_civil",
  "profesion",
  "subgrupo",
  "provincia_nacimiento",
  "unidad_provincia",
  "tipo_pena"
]);
const spfMetricSchema = z.enum(["personas", "edad_promedio", "pena_anios_promedio"]);

type SpfField = z.infer<typeof spfFieldSchema>;
type SpfMetric = z.infer<typeof spfMetricSchema>;

const spfFields: { id: SpfField; label: string }[] = [
  { id: "unidad", label: "Unidad" },
  { id: "situacion_procesal", label: "Situacion procesal" },
  { id: "delito", label: "Delito" },
  { id: "edad", label: "Edad" },
  { id: "nacionalidad", label: "Nacionalidad" },
  { id: "genero", label: "Genero" },
  { id: "jurisdiccion", label: "Jurisdiccion" },
  { id: "estado_civil", label: "Estado civil" },
  { id: "profesion", label: "Profesion" },
  { id: "subgrupo", label: "Subgrupo" },
  { id: "provincia_nacimiento", label: "Provincia de nacimiento" },
  { id: "unidad_provincia", label: "Provincia de alojamiento" },
  { id: "tipo_pena", label: "Tipo de pena" }
];

const spfMetricLabels: Record<SpfMetric, string> = {
  personas: "personas alojadas",
  edad_promedio: "edad promedio",
  pena_anios_promedio: "años de pena promedio"
};

type JournalistCategory = {
  id: string;
  label: string;
  description: string;
  crimes?: string[];
  patterns: string[];
};

const journalistCategories: JournalistCategory[] = [
  {
    id: "homicidios",
    label: "Homicidios y muertes",
    description: "Homicidios, muertes viales y suicidios informados por la taxonomía SNIC.",
    crimes: [
      "Homicidios culposos por otros hechos",
      "Homicidios dolosos",
      "Homicidios dolosos en grado de tentativa",
      "Muertes en accidentes viales",
      "Suicidios (consumados)"
    ],
    patterns: ["%homicid%", "%muerte%", "%suicidio%"]
  },
  {
    id: "lesiones",
    label: "Lesiones",
    description: "Lesiones dolosas y culposas registradas en los delitos normalizados.",
    crimes: [
      "Lesiones culposas en Accidentes Viales",
      "Lesiones culposas por otros hechos",
      "Lesiones dolosas"
    ],
    patterns: ["%lesion%"]
  },
  {
    id: "robos",
    label: "Robos",
    description: "Robos y tentativas de robo, incluidos agravados cuando aparecen en SNIC.",
    crimes: [
      "Robos (excluye los agravados por el resultado de lesiones y/o muertes)",
      "Robos agravados por el resultado de lesiones y/o muertes",
      "Tentativas de robo (excluye las agravadas por el res. de lesiones y/o muerte)",
      "Tentativas de robo agravado por el resultado de lesiones y/o muertes"
    ],
    patterns: ["%robo%"]
  },
  {
    id: "hurtos",
    label: "Hurtos",
    description: "Hurtos y tentativas de hurto registrados por provincia y año.",
    crimes: [
      "Hurtos",
      "Tentativas de hurto"
    ],
    patterns: ["%hurto%"]
  },
  {
    id: "integridad_sexual",
    label: "Integridad sexual",
    description: "Abusos sexuales, violaciones, tentativas y otros delitos contra la integridad sexual.",
    crimes: [
      "Abuso sexual agravado",
      "Abuso sexual simple",
      "Abusos sexuales con acceso carnal (violaciones)",
      "Otros delitos contra la integridad sexual",
      "Tentativa de abuso sexual con acceso carnal"
    ],
    patterns: ["%abuso sexual%", "%sex%", "%violacion%"]
  },
  {
    id: "propiedad_economica",
    label: "Propiedad y economía",
    description: "Daños, estafas, defraudaciones, extorsiones y otros delitos contra la propiedad.",
    crimes: [
      "Daños (no incluye informáticos)",
      "Estafas y defraudaciones (no incluye virtuales) y usura",
      "Extorsiones",
      "Otros delitos contra la propiedad"
    ],
    patterns: ["%propiedad%", "%daño%", "%estafa%", "%defraud%", "%extorsion%", "%usura%"]
  },
  {
    id: "armas",
    label: "Armas y explosivos",
    description: "Tenencia, portación, acopio, entrega de armas y materiales peligrosos.",
    crimes: [
      "Acopio y fabricación ilegal de armas piezas y municiones",
      "Contrabando de elementos nucleares agresivos químicos armas y municiones",
      "Entrega y comercialización ilegal de armas de fuego",
      "Fabricación adquisición transferencia y tenencia de explosivos y otros materiales peligrosos",
      "Omisión adulteración y supresión de marcaje",
      "Portación ilegal de armas de fuego",
      "Tenencia ilegal de armas de fuego"
    ],
    patterns: ["%arma%", "%explosivo%", "%municion%"]
  },
  {
    id: "estupefacientes",
    label: "Estupefacientes",
    description: "Ley 23.737 y figuras vinculadas a estupefacientes.",
    crimes: [
      "Comercialización y entrega de estupefacientes",
      "Confabulación de estupefacientes",
      "Contrabando de estupefacientes",
      "Desvío de Importación de estupefacientes",
      "Ley 23.737 (estupefacientes)",
      "Organización y financiación de estupefacientes",
      "Otros delitos previstos en la ley 23.737",
      "Siembra y producción de estupefacientes",
      "Tenencia o entrega atenuada de estupefacientes",
      "Tenencia simple atenuada para uso personal de estupefacientes",
      "Tenencia simple de estupefacientes"
    ],
    patterns: ["%estupefaciente%", "%ley 23.737%"]
  },
  {
    id: "estado_leyes_especiales",
    label: "Estado y leyes especiales",
    description: "Delitos contra el Estado, administración pública, contrabando, migratorios y leyes especiales.",
    crimes: [
      "Contrabando Agravado",
      "Contrabando Simple",
      "Contrabando agravado",
      "Contrabando simple",
      "Contravenciones",
      "Delitos contra el estado civil",
      "Delitos contra el orden económico y financiero",
      "Delitos contra el orden público",
      "Delitos contra la administración pública",
      "Delitos contra la fe pública",
      "Delitos contra la seguridad de la nación",
      "Delitos contra los poderes públicos y el orden constitucional",
      "Delitos migratorios",
      "Delitos previstos en otras leyes",
      "Ley de fauna",
      "Ley de residuos peligrosos",
      "Obstrucción del código aduanero",
      "Otros delitos previstos en leyes especiales",
      "Otros delitos previstos en leyes especiales n.c.p"
    ],
    patterns: ["%estado%", "%administracion publica%", "%orden publico%", "%poderes publicos%", "%fe publica%", "%nacion%", "%contrabando%", "%migratorio%", "%leyes especiales%", "%otras leyes%", "%contravencion%", "%aduanero%"]
  },
  {
    id: "ciberdelitos",
    label: "Ciberdelitos",
    description: "Delitos informáticos y delitos asistidos virtualmente cuando están disponibles.",
    crimes: [
      "Acceso ilegal a sistemas informáticos y daños informáticos",
      "Ciberdelitos sexuales vinculados a menores",
      "Estafas y defraudaciones asistidas virtualmente"
    ],
    patterns: ["%ciber%", "%informatico%", "%virtual%"]
  }
];

function categoryDefinition(category?: string) {
  if (!category) {
    return null;
  }

  return journalistCategories.find((item) => item.id === category) ?? null;
}

function normalizedCrimeSql(isCalculatedMetric: boolean) {
  return isCalculatedMetric
    ? `lower(unaccent(coalesce(crime_name, '')))`
    : `lower(unaccent(coalesce(data->>'codigo_delito_snic_nombre', '')))`;
}

function addCategoryFilter(where: string[], values: unknown[], category?: string, isCalculatedMetric = false) {
  const definition = categoryDefinition(category);

  if (!definition) {
    return null;
  }

  const crimeSql = normalizedCrimeSql(isCalculatedMetric);
  if (definition.crimes?.length) {
    values.push(definition.crimes);
    where.push(`${isCalculatedMetric ? "crime_name" : "data->>'codigo_delito_snic_nombre'"} = ANY($${values.length})`);
    return definition;
  }

  const clauses = definition.patterns.map((pattern) => {
    values.push(pattern);
    return `${crimeSql} LIKE lower(unaccent($${values.length}))`;
  });
  where.push(`(${clauses.join(" OR ")})`);
  return definition;
}

async function getJournalistCategories() {
  return Promise.all(journalistCategories.map(async (category) => {
    const values: unknown[] = [];
    const where = [
      `data ? 'codigo_delito_snic_nombre'`,
      `data->>'codigo_delito_snic_nombre' <> ''`,
      `data ? 'anio'`,
      `data ? 'provincia_nombre'`
    ];
    addCategoryFilter(where, values, category.id);
    const { rows } = await pool.query(
      `SELECT
         count(*)::int AS records,
         count(DISTINCT data->>'codigo_delito_snic_nombre')::int AS crime_count,
         array_agg(DISTINCT data->>'codigo_delito_snic_nombre' ORDER BY data->>'codigo_delito_snic_nombre') AS crimes
       FROM official_records
       WHERE ${where.join(" AND ")}`,
      values
    );
    const row = rows[0] ?? {};

    return {
      id: category.id,
      label: category.label,
      description: category.description,
      records: Number(row.records || 0),
      crimeCount: Number(row.crime_count || 0),
      crimes: row.crimes ?? []
    };
  })).then((items) => items.filter((item) => item.records > 0));
}

function metricExpression(metric: Metric, isCalculatedMetric: boolean) {
  return isCalculatedMetric ? metric : `replace(data->>'${metric}', ',', '.')::numeric`;
}

function yearExpression(isCalculatedMetric: boolean) {
  return isCalculatedMetric ? "year" : "(data->>'anio')::integer";
}

function baseMetricWhere(metric: Metric, isCalculatedMetric: boolean) {
  return isCalculatedMetric
    ? [`year IS NOT NULL`, `${metric} IS NOT NULL`]
    : [
      `data ? 'anio'`,
      `data ? '${metric}'`,
      `data->>'anio' ~ '^[0-9]{4}$'`,
      `data->>'${metric}' ~ '^-?[0-9]+([.,][0-9]+)?$'`
    ];
}

function addCommonFilters(
  where: string[],
  values: unknown[],
  query: { year?: string; startYear?: string; endYear?: string; province?: string; provinces?: string; crime?: string },
  isCalculatedMetric: boolean
) {
  const yearSql = yearExpression(isCalculatedMetric);

  if (query.year) {
    values.push(query.year);
    where.push(`${yearSql} = $${values.length}::integer`);
  }

  if (query.startYear) {
    values.push(query.startYear);
    where.push(`${yearSql} >= $${values.length}::integer`);
  }

  if (query.endYear) {
    values.push(query.endYear);
    where.push(`${yearSql} <= $${values.length}::integer`);
  }

  if (query.province) {
    values.push(query.province);
    where.push(isCalculatedMetric ? `province_name = $${values.length}` : `data->>'provincia_nombre' = $${values.length}`);
  }

  if (query.provinces) {
    const provinces = query.provinces.split(",").map((province) => province.trim()).filter(Boolean);
    if (provinces.length) {
      values.push(provinces);
      where.push(isCalculatedMetric ? `province_name = ANY($${values.length})` : `data->>'provincia_nombre' = ANY($${values.length})`);
    }
  }

  if (query.crime) {
    values.push(query.crime);
    where.push(`data->>'codigo_delito_snic_nombre' = $${values.length}`);
  }
}

function spfPeriodExpression() {
  return `substring(resource_name from '(20[0-9]{4})')`;
}

function spfProvinceExpression() {
  return `CASE
    WHEN data->>'unidad_provincia' = 'Ciudad Autónoma de Bs.As.' THEN 'Ciudad Autónoma de Buenos Aires'
    WHEN data->>'unidad_provincia' LIKE 'Tierra del Fuego%' THEN 'Tierra del Fuego'
    ELSE data->>'unidad_provincia'
  END`;
}

function spfMetricExpression(metric: SpfMetric) {
  if (metric === "edad_promedio") {
    return `avg(NULLIF(data->>'edad', '')::numeric)`;
  }

  if (metric === "pena_anios_promedio") {
    return `avg(NULLIF(data->>'anios_pena', '')::numeric)`;
  }

  return `count(*)`;
}

function spfBaseWhere(metric: SpfMetric) {
  const where = [`dataset_id = $1`];

  if (metric === "edad_promedio") {
    where.push(`data->>'edad' ~ '^[0-9]+([.,][0-9]+)?$'`);
  }

  if (metric === "pena_anios_promedio") {
    where.push(`data->>'anios_pena' ~ '^[0-9]+([.,][0-9]+)?$'`);
  }

  return where;
}

function addSpfFilters(
  where: string[],
  values: unknown[],
  query: Partial<Record<SpfField | "period" | "resourceId", string>>
) {
  if (query.period) {
    values.push(query.period);
    where.push(`${spfPeriodExpression()} = $${values.length}`);
  }

  if (query.resourceId) {
    values.push(query.resourceId);
    where.push(`resource_id = $${values.length}`);
  }

  for (const field of spfFields) {
    const value = query[field.id];
    if (!value) {
      continue;
    }

    const items = value.split(",").map((item) => item.trim()).filter(Boolean);
    if (!items.length) {
      continue;
    }

    values.push(items);
    where.push(`data->>'${field.id}' = ANY($${values.length})`);
  }
}

function pctChange(previous: number, current: number) {
  if (!Number.isFinite(previous) || previous === 0) {
    return null;
  }

  return ((current - previous) / Math.abs(previous)) * 100;
}

function toPct(value: unknown) {
  return `${Math.abs(Number(value || 0)).toLocaleString("es-AR", { maximumFractionDigits: 1 })}%`;
}

function toNumber(value: unknown) {
  return Number(value || 0).toLocaleString("es-AR", { maximumFractionDigits: 2 });
}

const listQuerySchema = z.object({
  q: z.string().optional(),
  datasetId: z.string().optional(),
  resourceId: z.string().optional(),
  year: z.string().optional(),
  province: z.string().optional(),
  crime: z.string().optional(),
  sortBy: z.enum(["anio", "provincia_nombre", "codigo_delito_snic_nombre", "cantidad_hechos", "cantidad_victimas", "tasa_hechos"]).default("anio"),
  sortDir: z.enum(["asc", "desc"]).default("desc"),
  page: z.coerce.number().int().min(1).default(1),
  pageSize: z.coerce.number().int().min(1).max(100).default(25)
});

async function sendPublicFile(reply: FastifyReply, fileName: string) {
  const allowedFiles: Record<string, string> = {
    "index.html": "text/html; charset=utf-8",
    "app.css": "text/css; charset=utf-8",
    "app.js": "application/javascript; charset=utf-8",
    "argentina-provinces.geojson": "application/geo+json; charset=utf-8"
  };
  const contentType = allowedFiles[fileName];

  if (!contentType) {
    return reply.code(404).send({ message: "File not found" });
  }

  const body = await readFile(join(publicRoot, fileName), "utf8");
  return reply
    .header("cache-control", "no-store")
    .type(contentType)
    .send(body);
}

export async function registerRoutes(app: FastifyInstance) {
  app.get("/", async (_request, reply) => sendPublicFile(reply, "index.html"));

  app.get("/assets/:fileName", async (request, reply) => {
    const params = z.object({ fileName: z.enum(["app.css", "app.js", "argentina-provinces.geojson"]) }).parse(request.params);
    return sendPublicFile(reply, params.fileName);
  });

  app.get("/health", async () => ({ ok: true }));

  app.get("/datasets", async () => {
    const { rows } = await pool.query(
      `SELECT
         dataset_id,
         dataset_title,
         resource_id,
         resource_name,
         max(downloaded_at) AS last_downloaded_at,
         max(row_count) AS last_row_count
       FROM source_files
       GROUP BY dataset_id, dataset_title, resource_id, resource_name
       ORDER BY dataset_title ASC, resource_name ASC`
    );
    return { data: rows };
  });

  app.get("/stats", async () => {
    const { rows } = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM official_records) AS records,
         (SELECT count(DISTINCT dataset_id)::int FROM official_records) AS datasets,
         (SELECT count(DISTINCT resource_id)::int FROM official_records) AS resources,
         (SELECT max(updated_at) FROM official_records) AS last_record_update,
         (SELECT max(finished_at) FROM sync_runs WHERE status = 'success') AS last_success_sync,
         $1::text AS sync_cron,
         24::int AS sync_interval_hours`,
      [config.SYNC_CRON]
    );

    return rows[0];
  });

  app.get("/facets", async () => {
    const [years, provinces, crimes, journalistCategoryOptions] = await Promise.all([
      pool.query(
        `SELECT DISTINCT data->>'anio' AS value
         FROM official_records
         WHERE data ? 'anio' AND data->>'anio' <> ''
         ORDER BY value DESC`
      ),
      pool.query(
        `SELECT DISTINCT data->>'provincia_nombre' AS value
         FROM official_records
         WHERE data ? 'provincia_nombre' AND data->>'provincia_nombre' <> ''
         ORDER BY value ASC`
      ),
      pool.query(
        `SELECT DISTINCT data->>'codigo_delito_snic_nombre' AS value
         FROM official_records
         WHERE data ? 'codigo_delito_snic_nombre' AND data->>'codigo_delito_snic_nombre' <> ''
         ORDER BY value ASC`
      ),
      getJournalistCategories()
    ]);

    return {
      years: years.rows.map((row) => row.value),
      provinces: provinces.rows.map((row) => row.value),
      crimes: crimes.rows.map((row) => row.value),
      journalistCategories: journalistCategoryOptions
    };
  });

  app.get("/spf/facets", async () => {
    const [periods, resources, ...fieldResults] = await Promise.all([
      pool.query(
        `SELECT DISTINCT ${spfPeriodExpression()} AS value
         FROM official_records
         WHERE dataset_id = $1 AND ${spfPeriodExpression()} IS NOT NULL
         ORDER BY value DESC`,
        [spfDatasetId]
      ),
      pool.query(
        `SELECT DISTINCT resource_id, resource_name
         FROM official_records
         WHERE dataset_id = $1
         ORDER BY resource_name ASC`,
        [spfDatasetId]
      ),
      ...spfFields.map((field) => pool.query(
        `SELECT DISTINCT data->>'${field.id}' AS value
         FROM official_records
         WHERE dataset_id = $1
           AND data ? '${field.id}'
           AND data->>'${field.id}' <> ''
         ORDER BY value ASC
         LIMIT 300`,
        [spfDatasetId]
      ))
    ]);

    return {
      fields: spfFields,
      metrics: Object.entries(spfMetricLabels).map(([id, label]) => ({ id, label })),
      periods: periods.rows.map((row) => row.value),
      resources: resources.rows,
      values: Object.fromEntries(spfFields.map((field, index) => [
        field.id,
        fieldResults[index].rows.map((row) => row.value)
      ]))
    };
  });

  app.get("/spf/chart", async (request) => {
    const query = z.object({
      metric: spfMetricSchema.default("personas"),
      groupBy: z.union([spfFieldSchema, z.literal("period")]).default("unidad_provincia"),
      period: z.string().optional(),
      resourceId: z.string().optional(),
      unidad: z.string().optional(),
      situacion_procesal: z.string().optional(),
      delito: z.string().optional(),
      edad: z.string().optional(),
      nacionalidad: z.string().optional(),
      genero: z.string().optional(),
      jurisdiccion: z.string().optional(),
      estado_civil: z.string().optional(),
      profesion: z.string().optional(),
      subgrupo: z.string().optional(),
      provincia_nacimiento: z.string().optional(),
      unidad_provincia: z.string().optional(),
      tipo_pena: z.string().optional(),
      limit: z.coerce.number().int().min(1).max(40).default(15)
    }).parse(request.query);
    const values: unknown[] = [spfDatasetId];
    const where = spfBaseWhere(query.metric);
    addSpfFilters(where, values, query);

    const groupExpression = query.groupBy === "period"
      ? spfPeriodExpression()
      : `data->>'${query.groupBy}'`;
    values.push(query.limit);

    const { rows } = await pool.query(
      `SELECT
         coalesce(nullif(${groupExpression}, ''), 'Sin dato') AS label,
         ${spfMetricExpression(query.metric)}::float AS value
       FROM official_records
       WHERE ${where.join(" AND ")}
       GROUP BY 1
       ORDER BY value DESC NULLS LAST
       LIMIT $${values.length}`,
      values
    );

    return { data: rows, label: `${spfMetricLabels[query.metric]} por ${query.groupBy}` };
  });

  app.get("/spf/map", async (request) => {
    const query = z.object({
      metric: spfMetricSchema.default("personas"),
      period: z.string().optional(),
      resourceId: z.string().optional(),
      situacion_procesal: z.string().optional(),
      delito: z.string().optional(),
      genero: z.string().optional(),
      jurisdiccion: z.string().optional()
    }).parse(request.query);
    const values: unknown[] = [spfDatasetId];
    const where = spfBaseWhere(query.metric);
    where.push(`data ? 'unidad_provincia'`, `data->>'unidad_provincia' <> ''`);
    addSpfFilters(where, values, query);

    const { rows } = await pool.query(
      `SELECT
         ${spfProvinceExpression()} AS province,
         ${spfMetricExpression(query.metric)}::float AS value
       FROM official_records
       WHERE ${where.join(" AND ")}
       GROUP BY 1
       ORDER BY value DESC NULLS LAST`,
      values
    );

    return { data: rows };
  });

  app.get("/journalist/categories", async () => {
    const categories = await getJournalistCategories();
    return { data: categories };
  });

  app.get("/enrichment/provinces", async () => {
    const { rows } = await pool.query(
      `SELECT
         d.province_id,
         d.province_name,
         d.region,
         d.macroregion,
         d.centroid_lat,
         d.centroid_lon,
         p.year AS population_year,
         p.population_total,
         p.source AS population_source,
         p.source_url AS population_source_url,
         p.source_retrieved_at AS population_source_retrieved_at,
         p.source_note AS population_source_note
       FROM province_dimensions d
       LEFT JOIN province_population p ON p.province_id = d.province_id
       ORDER BY d.province_name ASC`
    );

    return { data: rows };
  });

  app.get("/chart", async (request) => {
    const query = z.object({
      metric: metricSchema.default("cantidad_hechos"),
      compareBy: z.enum(["provincia_nombre", "codigo_delito_snic_nombre", "anio"]).default("provincia_nombre"),
      year: z.string().optional(),
      startYear: z.string().optional(),
      endYear: z.string().optional(),
      province: z.string().optional(),
      provinces: z.string().optional(),
      crime: z.string().optional()
    }).parse(request.query);

    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const chartYearExpression = isCalculatedMetric ? "year::text" : "data->>'anio'";
    const seriesExpression = query.compareBy === "anio"
      ? "'Total'"
      : isCalculatedMetric && query.compareBy === "provincia_nombre"
        ? "province_name"
        : `data->>'${query.compareBy}'`;
    const valueExpression = metricExpression(query.metric, isCalculatedMetric);
    const values: unknown[] = [];
    const where = baseMetricWhere(query.metric, isCalculatedMetric);

    if (query.compareBy !== "anio") {
      where.push(isCalculatedMetric && query.compareBy === "provincia_nombre"
        ? "province_name IS NOT NULL"
        : `data ? '${query.compareBy}'`);
    }

    addCommonFilters(where, values, query, isCalculatedMetric);

    const { rows } = await pool.query(
      `SELECT
         ${chartYearExpression} AS year,
         ${seriesExpression} AS series,
         sum(${valueExpression})::float AS value
       FROM ${isCalculatedMetric ? "enriched_crime_records" : "official_records"}
       WHERE ${where.join(" AND ")}
       GROUP BY 1, 2
       ORDER BY year ASC, value DESC`,
      values
    );

    return { data: rows };
  });

  app.get("/map", async (request) => {
    const query = z.object({
      metric: metricSchema.default("cantidad_hechos"),
      year: z.string().optional(),
      crime: z.string().optional()
    }).parse(request.query);

    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const values: unknown[] = [];
    const where = isCalculatedMetric
      ? [`${query.metric} IS NOT NULL`]
      : [
        `data ? 'provincia_nombre'`,
        `data ? $1`,
        `data->>'provincia_nombre' <> ''`,
        `data->>$1 ~ '^-?[0-9]+([.,][0-9]+)?$'`
      ];

    if (!isCalculatedMetric) {
      values.push(query.metric);
    }

    if (query.year) {
      values.push(query.year);
      where.push(isCalculatedMetric ? `year = $${values.length}::integer` : `data->>'anio' = $${values.length}`);
    }

    if (query.crime) {
      values.push(query.crime);
      where.push(`data->>'codigo_delito_snic_nombre' = $${values.length}`);
    }

    const { rows } = await pool.query(
      `SELECT
         ${isCalculatedMetric ? "province_name" : "data->>'provincia_nombre'"} AS province,
         sum(${isCalculatedMetric ? query.metric : "replace(data->>$1, ',', '.')::numeric"})::float AS value
       FROM ${isCalculatedMetric ? "enriched_crime_records" : "official_records"}
       WHERE ${where.join(" AND ")}
       GROUP BY ${isCalculatedMetric ? "province_name" : "data->>'provincia_nombre'"}
       ORDER BY value DESC`,
      values
    );

    return { data: rows };
  });

  app.get("/rankings", async (request) => {
    const query = z.object({
      type: z.enum(["province_top", "crime_growth", "crime_drop"]).default("province_top"),
      metric: metricSchema.default("cantidad_hechos"),
      year: z.string().optional(),
      startYear: z.string().optional(),
      endYear: z.string().optional(),
      crime: z.string().optional(),
      limit: z.coerce.number().int().min(1).max(25).default(10)
    }).parse(request.query);
    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const values: unknown[] = [];
    const from = isCalculatedMetric ? "enriched_crime_records" : "official_records";
    const valueExpression = metricExpression(query.metric, isCalculatedMetric);
    const rowYearExpression = yearExpression(isCalculatedMetric);

    if (query.type === "province_top") {
      const where = baseMetricWhere(query.metric, isCalculatedMetric);
      addCommonFilters(where, values, query, isCalculatedMetric);
      values.push(String(query.limit));
      const { rows } = await pool.query(
        `SELECT
           ${isCalculatedMetric ? "province_name" : "data->>'provincia_nombre'"} AS name,
           sum(${valueExpression})::float AS value
         FROM ${from}
         WHERE ${where.join(" AND ")}
           AND ${isCalculatedMetric ? "province_name IS NOT NULL" : "data ? 'provincia_nombre'"}
         GROUP BY 1
         ORDER BY value DESC
         LIMIT $${values.length}`,
        values
      );
      return { data: rows, label: `Top provincias por ${metricLabels[query.metric]}` };
    }

    const startYear = Number(query.startYear || 2020);
    const endYear = Number(query.endYear || query.year || 2024);
    values.push(String(startYear), String(endYear));
    const where = baseMetricWhere(query.metric, isCalculatedMetric);
    where.push(`${rowYearExpression} IN ($1::integer, $2::integer)`);
    if (query.crime) {
      values.push(query.crime);
      where.push(`data->>'codigo_delito_snic_nombre' = $${values.length}`);
    }
    values.push(String(query.limit));

    const { rows } = await pool.query(
      `WITH totals AS (
         SELECT
           data->>'codigo_delito_snic_nombre' AS name,
           ${rowYearExpression} AS year,
           sum(${valueExpression})::float AS value
         FROM ${from}
         WHERE ${where.join(" AND ")}
           AND data ? 'codigo_delito_snic_nombre'
         GROUP BY 1, 2
       ),
       paired AS (
         SELECT
           name,
           max(value) FILTER (WHERE year = $1::integer) AS start_value,
           max(value) FILTER (WHERE year = $2::integer) AS end_value
         FROM totals
         GROUP BY name
       )
       SELECT
         name,
         start_value,
         end_value,
         CASE WHEN start_value IS NULL OR start_value = 0 THEN NULL ELSE ((end_value - start_value) / abs(start_value)) * 100 END AS change_pct
       FROM paired
       WHERE start_value IS NOT NULL AND end_value IS NOT NULL
       ORDER BY change_pct ${query.type === "crime_drop" ? "ASC" : "DESC"} NULLS LAST
       LIMIT $${values.length}`,
      values
    );
    return { data: rows, label: query.type === "crime_drop" ? "Delitos que mas bajaron" : "Delitos que mas crecieron" };
  });

  app.get("/alerts", async (request) => {
    const query = z.object({
      metric: metricSchema.default("cantidad_hechos"),
      year: z.string().optional(),
      crime: z.string().optional(),
      threshold: z.coerce.number().min(1).max(500).default(25),
      limit: z.coerce.number().int().min(1).max(20).default(8)
    }).parse(request.query);
    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const currentYear = Number(query.year || 2024);
    const previousYear = currentYear - 1;
    const values: unknown[] = [String(previousYear), String(currentYear)];
    const where = baseMetricWhere(query.metric, isCalculatedMetric);
    const rowYearExpression = yearExpression(isCalculatedMetric);
    where.push(`${rowYearExpression} IN ($1::integer, $2::integer)`);
    if (query.crime) {
      values.push(query.crime);
      where.push(`data->>'codigo_delito_snic_nombre' = $${values.length}`);
    }
    values.push(String(query.threshold), String(query.limit));

    const { rows } = await pool.query(
      `WITH totals AS (
         SELECT
           ${isCalculatedMetric ? "province_name" : "data->>'provincia_nombre'"} AS province,
           ${rowYearExpression} AS year,
           sum(${metricExpression(query.metric, isCalculatedMetric)})::float AS value
         FROM ${isCalculatedMetric ? "enriched_crime_records" : "official_records"}
         WHERE ${where.join(" AND ")}
           AND ${isCalculatedMetric ? "province_name IS NOT NULL" : "data ? 'provincia_nombre'"}
         GROUP BY 1, 2
       ),
       paired AS (
         SELECT
           province,
           max(value) FILTER (WHERE year = $1::integer) AS previous_value,
           max(value) FILTER (WHERE year = $2::integer) AS current_value
         FROM totals
         GROUP BY province
       )
       SELECT
         province,
         previous_value,
         current_value,
         ((current_value - previous_value) / abs(previous_value)) * 100 AS change_pct,
         CASE WHEN current_value > previous_value THEN 'suba' ELSE 'baja' END AS direction
       FROM paired
       WHERE previous_value IS NOT NULL
         AND previous_value <> 0
         AND current_value IS NOT NULL
         AND abs(((current_value - previous_value) / abs(previous_value)) * 100) >= $${values.length - 1}::numeric
       ORDER BY abs(((current_value - previous_value) / abs(previous_value)) * 100) DESC
       LIMIT $${values.length}`,
      values
    );

    return {
      data: rows.map((row) => ({
        ...row,
        message: `${row.direction === "suba" ? "Aumentó" : "Bajó"} ${metricLabels[query.metric]} ${Math.abs(Number(row.change_pct)).toFixed(1)}% en ${row.province}`
      }))
    };
  });

  app.get("/trend", async (request) => {
    const query = z.object({
      metric: metricSchema.default("cantidad_hechos"),
      province: z.string().optional(),
      crime: z.string().optional()
    }).parse(request.query);
    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const values: unknown[] = [];
    const where = baseMetricWhere(query.metric, isCalculatedMetric);
    addCommonFilters(where, values, query, isCalculatedMetric);

    const { rows } = await pool.query(
      `SELECT
         ${yearExpression(isCalculatedMetric)} AS year,
         sum(${metricExpression(query.metric, isCalculatedMetric)})::float AS value
       FROM ${isCalculatedMetric ? "enriched_crime_records" : "official_records"}
       WHERE ${where.join(" AND ")}
       GROUP BY 1
       ORDER BY year ASC`,
      values
    );
    const points = rows.map((row) => ({ year: Number(row.year), value: Number(row.value || 0) }));
    const withMovingAverage = points.map((point, index) => {
      const window = points.slice(Math.max(0, index - 2), index + 1);
      const movingAverage = window.reduce((sum, item) => sum + item.value, 0) / Math.max(window.length, 1);
      const change = index > 0 ? pctChange(points[index - 1].value, point.value) : null;
      return { ...point, movingAverage, changePct: change };
    });
    const last = points.at(-1);
    const previous = points.at(-2);
    const forecast = last && previous
      ? { year: last.year + 1, value: Math.max(0, last.value + (last.value - previous.value)), method: "proyeccion lineal simple" }
      : null;
    const anomalies = withMovingAverage
      .filter((point) => point.changePct !== null && Math.abs(point.changePct) >= 30)
      .map((point) => ({ year: point.year, value: point.value, changePct: point.changePct }));

    return { data: withMovingAverage, forecast, anomalies };
  });

  app.get("/journalist/findings", async (request) => {
    const query = z.object({
      metric: metricSchema.default("cantidad_hechos"),
      year: z.string().optional(),
      category: z.string().optional(),
      crime: z.string().optional(),
      limit: z.coerce.number().int().min(3).max(18).default(12)
    }).parse(request.query);
    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const from = isCalculatedMetric ? "enriched_crime_records" : "official_records";
    const rowYearExpression = yearExpression(isCalculatedMetric);
    const provinceExpression = isCalculatedMetric ? "province_name" : "data->>'provincia_nombre'";
    const values: unknown[] = [];
    let currentYear = Number(query.year);
    const selectedCategory = categoryDefinition(query.category);

    if (!currentYear) {
      const latestValues: unknown[] = [];
      const latestWhere = baseMetricWhere(query.metric, isCalculatedMetric);
      addCategoryFilter(latestWhere, latestValues, query.category, isCalculatedMetric);
      if (query.crime) {
        latestValues.push(query.crime);
        latestWhere.push(`data->>'codigo_delito_snic_nombre' = $${latestValues.length}`);
      }
      const latestYear = await pool.query(
        `SELECT max(${rowYearExpression})::int AS year
         FROM ${from}
         WHERE ${latestWhere.join(" AND ")}`,
        latestValues
      );
      currentYear = Number(latestYear.rows[0]?.year || new Date().getFullYear());
    }

    const previousYear = currentYear - 1;
    const commonWhere = baseMetricWhere(query.metric, isCalculatedMetric);
    addCategoryFilter(commonWhere, values, query.category, isCalculatedMetric);
    if (query.crime) {
      values.push(query.crime);
      commonWhere.push(`data->>'codigo_delito_snic_nombre' = $${values.length}`);
    }

    const sourceInfo = await pool.query(
      `SELECT
         max(sf.downloaded_at) AS last_downloaded_at,
         string_agg(DISTINCT sf.dataset_title, ' | ') AS datasets,
         string_agg(DISTINCT sf.resource_name, ' | ') AS resources
       FROM source_files sf`
    );
    const populationSource = await pool.query(
      `SELECT source, source_url, source_retrieved_at, source_note
       FROM province_population
       WHERE year = 2022
       LIMIT 1`
    );
    const evidenceBase = {
      officialSource: "datos.gob.ar / Ministerio de Seguridad",
      datasets: sourceInfo.rows[0]?.datasets,
      resources: sourceInfo.rows[0]?.resources,
      lastDownloadedAt: sourceInfo.rows[0]?.last_downloaded_at,
      enrichment: isCalculatedMetric ? populationSource.rows[0] : null,
      metric: metricLabels[query.metric],
      year: currentYear,
      category: selectedCategory ? selectedCategory.label : "Todas",
      categoryId: selectedCategory?.id ?? null,
      crime: query.crime || "Todos"
    };

    const changes = await pool.query(
      `WITH totals AS (
         SELECT
           ${provinceExpression} AS province,
           ${rowYearExpression} AS year,
           sum(${metricExpression(query.metric, isCalculatedMetric)})::float AS value
         FROM ${from}
         WHERE ${commonWhere.join(" AND ")}
           AND ${provinceExpression} IS NOT NULL
           AND ${provinceExpression} <> ''
           AND ${provinceExpression} <> 'Total pais'
           AND ${rowYearExpression} IN ($${values.length + 1}::integer, $${values.length + 2}::integer)
         GROUP BY 1, 2
       ),
       paired AS (
         SELECT
           province,
           max(value) FILTER (WHERE year = $${values.length + 1}::integer) AS previous_value,
           max(value) FILTER (WHERE year = $${values.length + 2}::integer) AS current_value
         FROM totals
         GROUP BY province
       )
       SELECT
         province,
         previous_value,
         current_value,
         ((current_value - previous_value) / abs(previous_value)) * 100 AS change_pct
       FROM paired
       WHERE previous_value IS NOT NULL AND previous_value <> 0 AND current_value IS NOT NULL
       ORDER BY abs(((current_value - previous_value) / abs(previous_value)) * 100) DESC
       LIMIT $${values.length + 3}`,
      [...values, String(previousYear), String(currentYear), String(query.limit)]
    );

    const records = await pool.query(
      `WITH yearly AS (
         SELECT
           ${provinceExpression} AS province,
           ${rowYearExpression} AS year,
           sum(${metricExpression(query.metric, isCalculatedMetric)})::float AS value
         FROM ${from}
         WHERE ${commonWhere.join(" AND ")}
           AND ${provinceExpression} IS NOT NULL
           AND ${provinceExpression} <> ''
           AND ${provinceExpression} <> 'Total pais'
         GROUP BY 1, 2
       ),
       current_year AS (
         SELECT * FROM yearly WHERE year = $${values.length + 1}::integer
       ),
       history AS (
         SELECT province, max(value) AS historical_max
         FROM yearly
         GROUP BY province
       )
       SELECT c.province, c.year, c.value, h.historical_max
       FROM current_year c
       JOIN history h ON h.province = c.province
       WHERE c.value = h.historical_max
       ORDER BY c.value DESC
       LIMIT $${values.length + 2}`,
      [...values, String(currentYear), String(Math.max(3, Math.floor(query.limit / 2)))]
    );

    const top = await pool.query(
      `SELECT
         ${provinceExpression} AS province,
         sum(${metricExpression(query.metric, isCalculatedMetric)})::float AS value
       FROM ${from}
       WHERE ${commonWhere.join(" AND ")}
         AND ${provinceExpression} IS NOT NULL
         AND ${provinceExpression} <> ''
         AND ${provinceExpression} <> 'Total pais'
         AND ${rowYearExpression} = $${values.length + 1}::integer
       GROUP BY 1
       ORDER BY value DESC
       LIMIT 3`,
      [...values, String(currentYear)]
    );

    const findings = [
      ...changes.rows.map((row) => {
        const direction = Number(row.change_pct) >= 0 ? "suba" : "baja";
        const title = `${row.province}: ${direction === "suba" ? "subió" : "bajó"} ${toPct(row.change_pct)} en ${metricLabels[query.metric]}`;
        const paragraph = `Según datos oficiales, ${metricLabels[query.metric]} en ${row.province} ${direction === "suba" ? "aumentó" : "bajó"} ${toPct(row.change_pct)} entre ${previousYear} y ${currentYear}, al pasar de ${toNumber(row.previous_value)} a ${toNumber(row.current_value)}.`;
        return {
          type: direction === "suba" ? "suba_interanual" : "baja_interanual",
          title,
          lead: paragraph,
          province: row.province,
          value: row.current_value,
          previousValue: row.previous_value,
          changePct: row.change_pct,
          paragraph,
          evidence: evidenceBase
        };
      }),
      ...records.rows.map((row) => {
        const paragraph = `${row.province} registró en ${currentYear} su máximo de la serie para ${metricLabels[query.metric]}, con ${toNumber(row.value)}.`;
        return {
          type: "maximo_historico",
          title: `${row.province}: máximo histórico en ${currentYear}`,
          lead: paragraph,
          province: row.province,
          value: row.value,
          paragraph,
          evidence: evidenceBase
        };
      }),
      ...top.rows.map((row, index) => {
        const paragraph = `${row.province} ocupa el puesto ${index + 1} del ranking provincial de ${metricLabels[query.metric]} en ${currentYear}, con ${toNumber(row.value)}.`;
        return {
          type: "ranking",
          title: `${row.province}: puesto ${index + 1} nacional`,
          lead: paragraph,
          province: row.province,
          value: row.value,
          paragraph,
          evidence: evidenceBase
        };
      })
    ].slice(0, query.limit);

    return { data: findings, evidence: evidenceBase };
  });

  app.get("/report", async (request, reply) => {
    const query = z.object({
      title: z.string().optional(),
      metric: metricSchema.default("cantidad_hechos"),
      province: z.string().optional(),
      provinces: z.string().optional(),
      category: z.string().optional(),
      crime: z.string().optional(),
      year: z.string().optional(),
      startYear: z.string().optional(),
      endYear: z.string().optional()
    }).parse(request.query);
    const isCalculatedMetric = query.metric.endsWith("_calculada");
    const values: unknown[] = [];
    const where = baseMetricWhere(query.metric, isCalculatedMetric);
    const reportQuery = { ...query };
    if (!reportQuery.year && !reportQuery.startYear && !reportQuery.endYear) {
      const latestYear = await pool.query(
        `SELECT max(${yearExpression(isCalculatedMetric)})::text AS year
         FROM ${isCalculatedMetric ? "enriched_crime_records" : "official_records"}
         WHERE ${baseMetricWhere(query.metric, isCalculatedMetric).join(" AND ")}`
      );
      reportQuery.year = latestYear.rows[0]?.year;
    }
    addCategoryFilter(where, values, reportQuery.category, isCalculatedMetric);
    addCommonFilters(where, values, reportQuery, isCalculatedMetric);
    const { rows } = await pool.query(
      `SELECT
         ${yearExpression(isCalculatedMetric)} AS year,
         ${isCalculatedMetric ? "province_name" : "data->>'provincia_nombre'"} AS province,
         sum(${metricExpression(query.metric, isCalculatedMetric)})::float AS value
       FROM ${isCalculatedMetric ? "enriched_crime_records" : "official_records"}
       WHERE ${where.join(" AND ")}
         AND ${isCalculatedMetric ? "province_name IS NOT NULL" : "data ? 'provincia_nombre'"}
       GROUP BY 1, 2
       ORDER BY year ASC, value DESC
       LIMIT 120`,
      values
    );
    const title = query.title || `Informe ${query.province || query.provinces || "nacional"} ${reportQuery.year || [query.startYear, query.endYear].filter(Boolean).join("-") || ""}`.trim();
    const total = rows.reduce((sum, row) => sum + Number(row.value || 0), 0);
    const html = `<!doctype html>
      <html lang="es">
        <head>
          <meta charset="utf-8" />
          <title>${title}</title>
          <style>
            body { font-family: Arial, sans-serif; color: #10201c; margin: 32px; }
            h1 { margin-bottom: 4px; }
            .muted { color: #5b6b66; }
            table { width: 100%; border-collapse: collapse; margin-top: 24px; }
            th, td { border-bottom: 1px solid #d8e0dc; padding: 9px; text-align: left; }
            th { background: #eef7f3; }
            .summary { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-top: 20px; }
            .card { border: 1px solid #d8e0dc; border-radius: 8px; padding: 12px; }
            @media print { button { display: none; } }
          </style>
        </head>
        <body>
          <button onclick="window.print()">Generar PDF</button>
          <h1>${title}</h1>
          <p class="muted">Mapa Argentino del delito · ${metricLabels[query.metric]}</p>
          <div class="summary">
            <div class="card"><strong>${rows.length}</strong><br />filas agregadas</div>
            <div class="card"><strong>${Number(total).toLocaleString("es-AR", { maximumFractionDigits: 2 })}</strong><br />total</div>
            <div class="card"><strong>${new Date().toLocaleDateString("es-AR")}</strong><br />fecha de informe</div>
          </div>
          <table>
            <thead><tr><th>Año</th><th>Provincia</th><th>Valor</th></tr></thead>
            <tbody>${rows.map((row) => `<tr><td>${row.year}</td><td>${row.province}</td><td>${Number(row.value || 0).toLocaleString("es-AR", { maximumFractionDigits: 2 })}</td></tr>`).join("")}</tbody>
          </table>
        </body>
      </html>`;
    return reply.type("text/html; charset=utf-8").send(html);
  });

  app.get("/records", async (request) => {
    const query = listQuerySchema.parse(request.query);
    const result = await listRecords(pool, query);
    return {
      page: query.page,
      pageSize: query.pageSize,
      total: result.total,
      data: result.data
    };
  });

  app.get("/records/:externalId", async (request, reply) => {
    const params = z.object({ externalId: z.string().min(1) }).parse(request.params);
    const record = await getRecordByExternalId(pool, params.externalId);

    if (!record) {
      return reply.code(404).send({ message: "Record not found" });
    }

    return record;
  });

  app.get("/sync-runs", async (request) => {
    const query = z.object({ limit: z.coerce.number().int().min(1).max(100).default(20) }).parse(request.query);
    const { rows } = await pool.query(
      `SELECT id, status, reason, started_at, finished_at, source_file_id
       FROM sync_runs
       ORDER BY started_at DESC
       LIMIT $1`,
      [query.limit]
    );
    return { data: rows };
  });

  app.post("/sync", async (request, reply) => {
    runCsvSync().catch((error) => request.log.error(error, "Manual CSV sync failed"));
    return reply.code(202).send({ accepted: true });
  });
}
