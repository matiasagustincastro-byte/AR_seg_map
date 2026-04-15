import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(8010),
  DATABASE_URL: z.string().min(1),
  DATASET_IDS: z.string().min(1).default([
    "seguridad-snic---total-pais-estadisticas-criminales-republica-argentina",
    "seguridad-snic---provincial-estadisticas-criminales-republica-argentina-por-provincias",
    "seguridad-estadisticas-sobre-uso-racional-fuerza-empleo-armas-fuego-fuerzas-federales"
  ].join(",")),
  RESOURCE_FORMATS: z.string().min(1).default("CSV,XLSX"),
  SYNC_CRON: z.string().min(1).default("0 0 * * *"),
  EXPECTED_RESOURCE_SHA256: z.string().optional().default("")
});

const env = envSchema.parse(process.env);

function splitCsv(value: string): string[] {
  return value.split(",").map((item) => item.trim()).filter(Boolean);
}

function parseExpectedHashes(value: string): Map<string, string> {
  return new Map(
    splitCsv(value).map((entry) => {
      const [resourceId, hash] = entry.split("=").map((part) => part.trim());
      return [resourceId, hash] as const;
    }).filter(([resourceId, hash]) => resourceId && hash)
  );
}

export const config = {
  ...env,
  datasetIds: splitCsv(env.DATASET_IDS),
  resourceFormats: new Set(splitCsv(env.RESOURCE_FORMATS).map((format) => format.toUpperCase())),
  expectedResourceHashes: parseExpectedHashes(env.EXPECTED_RESOURCE_SHA256)
};
