import { parse } from "csv-parse/sync";
import { createHash } from "node:crypto";
import AdmZip from "adm-zip";
import xlsx from "xlsx";

export type NormalizedRecord = {
  externalId: string;
  data: Record<string, string>;
  normalizedText: string;
};

function normalizeKey(key: string): string {
  return key.trim().toLowerCase().replace(/\s+/g, "_");
}

function normalizeValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }

  return String(value).trim();
}

function normalizeSearchText(record: Record<string, string>): string {
  return Object.values(record)
    .join(" ")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeRow(row: Record<string, unknown>): Record<string, string> {
  const normalized = Object.fromEntries(
    Object.entries(row).map(([key, value]) => [normalizeKey(key), normalizeValue(value)])
  );

  return normalizeGenderNationality(normalized);
}

function fingerprint(resourceId: string, data: Record<string, string>): string {
  const stableData = Object.keys(data)
    .sort()
    .reduce<Record<string, string>>((acc, key) => {
      acc[key] = data[key];
      return acc;
    }, {});

  return createHash("sha256").update(`${resourceId}:${JSON.stringify(stableData)}`).digest("hex");
}

function normalizeGenderValue(value: string) {
  return value.trim().toUpperCase();
}

function isGender(value: string) {
  return ["FEMENINO", "MASCULINO", "TRANS", "X"].includes(normalizeGenderValue(value));
}

function normalizeGenderNationality(data: Record<string, string>) {
  if (data.nacionalidad && data.genero && isGender(data.nacionalidad) && !isGender(data.genero)) {
    return {
      ...data,
      nacionalidad: data.genero,
      genero: data.nacionalidad
    };
  }

  return data;
}

function parseCsvRows(csvBody: Buffer): Record<string, unknown>[] {
  const csvText = csvBody.toString("utf8");
  const options = {
    columns: (headers: string[]) => headers.map(normalizeKey),
    bom: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_empty_lines: true,
    trim: true
  };

  try {
    return parse(csvText, options) as Record<string, unknown>[];
  } catch (error) {
    return parse(csvText, { ...options, skip_records_with_error: true }) as Record<string, unknown>[];
  }
}

export function parseAndNormalizeCsv(csvBody: Buffer, resourceId: string): NormalizedRecord[] {
  return normalizeRows(parseCsvRows(csvBody), resourceId);
}

export function parseAndNormalizeXlsx(body: Buffer, resourceId: string): NormalizedRecord[] {
  const workbook = xlsx.read(body, { type: "buffer", cellDates: false });
  const sheetName = workbook.SheetNames[0];

  if (!sheetName) {
    return [];
  }

  const rows = xlsx.utils.sheet_to_json<Record<string, unknown>>(workbook.Sheets[sheetName], {
    defval: ""
  });

  return normalizeRows(rows, resourceId);
}

function inferPeriod(value: string) {
  return value.match(/20[0-9]{4}/)?.[0] ?? "";
}

export function parseAndNormalizeZip(body: Buffer, resourceId: string): NormalizedRecord[] {
  const zip = new AdmZip(body);
  return zip.getEntries()
    .filter((entry) => !entry.isDirectory && entry.entryName.toLowerCase().endsWith(".csv"))
    .flatMap((entry) => {
      const period = inferPeriod(entry.entryName);
      const rows = parseCsvRows(entry.getData());

      return normalizeRows(rows, `${resourceId}:${entry.entryName}`, {
        archivo_origen: entry.entryName,
        periodo: period,
        anio: period.slice(0, 4)
      });
    });
}

function normalizeRows(rows: Record<string, unknown>[], resourceId: string, extraData: Record<string, string> = {}): NormalizedRecord[] {
  return rows.map((row) => {
    const data = { ...normalizeRow(row), ...extraData };

    return {
      externalId: `${resourceId}:${fingerprint(resourceId, data)}`,
      data,
      normalizedText: normalizeSearchText(data)
    };
  });
}
