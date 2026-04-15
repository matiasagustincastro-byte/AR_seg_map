import { parse } from "csv-parse/sync";
import { createHash } from "node:crypto";
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
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [normalizeKey(key), normalizeValue(value)])
  );
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

export function parseAndNormalizeCsv(csvBody: Buffer, resourceId: string): NormalizedRecord[] {
  const rows = parse(csvBody.toString("utf8"), {
    columns: (headers: string[]) => headers.map(normalizeKey),
    bom: true,
    skip_empty_lines: true,
    trim: true
  }) as Record<string, unknown>[];

  return normalizeRows(rows, resourceId);
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

function normalizeRows(rows: Record<string, unknown>[], resourceId: string): NormalizedRecord[] {
  return rows.map((row) => {
    const data = normalizeRow(row);

    return {
      externalId: `${resourceId}:${fingerprint(resourceId, data)}`,
      data,
      normalizedText: normalizeSearchText(data)
    };
  });
}
