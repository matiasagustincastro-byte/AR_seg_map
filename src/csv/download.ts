import { createHash } from "node:crypto";

export type DownloadedCsv = {
  body: Buffer;
  sha256: string;
  etag?: string;
  lastModified?: Date;
};

export async function downloadFile(url: string): Promise<DownloadedCsv> {
  const response = await fetch(url, {
    headers: {
      accept: "text/csv, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, */*;q=0.1"
    }
  });

  if (!response.ok) {
    throw new Error(`File download failed with HTTP ${response.status} for ${url}`);
  }

  const body = Buffer.from(await response.arrayBuffer());
  const sha256 = createHash("sha256").update(body).digest("hex");
  const lastModifiedHeader = response.headers.get("last-modified");

  return {
    body,
    sha256,
    etag: response.headers.get("etag") ?? undefined,
    lastModified: lastModifiedHeader ? new Date(lastModifiedHeader) : undefined
  };
}
