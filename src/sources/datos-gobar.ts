export type DatosGobArDataset = {
  id: string;
  name: string;
  title: string;
  resources: DatosGobArResource[];
};

export type DatosGobArResource = {
  id: string;
  name: string;
  format: string;
  url: string;
  last_modified?: string;
  hash?: string;
};

type PackageShowResponse = {
  success: boolean;
  result: DatosGobArDataset;
};

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function fetchDataset(datasetIdOrSlug: string): Promise<DatosGobArDataset> {
  const url = new URL("https://datos.gob.ar/api/3/action/package_show");
  url.searchParams.set("id", datasetIdOrSlug);

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    const response = await fetch(url);

    if (response.ok) {
      const payload = await response.json() as PackageShowResponse;

      if (!payload.success) {
        throw new Error(`datos.gob.ar package_show failed for ${datasetIdOrSlug}`);
      }

      return payload.result;
    }

    if (attempt === 3) {
      throw new Error(`datos.gob.ar package_show failed with HTTP ${response.status}`);
    }

    await wait(attempt * 800);
  }

  throw new Error(`datos.gob.ar package_show failed for ${datasetIdOrSlug}`);
}

export function selectResources(dataset: DatosGobArDataset, formats: Set<string>): DatosGobArResource[] {
  return dataset.resources.filter((resource) => {
    const format = resource.format?.trim().toUpperCase();
    return Boolean(resource.url) && formats.has(format);
  });
}
