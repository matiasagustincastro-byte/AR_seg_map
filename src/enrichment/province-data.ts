export type ProvinceDimensionSeed = {
  provinceId: string;
  provinceName: string;
  region: string;
  macroregion: string;
  centroidLat: number;
  centroidLon: number;
};

export type ProvincePopulationSeed = {
  provinceId: string;
  year: number;
  populationTotal: number;
  source: string;
  sourceUrl: string;
  sourceRetrievedAt: string;
  sourceNote: string;
};

const populationSource = {
  source: "INDEC Censo Nacional de Población, Hogares y Viviendas 2022",
  sourceUrl: "https://www.argentina.gob.ar/pais/poblacion",
  sourceRetrievedAt: "2026-04-14",
  sourceNote: "Población por jurisdicción usada como denominador fijo para tasas propias cada 100.000 habitantes."
};

export const provinceDimensions: ProvinceDimensionSeed[] = [
  { provinceId: "02", provinceName: "Ciudad Autónoma de Buenos Aires", region: "CABA", macroregion: "AMBA", centroidLat: -34.6144, centroidLon: -58.4459 },
  { provinceId: "06", provinceName: "Buenos Aires", region: "Pampeana", macroregion: "AMBA/Pampeana", centroidLat: -36.6769, centroidLon: -60.5588 },
  { provinceId: "10", provinceName: "Catamarca", region: "NOA", macroregion: "Norte Grande", centroidLat: -27.3358, centroidLon: -66.9477 },
  { provinceId: "14", provinceName: "Córdoba", region: "Centro", macroregion: "Centro", centroidLat: -32.1429, centroidLon: -63.8018 },
  { provinceId: "18", provinceName: "Corrientes", region: "NEA", macroregion: "Norte Grande", centroidLat: -28.7743, centroidLon: -57.8012 },
  { provinceId: "22", provinceName: "Chaco", region: "NEA", macroregion: "Norte Grande", centroidLat: -26.3864, centroidLon: -60.7658 },
  { provinceId: "26", provinceName: "Chubut", region: "Patagonia", macroregion: "Patagonia", centroidLat: -43.7886, centroidLon: -68.5268 },
  { provinceId: "30", provinceName: "Entre Ríos", region: "Litoral", macroregion: "Centro/Litoral", centroidLat: -32.0589, centroidLon: -59.2014 },
  { provinceId: "34", provinceName: "Formosa", region: "NEA", macroregion: "Norte Grande", centroidLat: -24.8949, centroidLon: -59.9324 },
  { provinceId: "38", provinceName: "Jujuy", region: "NOA", macroregion: "Norte Grande", centroidLat: -23.3201, centroidLon: -65.7643 },
  { provinceId: "42", provinceName: "La Pampa", region: "Pampeana", macroregion: "Centro", centroidLat: -37.1316, centroidLon: -65.4467 },
  { provinceId: "46", provinceName: "La Rioja", region: "Cuyo/NOA", macroregion: "Cuyo", centroidLat: -29.6858, centroidLon: -67.1817 },
  { provinceId: "50", provinceName: "Mendoza", region: "Cuyo", macroregion: "Cuyo", centroidLat: -34.6299, centroidLon: -68.5831 },
  { provinceId: "54", provinceName: "Misiones", region: "NEA", macroregion: "Norte Grande", centroidLat: -26.8754, centroidLon: -54.6517 },
  { provinceId: "58", provinceName: "Neuquén", region: "Patagonia", macroregion: "Patagonia", centroidLat: -38.6419, centroidLon: -70.1199 },
  { provinceId: "62", provinceName: "Río Negro", region: "Patagonia", macroregion: "Patagonia", centroidLat: -40.4058, centroidLon: -67.2293 },
  { provinceId: "66", provinceName: "Salta", region: "NOA", macroregion: "Norte Grande", centroidLat: -24.2991, centroidLon: -64.8142 },
  { provinceId: "70", provinceName: "San Juan", region: "Cuyo", macroregion: "Cuyo", centroidLat: -30.8654, centroidLon: -68.8895 },
  { provinceId: "74", provinceName: "San Luis", region: "Cuyo", macroregion: "Cuyo", centroidLat: -33.7577, centroidLon: -66.0281 },
  { provinceId: "78", provinceName: "Santa Cruz", region: "Patagonia", macroregion: "Patagonia", centroidLat: -48.8155, centroidLon: -69.9558 },
  { provinceId: "82", provinceName: "Santa Fe", region: "Centro/Litoral", macroregion: "Centro/Litoral", centroidLat: -30.7069, centroidLon: -60.9498 },
  { provinceId: "86", provinceName: "Santiago del Estero", region: "NOA", macroregion: "Norte Grande", centroidLat: -27.7824, centroidLon: -63.2524 },
  { provinceId: "90", provinceName: "Tucumán", region: "NOA", macroregion: "Norte Grande", centroidLat: -26.9478, centroidLon: -65.3648 },
  { provinceId: "94", provinceName: "Tierra del Fuego", region: "Patagonia", macroregion: "Patagonia", centroidLat: -82.5215, centroidLon: -50.7427 }
];

export const population2022: ProvincePopulationSeed[] = [
  { provinceId: "02", year: 2022, populationTotal: 3120612, ...populationSource },
  { provinceId: "06", year: 2022, populationTotal: 17569053, ...populationSource },
  { provinceId: "10", year: 2022, populationTotal: 429556, ...populationSource },
  { provinceId: "14", year: 2022, populationTotal: 3978984, ...populationSource },
  { provinceId: "18", year: 2022, populationTotal: 1197553, ...populationSource },
  { provinceId: "22", year: 2022, populationTotal: 1142963, ...populationSource },
  { provinceId: "26", year: 2022, populationTotal: 603120, ...populationSource },
  { provinceId: "30", year: 2022, populationTotal: 1426426, ...populationSource },
  { provinceId: "34", year: 2022, populationTotal: 606041, ...populationSource },
  { provinceId: "38", year: 2022, populationTotal: 797955, ...populationSource },
  { provinceId: "42", year: 2022, populationTotal: 366022, ...populationSource },
  { provinceId: "46", year: 2022, populationTotal: 384607, ...populationSource },
  { provinceId: "50", year: 2022, populationTotal: 2014533, ...populationSource },
  { provinceId: "54", year: 2022, populationTotal: 1280960, ...populationSource },
  { provinceId: "58", year: 2022, populationTotal: 726590, ...populationSource },
  { provinceId: "62", year: 2022, populationTotal: 762067, ...populationSource },
  { provinceId: "66", year: 2022, populationTotal: 1440672, ...populationSource },
  { provinceId: "70", year: 2022, populationTotal: 818234, ...populationSource },
  { provinceId: "74", year: 2022, populationTotal: 540905, ...populationSource },
  { provinceId: "78", year: 2022, populationTotal: 337226, ...populationSource },
  { provinceId: "82", year: 2022, populationTotal: 3544908, ...populationSource },
  { provinceId: "86", year: 2022, populationTotal: 1054028, ...populationSource },
  { provinceId: "90", year: 2022, populationTotal: 1703186, ...populationSource },
  { provinceId: "94", year: 2022, populationTotal: 185732, ...populationSource }
];
