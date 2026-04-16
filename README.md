# CSV ingestion backend

Backend para automatizar este flujo:

1. Scheduler.
2. Descargar CSV oficial.
3. Validar hash y fecha.
4. Parsear y normalizar.
5. Upsert en PostgreSQL.
6. Indexar campos de busqueda.
7. Exponer API propia.

## Requisitos

- Node.js 22 o superior.
- Docker, si queres levantar PostgreSQL local con `docker compose`.

## Configuracion

```bash
copy .env.example .env
```

Edita `.env`:

- `DATASET_IDS`: datasets oficiales de datos.gob.ar a consultar por API CKAN.
- `JUSTICIA_DATASET_IDS`: datasets oficiales de datos.jus.gob.ar a consultar por API CKAN, incluyendo Internos del Servicio Penitenciario Federal - SPF.
- `RESOURCE_FORMATS`: formatos a ingerir. Queda en `CSV,XLSX` porque el dataset de uso racional de la fuerza publica XLSX.
- `EXPECTED_RESOURCE_SHA256`: opcional si el organismo publica hashes por recurso.
- `SYNC_CRON`: horario del job.

## Ejecutar

```bash
docker compose up --build
```

Forzar una sincronizacion manual:

```bash
docker compose exec app npm run ingest:once
```

## API

```http
GET /
GET /health
GET /datasets
GET /spf/facets
GET /spf/chart?metric=personas&groupBy=delito&genero=MASCULINO
GET /spf/map?metric=personas&situacion_procesal=CONDENADO
GET /records?q=texto&datasetId=seguridad_1&resourceId=seguridad_1.2&page=1&pageSize=25
GET /records/:externalId
GET /sync-runs?limit=20
POST /sync
```

`POST /sync` dispara el proceso completo en segundo plano y devuelve un `runId`.

La pantalla web queda disponible en:

```http
http://127.0.0.1:8011/#operacion
```
