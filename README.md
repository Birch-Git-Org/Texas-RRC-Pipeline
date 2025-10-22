# TXRRC Data Pipeline

This repository implements a lightweight data ingestion and normalization framework for key Texas Railroad Commission (TXRRC) public datasets.

## Requirements

* Python 3.11+
* SQLite 3.40+
* [uv](https://github.com/astral-sh/uv) for running commands

Install runtime dependencies with `uv` and invoke the CLI via:

```bash
uv run python -m txrrc --help
```

## Commands

* `uv run python -m txrrc discover` – scrape the TXRRC dataset catalog and store file metadata.
* `uv run python -m txrrc fetch --dataset-key <key> --url <url>` – download a specific file.
* `uv run python -m txrrc parse --dataset-key <key> --file-path <path>` – parse a raw file to structured rows.
* `uv run python -m txrrc load --dataset-key <key> --file-path <path>` – parse + load into SQLite.
* `uv run python -m txrrc refresh-all` – end-to-end discover → fetch → parse → load.
* `uv run python -m txrrc scheduler --interval 21600` – run refresh-all on a loop (default 6h).
* `uv run python -m txrrc api --port 8080` – serve a JSON status endpoint.

## Schema overview

The SQLite schema (see `txrrc/normalize/ddl.sql`) centers on these analysis-ready tables:

* `well` – canonical well identifiers and metadata
* `drilling_permit` – drilling permits, keyed by permit number
* `production_monthly` – monthly oil/gas volumes
* `organization` – P-5 organization records
* `field` – field reference table

A simplified schema diagram:

```
well(api14 PK, api10) --< production_monthly(api10, prod_month)
well(api10) --< drilling_permit(api10)
organization(org_number PK)
field(field_code PK)
```

### Example analysis query

```sql
SELECT w.county_code, SUM(p.oil_bbl) AS oil_bbl
FROM production_monthly p
JOIN well w ON w.api10 = p.api10
WHERE p.prod_month >= date('now','start of month','-24 months')
GROUP BY w.county_code
ORDER BY oil_bbl DESC;
```

## Testing

Run the unit tests via:

```bash
uv run pytest
```

The suite exercises HTML discovery, gzip-aware fetching, EBCDIC and fixed-width parsing, and idempotent loading.
