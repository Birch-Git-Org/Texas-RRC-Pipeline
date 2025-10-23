CREATE TABLE IF NOT EXISTS meta_file (
  file_url TEXT PRIMARY KEY,
  dataset_key TEXT NOT NULL,
  expected_format TEXT,
  discovered_at TEXT,
  last_checked_at TEXT,
  last_modified TEXT,
  etag TEXT,
  size_bytes INTEGER,
  status TEXT
);

CREATE TABLE IF NOT EXISTS raw_file (
  raw_file_id INTEGER PRIMARY KEY,
  dataset_key TEXT NOT NULL,
  file_url TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  stored_path TEXT NOT NULL,
  is_gzip INTEGER DEFAULT 0,
  downloaded_at TEXT NOT NULL,
  http_status INTEGER,
  content_type TEXT,
  error TEXT
);

CREATE TABLE IF NOT EXISTS dq_issue (
  issue_id INTEGER PRIMARY KEY,
  raw_file_id INTEGER,
  dataset_key TEXT,
  stage_table TEXT,
  row_identifier TEXT,
  error TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lineage_ingest (
  lineage_id INTEGER PRIMARY KEY,
  raw_file_id INTEGER,
  dataset_key TEXT NOT NULL,
  stage_table TEXT NOT NULL,
  loaded_tables TEXT NOT NULL,
  record_count INTEGER NOT NULL,
  error_count INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS well (
  api14 TEXT PRIMARY KEY,
  api10 TEXT NOT NULL,
  district TEXT,
  county_code TEXT,
  field_code TEXT,
  field_name TEXT,
  well_name TEXT,
  operator_number TEXT,
  operator_name TEXT,
  orig_completion_date DATE,
  total_depth INTEGER
);
CREATE INDEX IF NOT EXISTS idx_well_api10 ON well(api10);

CREATE TABLE IF NOT EXISTS organization (
  org_number TEXT PRIMARY KEY,
  name TEXT,
  status TEXT,
  address TEXT
);

CREATE TABLE IF NOT EXISTS drilling_permit (
  permit_number TEXT PRIMARY KEY,
  api10 TEXT,
  submit_date DATE,
  approve_date DATE,
  well_purpose TEXT,
  lateral_length INTEGER,
  is_horizontal INTEGER,
  surface_lat REAL,
  surface_lon REAL
);
CREATE INDEX IF NOT EXISTS idx_dp_api10 ON drilling_permit(api10);

CREATE TABLE IF NOT EXISTS production_monthly (
  api10 TEXT NOT NULL,
  prod_month DATE NOT NULL,
  oil_bbl INTEGER,
  gas_mcf INTEGER,
  water_bbl INTEGER,
  allowable_oil_bbl INTEGER,
  allowable_gas_mcf INTEGER,
  PRIMARY KEY (api10, prod_month)
);
CREATE INDEX IF NOT EXISTS idx_prod_api10_month ON production_monthly(api10, prod_month);

CREATE TABLE IF NOT EXISTS field (
  field_code TEXT PRIMARY KEY,
  field_name TEXT,
  district TEXT,
  oil_or_gas TEXT
);

CREATE TABLE IF NOT EXISTS completion_packet (
  tracking_number TEXT PRIMARY KEY,
  packet_number TEXT,
  district TEXT,
  status TEXT,
  submitted_date TEXT,
  forms_summary TEXT,
  attachments TEXT,
  payload_json TEXT
);
