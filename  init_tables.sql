-- Run while connected to plantvillage_db (psql -U postgres -d plantvillage_db -f sql/create_schema_and_tables.sql)

-- create schema (replace CURRENT_USER with a particular user if you like)
CREATE SCHEMA IF NOT EXISTS gold AUTHORIZATION CURRENT_USER;

-- bronze metadata
CREATE TABLE IF NOT EXISTS gold.bronze_image_metadata (
  image_id BIGINT PRIMARY KEY,
  path_str TEXT,
  filename TEXT,
  folder_name TEXT,
  file_size_kb NUMERIC,
  extract_ts TIMESTAMP
);

-- silver (cleaned)
CREATE TABLE IF NOT EXISTS gold.silver_images (
  image_id BIGINT PRIMARY KEY,
  path_str TEXT,
  filename TEXT,
  folder_name TEXT,
  crop_type TEXT,
  disease_class TEXT,
  is_healthy BOOLEAN,
  severity_level TEXT,
  crop_family TEXT,
  size_category TEXT,
  file_size_kb NUMERIC,
  process_ts TIMESTAMP
);

-- dimensions
CREATE TABLE IF NOT EXISTS gold.dim_disease (
  disease_key INTEGER PRIMARY KEY,
  disease_id TEXT,
  disease_class TEXT,
  is_healthy BOOLEAN,
  severity_level TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_crop (
  crop_key INTEGER PRIMARY KEY,
  crop_id TEXT,
  crop_type TEXT,
  crop_family TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_size (
  size_key INTEGER PRIMARY KEY,
  size_id TEXT,
  size_category TEXT
);

-- fact
CREATE TABLE IF NOT EXISTS gold.fact_image (
  image_id BIGINT PRIMARY KEY,
  disease_key INTEGER,
  crop_key INTEGER,
  size_key INTEGER,
  file_size_kb NUMERIC,
  processing_ts TIMESTAMP
);

-- analytics
CREATE TABLE IF NOT EXISTS gold.analytics_summary ( metric TEXT PRIMARY KEY, value BIGINT );
CREATE TABLE IF NOT EXISTS gold.disease_stats ( disease_class TEXT, crop_type TEXT, severity_level TEXT, count BIGINT );
CREATE TABLE IF NOT EXISTS gold.crop_stats ( crop_type TEXT, count BIGINT );
CREATE TABLE IF NOT EXISTS gold.severity_stats ( severity_level TEXT, count BIGINT );
CREATE TABLE IF NOT EXISTS gold.file_size_stats ( size_category TEXT, image_count BIGINT, avg_size_kb NUMERIC, min_size_kb NUMERIC, max_size_kb NUMERIC );