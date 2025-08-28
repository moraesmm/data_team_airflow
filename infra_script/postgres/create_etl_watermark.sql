-- Tabela de watermark (controle de última execução por fonte)
CREATE TABLE IF NOT EXISTS etl_watermark (
  key      TEXT PRIMARY KEY,
  last_ts  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);