-- Tabelas alvo no Postgres (usadas pelas DAGs)
CREATE TABLE IF NOT EXISTS acompanhamento_clientes (
  id_cliente     BIGINT PRIMARY KEY,
  status         TEXT,
  substatus         TEXT,
  obs         TEXT,
  atualizado_em  TIMESTAMPTZ NOT NULL
);

-- Tabela de watermark (controle de última execução por fonte)
CREATE TABLE IF NOT EXISTS etl_watermark (
  key      TEXT PRIMARY KEY,
  last_ts  TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01 00:00:00+00'
);

-- (Opcional) Índice por atualizado_em para consultas de auditoria
CREATE INDEX IF NOT EXISTS idx_acompanhamento_clientes_atualizado_em
  ON acompanhamento_clientes (atualizado_em);