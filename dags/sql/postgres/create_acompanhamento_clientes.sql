-- Tabelas alvo no Postgres (usadas pelas DAGs)
CREATE TABLE IF NOT EXISTS acompanhamento_clientes (
  id_cliente     BIGINT PRIMARY KEY,
  status         TEXT,
  substatus         TEXT,
  obs         TEXT,
  atualizado_em  TIMESTAMPTZ NOT NULL
);

-- (Opcional) Índice por atualizado_em para consultas de auditoria
CREATE INDEX IF NOT EXISTS idx_acompanhamento_clientes_atualizado_em
  ON acompanhamento_clientes (atualizado_em);