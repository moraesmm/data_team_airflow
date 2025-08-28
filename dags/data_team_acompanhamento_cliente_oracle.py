"""
DAG: clientes_delta_sync (sem providers)
- Lê delta de CLIENTE_ACOMP no Oracle (ATUALIZADO_EM > watermark).
- Upsert no Postgres em acompanhamento_clientes.
- Atualiza watermark (etl_watermark).
Conexões:
  - oracle_default  (extra: {"service_name":"FREEPDB1"})
  - postgres_default
Requisitos (na imagem custom):
  - oracledb, psycopg2-binary
"""

from datetime import datetime, timezone
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

WATERMARK_KEY = "oracle_clientes_acomp"
SQL_DIR = Path(__file__).parent / "sql"
ORACLE_SELECT_SQL = (SQL_DIR / "oracle" / "select_delta_clientes.sql").read_text(encoding="utf-8")
PG_CREATE_SQL = (SQL_DIR / "postgres" / "create_target_tables.sql").read_text(encoding="utf-8")
PG_UPDATE_WM_SQL = (SQL_DIR / "postgres" / "update_watermark.sql").read_text(encoding="utf-8")


def _pg_conn():
    import psycopg2
    c = BaseHook.get_connection("postgres_default")
    return psycopg2.connect(
        host=c.host or "postgres-db",
        port=int(c.port or 5432),
        user=c.login,
        password=c.password,
        dbname=c.schema,
    )


def _ora_conn():
    import oracledb
    c = BaseHook.get_connection("oracle_default")
    extra = c.extra_dejson or {}
    dsn = oracledb.makedsn(
        host=c.host or "oracle-db",
        port=int(c.port or 1521),
        service_name=extra.get("service_name") or extra.get("sid") or "FREEPDB1",
    )
    return oracledb.connect(user=c.login, password=c.password, dsn=dsn)


def ensure_postgres_tables():
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(PG_CREATE_SQL)
        conn.commit()


def get_watermark(**context):
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT last_ts FROM etl_watermark WHERE key = %s", (WATERMARK_KEY,))
        row = cur.fetchone()
    last_ts = row[0] if row and row[0] else datetime(1970, 1, 1, tzinfo=timezone.utc)
    return last_ts.isoformat()


def extract_from_oracle(**context):
    last_ts_iso = context["ti"].xcom_pull(task_ids="get_watermark")
    last_ts = datetime.fromisoformat(last_ts_iso)
    with _ora_conn() as conn, conn.cursor() as cur:
        cur.execute(ORACLE_SELECT_SQL, {"last_ts": last_ts})
        data = cur.fetchall()
    rows = []
    max_ts = None
    for (id_cliente, status, substatus, obs, atualizado_em) in data:
        if max_ts is None or atualizado_em > max_ts:
            max_ts = atualizado_em
        rows.append({
            "id_cliente": int(id_cliente),
            "status": None if status is None else str(status),
            "substatus": None if substatus is None else str(substatus),
            "obs": None if obs is None else str(obs),
            "atualizado_em_iso": atualizado_em.isoformat(),
        })
    return {"rows": rows, "max_ts_iso": max_ts.isoformat() if max_ts else None}


def upsert_into_postgres(**context):
    payload = context["ti"].xcom_pull(task_ids="extract_from_oracle") or {}
    rows = payload.get("rows", [])
    if not rows:
        print("Nenhum registro novo/atualizado no Oracle.")
        return 0

    from psycopg2.extras import execute_values
    from datetime import datetime
    tuples = [
        (r["id_cliente"], r["status"], r["substatus"], r["obs"], datetime.fromisoformat(r["atualizado_em_iso"]))
        for r in rows
    ]
    upsert_sql = """
        INSERT INTO acompanhamento_clientes (id_cliente, status, substatus, obs, atualizado_em)
        VALUES %s
        ON CONFLICT (id_cliente) DO UPDATE
        SET status = EXCLUDED.status,
            substatus = EXCLUDED.substatus,
            obs = EXCLUDED.obs,
            atualizado_em = GREATEST(acompanhamento_clientes.atualizado_em, EXCLUDED.atualizado_em);
    """
    with _pg_conn() as conn, conn.cursor() as cur:
        execute_values(cur, upsert_sql, tuples, page_size=1000)
        conn.commit()
    return len(tuples)


def update_watermark(**context):
    affected = context["ti"].xcom_pull(task_ids="upsert_into_postgres")
    payload = context["ti"].xcom_pull(task_ids="extract_from_oracle") or {}
    max_ts_iso = payload.get("max_ts_iso")
    if not affected or not max_ts_iso:
        print("Sem alterações; watermark não atualizado.")
        return
    from datetime import datetime
    max_ts = datetime.fromisoformat(max_ts_iso)
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(PG_UPDATE_WM_SQL, (WATERMARK_KEY, max_ts))
        conn.commit()


with DAG(
    dag_id="data_team_acompanhamento_cliente_oracle",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["oracle", "postgres", "cdc", "delta"],
) as dag:
    t0 = PythonOperator(task_id="ensure_postgres_tables", python_callable=ensure_postgres_tables)
    t1 = PythonOperator(task_id="get_watermark", python_callable=get_watermark, do_xcom_push=True)
    t2 = PythonOperator(task_id="extract_from_oracle", python_callable=extract_from_oracle, do_xcom_push=True)
    t3 = PythonOperator(task_id="upsert_into_postgres", python_callable=upsert_into_postgres, do_xcom_push=True)
    t4 = PythonOperator(task_id="update_watermark", python_callable=update_watermark)
    t0 >> t1 >> t2 >> t3 >> t4
