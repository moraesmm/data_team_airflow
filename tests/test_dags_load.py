import os
from airflow.models import DagBag

def test_dags_import_ok():
    os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert bag.import_errors == {}, f"Import errors: {bag.import_errors}"
    assert len(bag.dags) > 0, "Nenhuma DAG encontrada em ./dags"
