import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1. Define Python functions that will perform the actions
def debut_pipeline():
    print("Debut du pipeline Big Data")

def traitement_pipeline():
    print("Traitement en cours")
    print("Simulation d’une etape de traitement de donnees")

def fin_pipeline():
    print("Fin du pipeline Big Data")

# 2. Define the DAG structure
with DAG(
    dag_id="mon_premier_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,  # Triggered manually only
    catchup=False,
    tags=["initiation", "python-operator"],
) as dag:

    # 3. Instantiate tasks using PythonOperator
    debut = PythonOperator(
        task_id="debut",
        python_callable=debut_pipeline,
    )

    traitement = PythonOperator(
        task_id="traitement",
        python_callable=traitement_pipeline,
    )

    fin = PythonOperator(
        task_id="fin",
        python_callable=fin_pipeline,
    )

    # 4. Set the execution order (dependencies)
    debut >> traitement >> fin