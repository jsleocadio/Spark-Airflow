import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id='spark_airflow',
    default_args={
        "owner": "Jefferson Leocadio",
        "start_date": airflow.utils.dates.days_ago(1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Job Started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application="jobs/python_job.py",
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Job Finished"),
    dag=dag
)

start >> python_job >> end