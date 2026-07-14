from datetime import datetime, timedelta
import os
import subprocess
import sys
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Get the project root directory
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR)

default_args = {
    'owner': 'smartlogix_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15),
}

dag = DAG(
    'smartlogix_logistics_pipeline',
    default_args=default_args,
    description='SmartLogix Automated Logistics Data Pipeline',
    schedule_interval='@daily',
    catchup=False,
)

# Task 1: Start Kafka Producer
# Run producer for 30 seconds to generate a fresh batch of events
start_producer = BashOperator(
    task_id='start_producer',
    bash_command=f'"{sys.executable}" "{os.path.join(PROJECT_ROOT, "producer.py")}" --duration 30',
    dag=dag,
)

# Task 2: Run Spark Streaming
# Run spark streaming with a 45 second timeout to process the generated events
run_spark_streaming = BashOperator(
    task_id='run_spark_streaming',
    bash_command=f'"{sys.executable}" "{os.path.join(PROJECT_ROOT, "spark_streaming.py")}" --timeout 45',
    dag=dag,
)

# Task 3: Verify SQLite
# Run verification query to print count of shipments and table statuses
verify_sqlite = BashOperator(
    task_id='verify_sqlite',
    bash_command=f'"{sys.executable}" "{os.path.join(PROJECT_ROOT, "test_query.py")}"',
    dag=dag,
)

# Task 4: Launch Dashboard
# Starts Streamlit in a detached background process to prevent blocking the Airflow task
def launch_streamlit_bg():
    dashboard_path = os.path.join(PROJECT_ROOT, "dashboard.py")
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = 0x00000008  # DETACHED_PROCESS flag for Windows
        
    subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", dashboard_path],
        creationflags=creation_flags,
        close_fds=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )
    print("Streamlit dashboard successfully launched in the background.")

launch_dashboard = PythonOperator(
    task_id='launch_dashboard',
    python_callable=launch_streamlit_bg,
    dag=dag,
)

# Task dependencies
start_producer >> run_spark_streaming >> verify_sqlite >> launch_dashboard
