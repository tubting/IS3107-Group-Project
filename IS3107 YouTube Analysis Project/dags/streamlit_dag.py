from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
import os
from pendulum import datetime, duration


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "dagrun_timeout":duration(hours=1),
    "description":"Run streamlit app.",
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG('streamlit_app', default_args=default_args, schedule=None, catchup=False, tags=['tutorial5']) as dag:
    run_streamlit_app = BashOperator(
        task_id="run_streamlit_app",
        # retrieve the command from the global variables file
        bash_command='python -m streamlit run Streamlit_new.py',
        # provide the directory to run the bash command in
        cwd="/",
    )

    run_streamlit_app
