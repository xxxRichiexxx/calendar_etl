import pandas as pd
import sqlalchemy as sa
from urllib.parse import quote
import datetime as dt

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.hooks.base import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.contrib.operators.vertica_operator import VerticaOperator


dwh_con = BaseHook.get_connection('vertica')
ps = quote(dwh_con.password)
dwh_engine = sa.create_engine(
    f'vertica+vertica_python://{dwh_con.login}:{ps}@{dwh_con.host}:{dwh_con.port}/sttgaz'
)

def extract(year):
    return pd.read_csv(f'http://xmlcalendar.ru/data/ru/{year}/calendar.csv')

def transform(data):
    return data

def load(data):
    data.to_sql(
        f'stage_calendar',
        dwh_engine,
        schema='sttgaz',
        if_exists='append',
        index=False,        
    )
    
def etl(year, **context):
    year = context['execution_date'].year
    load(transform(extract(year)))



#-------------- DAG -----------------

default_args = {
    'owner': 'Швейников Андрей',
    'email': ['shveynikovab@st.tech'],
    'retries': 3,
    'retry_delay': dt.timedelta(minutes=30),
}
with DAG(
        'calendar',
        default_args=default_args,
        description='Получение данных производственного календаря.',
        start_date=days_ago(90),
        schedule_interval='@monthly',
        catchup=True,
        max_active_runs=1
) as dag:

    start = DummyOperator(task_id='Начало')

    with TaskGroup('Загрузка_данных_в_stage_слой') as data_to_stage:

        load_data = PythonOperator(
            task_id=f'Получение_календаря',
            python_callable=etl,
        )

        load_data

    with TaskGroup('Формирование_слоя_DDS') as data_to_dds:

        pass

    end = DummyOperator(task_id='Конец')

    start >> data_to_stage >> data_to_dds >> end