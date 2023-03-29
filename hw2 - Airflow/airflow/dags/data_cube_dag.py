from datetime import timedelta, datetime
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator

from operators.care_providers import data_cb_care_providers
from operators.population import data_cb_population


def consumer_operator(consumer, **kwargs):
    output = kwargs['dag_run'].conf.get("output_path", "N/A")
    consumer(output)

def get_dataset(url, filename: str):
    urllib.request.urlretrieve(url, filename)


default_args = {
    'owner': 'nguyeha',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id="data-cube-dag",
        default_args=default_args,
        start_date=datetime(2023, 3, 27),
        schedule=None,
        catchup=False
) as dag:
    task01 = PythonOperator(
        task_id="donwload_dataset_careprovider",
        python_callable=get_dataset,
        op_kwargs={
            "url": "https://opendata.mzcr.cz/data/nrpzs/narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv",
            "filename": "./narodni-registr-poskytovatelu-zdravotnich-sluzeb.csv"
        }
    )

    task02 = PythonOperator(
        task_id="care-providers-dc",
        python_callable=consumer_operator,
        op_args=[data_cb_care_providers]
    )

    task03 = PythonOperator(
        task_id="download_dataset_population",
        python_callable=get_dataset,
        op_kwargs={
            'url': "https://www.czso.cz/documents/10180/165603907/13007221n01.pdf/c09364f1-11ab-46eb-94d0-686d3857cede?version=1.2",
            "filename": "./populace-okresy-2021.xlsx"
        }
    )

    task04 = PythonOperator(
        task_id="population_dc",
        python_callable=consumer_operator,
        op_args=[data_cb_population]
    )

    task01 >> task02
    task03 >> task04
