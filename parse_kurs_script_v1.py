from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
from bs4 import BeautifulSoup
import json

def parse_kurs():
    url = 'https://kurs.kz/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    script_tag = soup.find('script', string=lambda text: 'punkts' in str(text))

    if script_tag:
        script_text = script_tag.get_text()
        start_index = script_text.find('var punkts = ') + len('var punkts = ')
        end_index = script_text.find(';', start_index)

        if start_index != -1 and end_index != -1:
            punks_data = script_text[start_index:end_index]

            try:
                punks_list = json.loads(punks_data)
                return punks_list

            except json.JSONDecodeError as e:
                print(f"Ошибка при декодировании JSON: {e}")
    else:
        print("Тег <script> с искомым текстом не найден.")

def find_max_min_rates():
    data = parse_kurs()
    all_rates = []
    for item in data:
        for currency, rates in item['data'].items():
            if currency != 'GOLD' and all(rate != 0 for rate in rates):
                all_rates.extend(rates)
    max_rate = max(all_rates)
    min_rate = min(all_rates)

    print(f"Максимальное значение курса: {max_rate}")
    print(f"Минимальное значение курса: {min_rate}")
    return max_rate, min_rate

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'parse_currency_dag',
    default_args=default_args,
    description='A DAG to parse currency rates from kurs.kz',
    schedule_interval='0 12 * * *',  # Executes the DAG at 12:00 PM UTC daily
)

parse_task = PythonOperator(
    task_id='parse_currency_task',
    python_callable=find_max_min_rates,
    dag=dag,
)

parse_task
