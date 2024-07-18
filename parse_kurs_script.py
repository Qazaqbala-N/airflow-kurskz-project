import requests
from bs4 import BeautifulSoup
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


@dag(schedule_interval='@daily', start_date=datetime(2024, 7, 1), catchup=False, tags=['kurs parcing example'])
def parse_kurs_dag():
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

    @task()
    def find_max_min_rates(data):
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

    parse_kurs_task = find_max_min_rates(parse_kurs())


dag = parse_kurs_dag()

