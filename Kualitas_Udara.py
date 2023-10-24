from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
import requests

# Koneksi ke database PostgreSQL
db_conn = {
    'host': 'localhost',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'admin'
}

# Fungsi untuk pengambilan data dari OpenAQ API
def extract_data():
    url = "https://api.openaq.org/v2/locations?limit=100&page=1&offset=0&sort=desc&radius=1000&country=ID&order_by=lastUpdated&dump_raw=false"
    headers = {"accept": "application/json"}
    response = requests.get(url, headers=headers)
    data = response.json()

    # Memfilter data untuk Jakarta Central, Indonesia
    jakarta_data = [item for item in data['results'] if item['country'] == 'ID' and item['name'] == 'Jakarta Central']

    return jakarta_data

# Fungsi untuk menyimpan data ke dalam tabel database PostgreSQL
def save_to_database(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='extract_data')

    # Koneksi ke database menggunakan Hook dengan informasi koneksi yang telah ditentukan
    postgres_hook = PostgresHook(postgres_conn_id='Kualitas_udara_connection', conn_name_attr=db_conn)

    # Membuat tabel 'openaq_data'
    create_table_query = """
    CREATE TABLE IF NOT EXISTS openaq_data (
        id SERIAL PRIMARY KEY,
        city TEXT,
        name TEXT,
        entity TEXT,
        country TEXT,
        sources TEXT,
        isMobile BOOLEAN,
        isAnalysis BOOLEAN,
        parameters JSONB,
        sensorType TEXT,
        coordinates JSONB,
        lastUpdated TIMESTAMP,
        firstUpdated TIMESTAMP,
        measurements INTEGER,
        bounds JSONB,
        manufacturers JSONB
    );
    """
    postgres_hook.run(create_table_query)

    # Menyimpan data ke dalam tabel
    for item in data:
        insert_query = """
        INSERT INTO openaq_data (city, name, entity, country, sources, isMobile, isAnalysis, parameters, sensorType, coordinates, lastUpdated, firstUpdated, measurements, bounds, manufacturers)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            item['city'],
            item['name'],
            item['entity'],
            item['country'],
            item['sources'],
            item['isMobile'],
            item['isAnalysis'],
            item['parameters'],
            item['sensorType'],
            item['coordinates'],
            item['lastUpdated'],
            item['firstUpdated'],
            item['measurements'],
            item['bounds'],
            item['manufacturers']
        ))

# DAG
dag = DAG(
    'openaq_data_pipeline',
    start_date=datetime(2023, 10, 21, 4, 0),  # Jadwalkan eksekusi pada jam 4 pagi
    schedule_interval='0 4 * * *',  # Ekspresi cron
    catchup=False,  # Jangan mengejar eksekusi yang terlewat
)

# Task untuk ekstraksi data dari OpenAQ API dan memfilternya
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Task untuk menyimpan data ke dalam tabel database
save_to_db_task = PythonOperator(
    task_id='save_to_database',
    python_callable=save_to_database,
    provide_context=True,
    dag=dag,
)

# Mengatur urutan tugas
extract_task >> save_to_db_task