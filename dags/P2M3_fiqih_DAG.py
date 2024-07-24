'''
Nama        : Muhammad Fiqih Al-ayubi

Batch       : HCK - 017

Objective   : Program ini ditulis untuk membuat workflow task DAG. Program ini terdiri dari beberapa step seperti input data dari database, 
              mengambil data dari database, membersihkan data, menyimpan data, membuat schedule daily batch processing, dan mendefinisikan alur task DAG

Catatan     : Setelah run, tolong tunggu hingga 1 menit agar dapat masuk ke laman login airflow
'''


from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine 
import pandas as pd
import uuid
from elasticsearch import Elasticsearch
import pendulum
 
# Membuat path data
raw_data_path = '/opt/airflow/dags/P2M3_fiqih_data_raw.csv' 
fromPG_data_path = '/opt/airflow/dags/P2M3_fiqih_dataPG_raw.csv'
clean_data_path = '/opt/airflow/dags/P2M3_fiqih_data_clean.csv'

# Membuat nama database, username, dan password postgre
database = "airflow_m3"
username = "airflow_m3"
password = "airflow_m3"
host = "postgres"

# Membuat variable koneksi postgre
postgres_url = f"postgresql+psycopg2://{username}:{password}@{host}/{database}"

# Membuat koneksi sqlalchemy
engine = create_engine(postgres_url)
conn = engine.connect()

def load_csv_pg():

    '''
    Fungsi ini digunakan untuk melakukan data loading/input data ke server postgre.
    Cara kerja :
    1. Membuat dataframe dari alamat raw_data_path
    2. Load table 'table_m3' ke dalam server postgre
    '''

    # Membuat dataframe
    df = pd.read_csv(raw_data_path)

    # Konversi dataframe ke table sql
    df.to_sql('table_m3', conn, index=False, if_exists='replace')  
    

def take_data_postgre():

    '''
    Fungsi ini digunakan untuk mengambil data dari dari database 'airflow_m3' yang berada di server postgre.
    Cara kerja :
    1. Mengkoneksikan ke database airflow_m3
    2. Membuka 'table_m3' dari database airflow_m3
    3. Membuat dataframe dari 'table_m3'
    4. Menyimpan dataframe ke dalam format csv
    '''

    # Melakukan koneksi ke database dan membuat dataframe dari table_m3
    df = pd.read_sql_query("select * from table_m3", conn) 

    # Konversi dataframe ke format csv
    df.to_csv(fromPG_data_path, sep=',', index=False)
    

def data_cleaning():

    ''' 
    Fungsi ini digunakan untuk membersihkan data agar data siap dan dapat dipakai untuk analisa.
    Cara kerja :
    1. Loading data
    2. Menghapus karakter spasi pada bagian depan dan belakang kolom
    3. Mengubah format nama kolom menjadi format lowercase
    4. Mengganti karakter spasi pada kolom dengan karakter '_'
    5. Mengganti nama kolom 'commision_(in_value)' menjadi commision
    6. Handling missing values
    7. Mengganti format nama values pada kolom tertentu menjadi format yang sesuai dengan konteksnya
    8. Memastikan tidak ada karakter spasi di bagian depan dan belakang values (string)
    9. Membuat kolom baru yang bernama travel_id untuk keperluan validasi GX
    10. Menyimpan data hasil cleaning ke format csv
    '''

    # Load data
    df = pd.read_csv(fromPG_data_path)

    # Menghapus karakter spasi di bagian depan dan belakang nama kolom
    df.columns = df.columns.str.strip()

    # Mengubah format nama kolom menjadi format lowercase
    df.columns = df.columns.str.lower()

    # Mengganti karakter spasi pada nama kolom dengan karakter '_'
    df.columns = df.columns.str.replace(' ', '_')

    # Mengganti nama kolom 'commision_(in_value)' menjadi commision
    df.rename(columns= {'commision_(in_value)' : 'commision'}, inplace=True)

    # Menghapus baris duplikat
    df.drop_duplicates(inplace=True)

    # Mengganti NaN values pada kolom gender menjadi 'Unknown'
    df['gender'] = df['gender'].fillna('Unknown')

    # Mengganti format nama product pada kolom product menjadi bentuk title
    df['product_name'] = df['product_name'].str.title()

    # Memastikan tidak ada karakter spasi di bagian depan dan belakang values
    for col in df.select_dtypes(exclude=['int64', 'float64']).columns :
        df[col] = df[col].str.strip()
    
    # Generate unique names combined with UUIDs
    df['travel_id'] = [f"{uuid.uuid4()}" for _ in range(len(df))]

    # Menyimpan data hasil cleaning ke format csv
    df.to_csv(clean_data_path, sep=',', index=False)
    

def upload_to_elasticsearch():

    '''
    Fungsi ini digunakan untuk mengupload data yang sudah bersih ke server elasticsearch.
    Cara kerja :
    1. Membuat object server elasticsearch
    2. Load data hasil cleaning
    3. Mengubah format data menjadi format dictionary agar dapat dibaca dengan json
    '''

    # Membuat object server
    es = Elasticsearch("http://elasticsearch:9200")

    # Membaca file hasil cleaning
    df = pd.read_csv(clean_data_path)
    
    # Looping untuk mengconvert tiap row menjadi dictionary
    for i, r in df.iterrows():
        doc = r.to_dict()  
        res = es.index(index="table_testing", id=i+1, body=doc)
        print(f"Response from Elasticsearch: {res}")
        

        
default_args = {
    'owner': 'Fiqih', 
    'start_date': datetime(2024, 7, 18, tzinfo=pendulum.timezone('Asia/Jakarta'))
}


with DAG(
    "P2M3_Fiqih_DAG", 
    description='Milestone 3 DAG',
    schedule_interval= '30 06 * * *', 
    default_args=default_args, 
    catchup=False
    ) as dag:
    
        # Task : 1
        '''Task ini ditujukan untuk melakukan load data ke server postgre'''
        load_csv_pg_task = PythonOperator(
            task_id='load_csv_pg',
            python_callable=load_csv_pg) 
    
        # Task: 2
        '''Task ini ditujukan untuk mengambil data dari database yang telah dibuat pada server postgre'''
        take_data_postgre_task = PythonOperator(
            task_id='take_data_postgre',
            python_callable=take_data_postgre) #
    

        # Task: 3
        '''Task ini ditujukan untuk melakukan proses cleaning pada data'''
        data_cleaning_task = PythonOperator(
            task_id= 'data_cleaning',
            python_callable= data_cleaning)

        # Task: 4
        '''Task ini ditujukan untuk mengupload data ke server elasticsearch'''
        upload_to_elastic_task = PythonOperator(
            task_id='upload_data_elastic',
            python_callable=upload_to_elasticsearch)

        # Urutan task
        load_csv_pg_task >> take_data_postgre_task >> data_cleaning_task >> upload_to_elastic_task
