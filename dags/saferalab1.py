from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from sqlalchemy import create_engine

# Define your Azure Storage and MySQL database connection details
azure_account_name = "airflowtestorage"
azure_account_key = "n8gt+c0VtYhPuZKgYbveWfLQ8qDtsLvcu8/vZflfAszPms1+g1h3AO0Qa0EHj0aZQzqaIr4kH/ln+ASt6k/iGA=="
azure_container_name = "test"
azure_blob_name1 = "CrimeData.csv"
azure_blob_name2 = "WeatherData.csv"
mysql_hostname = '68.183.74.45'
mysql_username = 'muskan'
mysql_password = 'ThirdEye257!?'
mysql_database_name = 'adf'
mysql_table_name = 'CrimeWeather_Merged'

default_args = {
    'owner': 'muskan',
    'start_date': datetime(2023, 9, 28),
    'retries': 1,
}

def process_weather_data():
    blob_service_client = BlobServiceClient(account_url=f"https://{azure_account_name}.blob.core.windows.net", credential=azure_account_key)
    blob_client2 = blob_service_client.get_blob_client(container=azure_container_name, blob=azure_blob_name2)

    with io.BytesIO() as stream2:
        blob_data2 = blob_client2.download_blob()
        blob_data2.download_to_stream(stream2)
        stream2.seek(0)
        weather_df = pd.read_csv(stream2)

        # Load the data into a DataFrame
        weather_df.iloc[:, 1] = pd.to_datetime(weather_df.iloc[:, 1], format="%d-%m-%Y").dt.strftime("%Y/%m/%d")

        # Define a function to convert temperature strings to decimal numbers
        def toDecimal(temp_str):
            return pd.to_numeric(temp_str, errors='coerce')

        # Apply the function to the "tempmax," "tempmin," and "temp" columns
        weather_df['tempmax'] = weather_df['tempmax'].apply(lambda x: toDecimal(x))
        weather_df['tempmin'] = weather_df['tempmin'].apply(lambda x: toDecimal(x))
        weather_df['temp'] = weather_df['temp'].apply(lambda x: toDecimal(x))

        # Define a function to convert "datetime" to the desired format
        def toDate(datetime_str):
            return pd.to_datetime(datetime_str, format="%Y/%m/%d")

        # Apply the function to the "datetime" column
        weather_df['datetime'] = weather_df['datetime'].apply(lambda x: toDate(x))

    return weather_df

def process_crime_data():
    blob_service_client = BlobServiceClient(account_url=f"https://{azure_account_name}.blob.core.windows.net", credential=azure_account_key)
    blob_client1 = blob_service_client.get_blob_client(container=azure_container_name, blob=azure_blob_name1)

    with io.BytesIO() as stream1:
        blob_data1 = blob_client1.download_blob()
        blob_data1.download_to_stream(stream1)
        stream1.seek(0)
        data = pd.read_csv(stream1)

    report_date = data['REPORT_DATE'].copy()
    report_date_10 = report_date.str[:10]
    report_day = report_date.str[8:10]
    month = report_date.str[5:7]
    report_date = pd.to_datetime(report_date)
    time_of_crime = report_date.dt.time

    data['REPORT_DATE'] = report_date_10
    data['REPORT_DAY'] = report_day
    data['Month'] = month
    data['TimeofCrime'] = time_of_crime

    data['REPORT_DATE'] = pd.to_datetime(data['REPORT_YEAR'].astype(str) + '-' + data['Month'] + '-' + data['REPORT_DAY'].astype(str))
    data['REPORT_DATE'] = data['REPORT_DATE'].dt.strftime('%Y-%m-%d')

    data['OCC_DATE'] = pd.to_datetime(data['OCC_YEAR'].astype(str) + '-' + data['OCC_DATE'].str[5:7] + '-' + data['OCC_DATE'].str[8:10])
    data['OCC_DATE'] = data['OCC_DATE'].dt.strftime('%Y-%m-%d')

    numeric_columns = ['UCR_CODE', 'UCR_EXT', 'HOOD_158', 'HOOD_140', 'OBJECTID', 'OCC_YEAR', 'REPORT_HOUR', 'REPORT_YEAR', 'REPORT_DAY', 'REPORT_DOY', 'OCC_DAY', 'OCC_DOY', 'OCC_HOUR']
    data[numeric_columns] = data[numeric_columns].apply(pd.to_numeric, errors='coerce')

    return data

def merge_weather_and_crime_data(weather_data, crime_data):
    # Convert 'datetime' in weather_data to object (string) data type
    weather_data['datetime'] = weather_data['datetime'].dt.strftime('%Y-%m-%d')

    # Merge the processed weather and crime data on the common date column
    merged_data = crime_data.merge(weather_data, left_on='REPORT_DATE', right_on='datetime', how='inner')

    return merged_data

def insert_merged_data_into_table(hostname, username, password, database_name, table_name, merged_data):
    try:
        # Create a SQLAlchemy engine to connect to the database
        engine = create_engine(f'mysql://{username}:{password}@{hostname}/{database_name}')

        # Use the 'to_sql' method to insert data into the specified table
        merged_data.to_sql(table_name, con=engine, if_exists='replace', index=False)

        print(f'Data inserted into table "{table_name}" successfully.')

    except Exception as e:
        print(f'An error occurred: {str(e)}')

    finally:
        # Close the database connection when done
        engine.dispose()

with DAG(
    'saferalab1',
    default_args=default_args,
    schedule_interval=None,  # Define your desired schedule interval
    catchup=False,
) as dag:
    def download_and_process_weather_data():
        # Process the weather data and store it in a variable
        processed_weather_data = process_weather_data()

    def download_and_process_crime_data():
        # Process the crime data and store it in a variable
        processed_crime_data = process_crime_data()

    def merge_and_insert_data():
        # Process the weather data and store it in a variable
        processed_weather_data = process_weather_data()

        # Process the crime data and store it in a variable
        processed_crime_data = process_crime_data()

        # Merge the data
        merged_data = merge_weather_and_crime_data(processed_weather_data, processed_crime_data)

        # Insert merged data into the database table
        insert_merged_data_into_table(mysql_hostname, mysql_username, mysql_password, mysql_database_name, mysql_table_name, merged_data)

    # Define three PythonOperator tasks
    download_and_process_weather_task = PythonOperator(
        task_id='download_and_process_weather_data',
        python_callable=download_and_process_weather_data,
    )

    download_and_process_crime_task = PythonOperator(
        task_id='download_and_process_crime_data',
        python_callable=download_and_process_crime_data,
    )

    merge_and_insert_data_task = PythonOperator(
        task_id='merge_and_insert_data',
        python_callable=merge_and_insert_data,
    )

    # Set task dependencies
    download_and_process_weather_task >> download_and_process_crime_task >> merge_and_insert_data_task
