from airflow import DAG 
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


def transform_load_data(task_instance):
    #data taken from extract_weather_data is used here
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "ASIA5XDFTRU3ZBNEXTWD", "secret": "keOCq3Wv5fI+eeRLn4gsU7X/A431eii17veqcgz7", "token": "FwoGZXIvYXdzEOn//////////wEaDBi/taMC4pONKCy6qiJqcyXbLJc9JaeW3W06if7qp6evBAo68TSmWwMsmsm/Xfx+OcPqQSYc0i4jqWao626/dOSWC+QbOwjXZUt0l3aI5/QS8xx+4VyX1juXaOnRjTC0uobyNvhZJ++Y50c8tlg0RfKVXhufbhDlsii61pGtBjIoN8s/3slDPhuM/H4WvmHNH9/0K9ySoaiB/cPTQu/+hyi/DtNERxAo0A=="}

    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_chicago_' + dt_string
    df_data.to_csv(f"s3://openweatherapiairflowbucket/{dt_string}.csv", index=False, storage_options=aws_credentials)


default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2024, 1, 11),
    'email' : ['med.prodduturi@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(minutes=2)

}
 
with DAG('weather_dag',
        default_args = default_args,
        schedule_interval = '@daily',
        catchup = False) as dag:

        is_weather_api_ready = HttpSensor(
        task_id = 'is_weather_api_ready',
        http_conn_id = 'weathermap_api',
        endpoint = '/data/2.5/weather?q=Chicago&APPID=6254177267a073929ea8c868fa3a03e4'
        )
        extract_weather_data = SimpleHttpOperator(
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Chicago&APPID=6254177267a073929ea8c868fa3a03e4',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
        
        )

        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data #running the transform_load_data function
        )

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
     
