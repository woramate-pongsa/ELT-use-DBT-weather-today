import os
import pandas as pd

def extract_weather_data(response):
    import xml.etree.ElementTree as ET

    ## Extract data from API
    province = []
    station_name = []
    latitude = []
    longitude = []
    date_time = []
    temperature = []
    max_temperature = []
    min_temperature = []
    wind_speed = []
    
    if response.status_code == 200:
        root = ET.fromstring(response.text)
        station = root.find("Stations")
        if station is not None:
            for stations in station.findall("Station"):
                # scrap data to variable
                province_scrap = stations.find("Province")
                station_name_scrap = stations.find("StationNameThai")
                latitude_scrap = stations.find("Latitude")
                longitude_scrap = stations.find("Longitude")
                
                observation_scrap = stations.find("Observation")
                date_time_scrap = observation_scrap.find("DateTime")
                temperature_scrap = observation_scrap.find("Temperature")
                max_temperature_scrap = observation_scrap.find("MaxTemperature")
                min_temperature_scrap = observation_scrap.find("MinTemperature")
                wind_speed_scrap = observation_scrap.find("WindSpeed")
                
                # append data to list
                province.append(province_scrap.text)
                station_name.append(station_name_scrap.text)
                latitude.append(latitude_scrap.text)
                longitude.append(longitude_scrap.text)
                date_time.append(date_time_scrap.text)
                temperature.append(temperature_scrap.text)
                max_temperature.append(max_temperature_scrap.text)
                min_temperature.append(min_temperature_scrap.text)
                wind_speed.append(wind_speed_scrap.text)
        else:
            print("Not found <station>")
    else:
        print("Request failed", response.status_code)

    df = pd.DataFrame({
        "province": province,
        "station_name": station_name,
        "latitude": latitude,
        "longitude": longitude,
        "date_time": date_time,
        "temperature": temperature,
        "max_temperature": max_temperature,
        "min_temperature": min_temperature,
        "wind_speed": wind_speed
    })
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["temperature"] = pd.to_numeric(df["temperature"], errors="coerce")
    df["max_temperature"] = pd.to_numeric(df["max_temperature"], errors="coerce")
    df["min_temperature"] = pd.to_numeric(df["min_temperature"], errors="coerce")
    df["wind_speed"] = pd.to_numeric(df["wind_speed"], errors="coerce")
    df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")
    df["date_time"] = df["date_time"].astype("datetime64[us]")

    return df

def load_to_gcs(df: pd.DataFrame) -> None:
    import json
    import pandas as pd
    from io import BytesIO
    from datetime import datetime
    from google.cloud import storage
    from google.oauth2 import service_account
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    date = datetime.today().strftime("%Y-%m-%d")
    GCP_PROJECT_ID = os.environ.get("PROJECT_ID")
    BUCKET_NAME = "lake_project"
    BUSINESS_DOMAIN = "weather_today_data"
    DATA_NAME = f"{date}_weather_today"
    KEYFILE_GCS = os.environ.get("GOOGLE_CLOUD_STORAGE_APPLICATION_CREDENTIALS")
    
    service_account_info_gcs = json.load(open(KEYFILE_GCS))
    credentials_gcs = service_account.Credentials.from_service_account_info(service_account_info_gcs)

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, compression="snappy")

    # Destination to upload
    destination_blob_name = f"{BUSINESS_DOMAIN}/raw_data/{date}/{DATA_NAME}.parquet"

    hook = GCSHook(gcp_conn_id="google_gcs_default")
    hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=destination_blob_name,
        data=parquet_buffer.getvalue()
    )