import logging
import os
from datetime import datetime, timezone

import requests
import psycopg2
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv()
API_KEY = os.getenv("API_KEY")
CITY = os.getenv("CITY")
RESPONSE_FORMAT = os.getenv("RESPONSE_FORMAT")

url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units={RESPONSE_FORMAT}"


def extract_data():
    try:
        logging.info(f"Requesting weather data for city={CITY}")

        response_api = requests.get(url, timeout=5)
        response_api.raise_for_status()

        logging.info("Weather data successfully received")
        return response_api.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"Requesting weather data for city={CITY} is not successful. Error: {e}")
        raise


def transform_data(weather_dict):
    try:
        city = weather_dict["name"]
        wind_speed = weather_dict["wind"]["speed"]
        temp = weather_dict["main"]["temp"]
        weather_time = datetime.fromtimestamp(weather_dict["dt"], timezone.utc)
        feels_like = weather_dict["main"]["feels_like"]
        description = weather_dict["weather"][0]["description"]

        logging.info(f"""Transformed data: city={city}, temp={temp}, wind_speed={wind_speed}, 
                    weather_time={weather_time}, feels_like={feels_like}, description={description}""")

        return city, temp, wind_speed, weather_time, feels_like, description

    except KeyError as e:
        logging.error(f"Missing key in API response for city={CITY}: {e}", exc_info=True)
        raise

    except TypeError as e:
        logging.error(f"Invalid data format: {e}")
        raise


def load_data(city, temp, wind_speed, weather_time, feels_like, description):
    conn = None
    cursor = None
    try:
        logging.info(f"""Start loading data for city={city}, temp={temp}, wind_speed={wind_speed}, 
                    weather_time={weather_time}, feels_like={feels_like}, description={description}""")

        conn = psycopg2.connect(
            dbname=os.getenv("DBNAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT")
        )
        cursor = conn.cursor()

        cursor.execute("""INSERT INTO weather (city, temp, wind_speed, weather_time, feels_like, description) 
                                VALUES (%s, %s, %s, %s, %s, %s);""",
                         (city, temp, wind_speed, weather_time, feels_like, description))
        conn.commit()

        logging.info(f"Successfully loaded data into DB for city={city}")

    except psycopg2.Error as e:
        logging.error(f"Database error: {e}", exc_info=True)

        if conn:
            conn.rollback()

        raise

    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()


def main():
    weather_dict = extract_data()
    city, temp, wind_speed, weather_time, feels_like, description  = transform_data(weather_dict)
    load_data(city, temp, wind_speed, weather_time, feels_like, description)

if __name__ == "__main__":
    main()