import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import psycopg2
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

API_KEY = os.getenv("API_KEY")
CITIES = os.getenv("CITIES")
RESPONSE_FORMAT = os.getenv("RESPONSE_FORMAT")

MAX_RETRIES = 3


def extract_data(api_key: str, city: str, response_format: str) -> dict:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units={response_format}"
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Requesting weather data for city={city}")

            response_api = requests.get(url, timeout=10)
            response_api.raise_for_status()

            logging.info(f"Weather data successfully received for city={city}")
            return response_api.json()

        except requests.exceptions.RequestException as e:
            logging.error(f"Requesting weather data for city={city} is not successful. Error: {e}", exc_info=True)
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))
    raise RuntimeError("Failed to fetch data after retries")

def transform_data(weather_dict: dict) -> dict:
    try:
        city = weather_dict["name"]
        wind_speed = weather_dict["wind"]["speed"]
        temp = weather_dict["main"]["temp"]
        weather_time = datetime.fromtimestamp(weather_dict["dt"], timezone.utc)
        feels_like = weather_dict["main"]["feels_like"]
        description = weather_dict["weather"][0]["description"]

        logging.info(f"Transformed data: city={city}, temp={temp}, wind_speed={wind_speed}, "
                     f"weather_time={weather_time}, feels_like={feels_like}, description={description}")

        data = {
            "city": city,
            "temp": temp,
            "wind_speed": wind_speed,
            "weather_time": weather_time,
            "feels_like": feels_like,
            "description": description
        }

        return data

    except KeyError as e:
        logging.error(f"Missing key in API response for city={weather_dict.get('name')}: {e}", exc_info=True)
        raise

    except TypeError as e:
        logging.error(f"Invalid data format: {e}", exc_info=True)
        raise


def load_data(data: dict, conn):
    for attempt in range(MAX_RETRIES):
        cursor = None
        try:
            logging.info(f"Start loading data for city={data.get('city', 'UNKNOWN')}")

            query = """INSERT INTO weather (city, temp, wind_speed, weather_time, feels_like, description) 
                    VALUES (%(city)s, %(temp)s, %(wind_speed)s, %(weather_time)s, %(feels_like)s, %(description)s)
                    ON CONFLICT (city, weather_time) DO NOTHING;"""

            cursor = conn.cursor()

            cursor.execute(query, data)

            conn.commit()

            logging.info(f"Successfully loaded data into DB for city={data.get('city', 'UNKNOWN')}")
            return

        except psycopg2.Error as e:
            logging.error(f"Database error: {e}", exc_info=True)

            if conn:
                conn.rollback()

            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** (attempt + 1))

        finally:
            if cursor:
                cursor.close()


def main():
    cities = [city.strip() for city in CITIES.split(",")]

    conn = None

    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DBNAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT")
        )

        for city in cities:
            try:
                logging.info(f"Start processing city={city}")

                weather_dict = extract_data(api_key=API_KEY, city=city, response_format=RESPONSE_FORMAT)
                data = transform_data(weather_dict)
                load_data(data, conn)

                logging.info(f"Finished processing city={city}")

            except Exception as e:
                logging.error(f"Failed processing city={city}: {e}", exc_info=True)
                continue

    except psycopg2.Error as e:
        logging.error(f"Failed to connect to DB: {e}", exc_info=True)
        raise

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()