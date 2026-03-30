import time
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
    for attempt in range(3):
        try:
            logging.info(f"Requesting weather data for city={CITY}")

            response_api = requests.get(url, timeout=3)
            response_api.raise_for_status()

            logging.info("Weather data successfully received")
            return response_api.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Requesting weather data for city={CITY} is not successful. Error: {e}",exc_info=True)
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)
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
        logging.error(f"Missing key in API response for city={weather_dict.get('name')}: {e}",
                      exc_info=True)
        raise

    except TypeError as e:
        logging.error(f"Invalid data format: {e}", exc_info=True)
        raise


def load_data(data: dict):
    conn = None
    cursor = None
    try:
        logging.info(f"Start loading data for city={data.get('city', 'UNKNOWN')}")

        conn = psycopg2.connect(
            dbname=os.getenv("DBNAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT")
        )
        cursor = conn.cursor()

        query = """INSERT INTO weather (city, temp, wind_speed, weather_time, feels_like, description) 
                VALUES (%(city)s, %(temp)s, %(wind_speed)s, %(weather_time)s, %(feels_like)s, %(description)s);"""

        cursor.execute(query, data)
        conn.commit()

        logging.info(f"Successfully loaded data into DB for city={data.get('city', 'UNKNOWN')}")

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


# def retry(retries=3):
#     for attempt in range(retries):
#         except (requests.exceptions.RequestException, psycopg2.Error) as e:
#             if e.response is None or 500 <= e.response.status_code < 600 or e.response.status_code == 429:
#                 retry
#             else:
#                 raise
def main():
    weather_dict = extract_data()
    data = transform_data(weather_dict)
    load_data(data)

if __name__ == "__main__":
    main()