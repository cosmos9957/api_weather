import os
from datetime import datetime, timezone

import requests
import psycopg2
from dotenv import load_dotenv


load_dotenv()
API_KEY = os.getenv("API_KEY")
CITY = os.getenv("CITY")
RESPONSE_FORMAT = os.getenv("RESPONSE_FORMAT")

url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units={RESPONSE_FORMAT}"

def main():
    def extract_data():
        response_api = requests.get(url)
        return response_api.json()

    weather_data = extract_data()


    def transform_data(weather_data):
        city = weather_data.get("name")
        wind_speed = weather_data.get("wind", {}).get("speed")
        temp = weather_data.get("main", {}).get("temp")
        return city, temp, wind_speed

    city, temp, wind_speed  = transform_data(weather_data)


    def load_data(city, temp, wind_speed):
        conn = psycopg2.connect(
            dbname=os.getenv("DBNAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("HOST"),
            port=os.getenv("PORT")
        )

        cursor = conn.cursor()
        cursor.execute("INSERT INTO weather (city, temp, wind_speed) \
                       VALUES (%s, %s, %s);", (city, temp, wind_speed))
        conn.commit()
        cursor.close()
        conn.close()

    load_data(city, temp, wind_speed)


if __name__ == "__main__":
    main()