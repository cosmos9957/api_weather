import requests
from dotenv import load_dotenv

import os


load_dotenv()
API_KEY = os.getenv("API_KEY")
CITY = os.getenv("CITY")
RESPONSE_FORMAT = os.getenv("RESPONSE_FORMAT")

url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units={RESPONSE_FORMAT}"

response_data = requests.get(url)
weather_data = response_data.json()

for i in weather_data.items():
    print(i)