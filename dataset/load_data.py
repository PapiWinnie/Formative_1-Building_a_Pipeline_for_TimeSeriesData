import pandas as pd
from datetime import datetime
import mysql.connector
import numpy as np  

# Load dataset
df = pd.read_csv(".\Metro_Interstate_Traffic_Volume.csv")
df['date_time'] = pd.to_datetime(df['date_time'], format='%d-%m-%Y %H:%M')

df['date_time'] = df['date_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Connect to MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password", #replace with ur password or the variable storing it
    database="traffic_db"
)

cursor = conn.cursor()

# Loop through dataset
for _, row in df.iterrows():

    # Insert weather
    cursor.execute("""
        INSERT INTO weather (temp, rain_1h, snow_1h, clouds_all, weather_main, weather_description)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        row["temp"],
        row["rain_1h"],
        row["snow_1h"],
        row["clouds_all"],
        row["weather_main"],
        row["weather_description"]
    ))

    weather_id = cursor.lastrowid

    # Insert holiday
    holiday_name = row["holiday"] if pd.notna(row["holiday"]) else "None"

    cursor.execute("""
        INSERT INTO holiday (holiday_name, is_holiday)
        VALUES (%s, %s)
    """, (
        holiday_name,
        holiday_name != "None"
    ))

    holiday_id = cursor.lastrowid

    # Insert traffic record
    cursor.execute("""
        INSERT INTO traffic_data (datetime, traffic_volume, weather_id, holiday_id)
        VALUES (%s, %s, %s, %s)
    """, (
        row["date_time"],
        row["traffic_volume"],
        weather_id,
        holiday_id
    ))

# Commit changes
conn.commit()

print("Dataset successfully loaded!")

cursor.close()
conn.close()