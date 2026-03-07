-- Metro Interstate Traffic Volume - Relational Database Schema

CREATE DATABASE IF NOT EXISTS traffic_db;
USE traffic_db;

-- Table 1: weather_conditions
-- Stores unique combinations of weather main category and description
CREATE TABLE IF NOT EXISTS weather_conditions (
    weather_id          INT AUTO_INCREMENT PRIMARY KEY,
    weather_main        VARCHAR(50)  NOT NULL,
    weather_description VARCHAR(100) NOT NULL,
    UNIQUE KEY uq_weather (weather_main, weather_description)
);

-- Table 2: holidays
-- Stores named public holidays; 'None' represents a regular day
CREATE TABLE IF NOT EXISTS holidays (
    holiday_id   INT AUTO_INCREMENT PRIMARY KEY,
    holiday_name VARCHAR(100) NOT NULL UNIQUE
);

-- Table 3: traffic_records
-- Core fact table, one row per hourly observation
CREATE TABLE IF NOT EXISTS traffic_records (
    record_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_time      DATETIME     NOT NULL,
    holiday_id     INT          NOT NULL,
    weather_id     INT          NOT NULL,
    temp           FLOAT        NOT NULL,
    rain_1h        FLOAT        NOT NULL DEFAULT 0,
    snow_1h        FLOAT        NOT NULL DEFAULT 0,
    clouds_all     TINYINT      NOT NULL,
    traffic_volume INT          NOT NULL,
    CONSTRAINT fk_holiday FOREIGN KEY (holiday_id) REFERENCES holidays(holiday_id),
    CONSTRAINT fk_weather FOREIGN KEY (weather_id) REFERENCES weather_conditions(weather_id),
    INDEX idx_datetime (date_time)
);

-- Seed holidays
INSERT INTO holidays (holiday_name) VALUES
    ('None'), ('Christmas Day'), ('New Years Day'), ('Thanksgiving Day'),
    ('Independence Day'), ('Labor Day'), ('Memorial Day'), ('Columbus Day'),
    ('Veterans Day'), ('Martin Luther King Jr Day'), ('Washington Birthday'), ('State Fair');

-- Seed weather conditions
INSERT INTO weather_conditions (weather_main, weather_description) VALUES
    ('Clouds', 'overcast clouds'), ('Clouds', 'broken clouds'),
    ('Clouds', 'scattered clouds'), ('Clouds', 'few clouds'),
    ('Clear', 'sky is clear'), ('Rain', 'light rain'),
    ('Rain', 'moderate rain'), ('Rain', 'heavy intensity rain'),
    ('Drizzle', 'light intensity drizzle'), ('Mist', 'mist'),
    ('Haze', 'haze'), ('Fog', 'fog'),
    ('Thunderstorm', 'thunderstorm with light rain'),
    ('Thunderstorm', 'proximity thunderstorm'),
    ('Snow', 'light snow'), ('Smoke', 'smoke'), ('Squall', 'squall');

-- Seed traffic records
INSERT INTO traffic_records (date_time, holiday_id, weather_id, temp, rain_1h, snow_1h, clouds_all, traffic_volume) VALUES
    ('2012-10-02 09:00:00', 1, 1, 288.28, 0,    0, 40, 5545),
    ('2012-10-02 10:00:00', 1, 1, 289.36, 0,    0, 75, 4516),
    ('2012-10-02 11:00:00', 1, 1, 289.58, 0,    0, 90, 4767),
    ('2012-10-02 12:00:00', 1, 1, 290.13, 0,    0, 90, 5026),
    ('2012-10-02 13:00:00', 1, 1, 291.14, 0,    0, 75, 4918),
    ('2012-10-02 14:00:00', 1, 2, 291.72, 0,    0, 1,  5181),
    ('2012-10-02 15:00:00', 1, 3, 293.17, 0,    0, 1,  5584),
    ('2012-10-02 16:00:00', 1, 4, 293.58, 0,    0, 1,  6460),
    ('2012-10-02 17:00:00', 1, 5, 294.07, 0,    0, 1,  6656),
    ('2012-10-02 18:00:00', 1, 5, 293.62, 0,    0, 1,  5932),
    ('2012-12-25 08:00:00', 2, 6, 272.15, 0.25, 0, 90, 1200),
    ('2012-12-25 12:00:00', 2, 5, 274.82, 0,    0, 20, 980),
    ('2012-12-25 17:00:00', 2, 5, 273.11, 0,    0, 10, 1560),
    ('2013-07-04 10:00:00', 5, 5, 300.15, 0,    0, 1,  2340),
    ('2013-07-04 15:00:00', 5, 3, 302.48, 0,    0, 20, 3100),
    ('2013-07-04 20:00:00', 5, 1, 297.33, 0,    0, 75, 2800),
    ('2013-11-28 08:00:00', 4, 9, 278.71, 0.1,  0, 90, 1100),
    ('2013-11-28 14:00:00', 4, 5, 281.22, 0,    0, 1,  1800),
    ('2018-09-03 09:00:00', 6, 4, 295.15, 0,    0, 1,  2200),
    ('2018-09-03 17:00:00', 6, 5, 293.82, 0,    0, 1,  2980);

-- Query 1: Average hourly traffic volume by hour of day
SELECT
    HOUR(date_time)               AS hour_of_day,
    ROUND(AVG(traffic_volume), 0) AS avg_traffic,
    COUNT(*)                      AS total_records
FROM traffic_records
GROUP BY HOUR(date_time)
ORDER BY hour_of_day;

-- Query 2: Traffic volume by holiday
SELECT
    h.holiday_name,
    ROUND(AVG(r.traffic_volume), 0) AS avg_traffic,
    MIN(r.traffic_volume)            AS min_traffic,
    MAX(r.traffic_volume)            AS max_traffic,
    COUNT(*)                         AS record_count
FROM traffic_records r
JOIN holidays h ON r.holiday_id = h.holiday_id
GROUP BY h.holiday_name
ORDER BY avg_traffic DESC;

-- Query 3: Average traffic by weather condition
SELECT
    w.weather_main,
    w.weather_description,
    ROUND(AVG(r.traffic_volume), 0) AS avg_traffic,
    COUNT(*)                         AS observations
FROM traffic_records r
JOIN weather_conditions w ON r.weather_id = w.weather_id
GROUP BY w.weather_main, w.weather_description
ORDER BY avg_traffic DESC;

-- Query 4: Latest record
SELECT r.*, h.holiday_name, w.weather_main, w.weather_description
FROM traffic_records r
JOIN holidays h           ON r.holiday_id = h.holiday_id
JOIN weather_conditions w ON r.weather_id = w.weather_id
ORDER BY r.date_time DESC
LIMIT 1;

-- Query 5: Records within a date range
SELECT r.record_id, r.date_time, r.traffic_volume,
       h.holiday_name, w.weather_main, r.temp, r.rain_1h, r.clouds_all
FROM traffic_records r
JOIN holidays h           ON r.holiday_id = h.holiday_id
JOIN weather_conditions w ON r.weather_id = w.weather_id
WHERE r.date_time BETWEEN '2012-10-02 00:00:00' AND '2012-10-02 23:59:59'
ORDER BY r.date_time;