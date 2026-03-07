// Metro Interstate Traffic Volume - MongoDB Collection Design
// Run these commands in mongosh or MongoDB Compass Shell

// Collection: traffic_records
// One document per hourly observation. Weather is embedded (denormalised)
// for optimised time-series reads.

db.createCollection("traffic_records", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["date_time", "traffic_volume", "weather", "temp_kelvin"],
      properties: {
        date_time:      { bsonType: "date",   description: "Hourly timestamp (UTC)" },
        holiday:        { bsonType: "string", description: "Holiday name or 'None'" },
        is_holiday:     { bsonType: "bool",   description: "True if a public holiday" },
        temp_kelvin:    { bsonType: "double", description: "Temperature in Kelvin" },
        temp_celsius:   { bsonType: "double", description: "Derived Celsius value" },
        rain_1h_mm:     { bsonType: "double", description: "Rainfall in mm" },
        snow_1h_mm:     { bsonType: "double", description: "Snowfall in mm" },
        clouds_pct:     { bsonType: "int",    description: "Cloud coverage percentage" },
        traffic_volume: { bsonType: "int",    description: "Vehicles per hour" },
        weather: {
          bsonType: "object",
          required: ["main", "description"],
          properties: {
            main:        { bsonType: "string" },
            description: { bsonType: "string" }
          }
        },
        time_features: {
          bsonType: "object",
          properties: {
            year:        { bsonType: "int" },
            month:       { bsonType: "int" },
            day:         { bsonType: "int" },
            hour:        { bsonType: "int" },
            day_of_week: { bsonType: "int" },
            is_weekend:  { bsonType: "bool" }
          }
        }
      }
    }
  }
})

// Indexes for efficient time-series queries
db.traffic_records.createIndex({ date_time: 1 },                       { name: "idx_datetime" })
db.traffic_records.createIndex({ "time_features.hour": 1 },            { name: "idx_hour" })
db.traffic_records.createIndex({ "weather.main": 1 },                  { name: "idx_weather" })
db.traffic_records.createIndex({ is_holiday: 1 },                      { name: "idx_holiday" })
db.traffic_records.createIndex({ date_time: -1, traffic_volume: -1 },  { name: "idx_latest" })

// Sample documents
db.traffic_records.insertMany([
  {
    date_time:      ISODate("2012-10-02T09:00:00Z"),
    holiday:        "None",
    is_holiday:     false,
    temp_kelvin:    288.28,
    temp_celsius:   15.13,
    rain_1h_mm:     0.0,
    snow_1h_mm:     0.0,
    clouds_pct:     40,
    traffic_volume: 5545,
    weather: { main: "Clouds", description: "scattered clouds" },
    time_features: { year: 2012, month: 10, day: 2, hour: 9, day_of_week: 1, is_weekend: false }
  },
  {
    date_time:      ISODate("2012-10-02T17:00:00Z"),
    holiday:        "None",
    is_holiday:     false,
    temp_kelvin:    294.07,
    temp_celsius:   20.92,
    rain_1h_mm:     0.0,
    snow_1h_mm:     0.0,
    clouds_pct:     1,
    traffic_volume: 6656,
    weather: { main: "Clear", description: "sky is clear" },
    time_features: { year: 2012, month: 10, day: 2, hour: 17, day_of_week: 1, is_weekend: false }
  },
  {
    date_time:      ISODate("2012-12-25T12:00:00Z"),
    holiday:        "Christmas Day",
    is_holiday:     true,
    temp_kelvin:    274.82,
    temp_celsius:   1.67,
    rain_1h_mm:     0.0,
    snow_1h_mm:     0.0,
    clouds_pct:     20,
    traffic_volume: 980,
    weather: { main: "Clear", description: "sky is clear" },
    time_features: { year: 2012, month: 12, day: 25, hour: 12, day_of_week: 1, is_weekend: false }
  },
  {
    date_time:      ISODate("2013-07-04T15:00:00Z"),
    holiday:        "Independence Day",
    is_holiday:     true,
    temp_kelvin:    302.48,
    temp_celsius:   29.33,
    rain_1h_mm:     0.0,
    snow_1h_mm:     0.0,
    clouds_pct:     20,
    traffic_volume: 3100,
    weather: { main: "Clouds", description: "scattered clouds" },
    time_features: { year: 2013, month: 7, day: 4, hour: 15, day_of_week: 3, is_weekend: false }
  },
  {
    date_time:      ISODate("2013-11-28T08:00:00Z"),
    holiday:        "Thanksgiving Day",
    is_holiday:     true,
    temp_kelvin:    278.71,
    temp_celsius:   5.56,
    rain_1h_mm:     0.1,
    snow_1h_mm:     0.0,
    clouds_pct:     90,
    traffic_volume: 1100,
    weather: { main: "Drizzle", description: "light intensity drizzle" },
    time_features: { year: 2013, month: 11, day: 28, hour: 8, day_of_week: 3, is_weekend: false }
  },
  {
    date_time:      ISODate("2016-03-15T08:00:00Z"),
    holiday:        "None",
    is_holiday:     false,
    temp_kelvin:    277.51,
    temp_celsius:   4.36,
    rain_1h_mm:     2.5,
    snow_1h_mm:     0.0,
    clouds_pct:     100,
    traffic_volume: 4200,
    weather: { main: "Rain", description: "moderate rain" },
    time_features: { year: 2016, month: 3, day: 15, hour: 8, day_of_week: 1, is_weekend: false }
  },
  {
    date_time:      ISODate("2018-01-10T07:00:00Z"),
    holiday:        "None",
    is_holiday:     false,
    temp_kelvin:    263.15,
    temp_celsius:   -10.0,
    rain_1h_mm:     0.0,
    snow_1h_mm:     1.2,
    clouds_pct:     100,
    traffic_volume: 2800,
    weather: { main: "Snow", description: "light snow" },
    time_features: { year: 2018, month: 1, day: 10, hour: 7, day_of_week: 2, is_weekend: false }
  },
  {
    date_time:      ISODate("2018-09-03T17:00:00Z"),
    holiday:        "Labor Day",
    is_holiday:     true,
    temp_kelvin:    293.82,
    temp_celsius:   20.67,
    rain_1h_mm:     0.0,
    snow_1h_mm:     0.0,
    clouds_pct:     1,
    traffic_volume: 2980,
    weather: { main: "Clear", description: "sky is clear" },
    time_features: { year: 2018, month: 9, day: 3, hour: 17, day_of_week: 0, is_weekend: false }
  }
])

// Query 1: Average traffic volume by hour of day
db.traffic_records.aggregate([
  { $group: {
      _id:         "$time_features.hour",
      avg_traffic: { $avg: "$traffic_volume" },
      count:       { $sum: 1 }
  }},
  { $sort: { _id: 1 } },
  { $project: { hour_of_day: "$_id", avg_traffic: { $round: ["$avg_traffic", 0] }, count: 1, _id: 0 } }
])

// Query 2: Holiday vs non-holiday average traffic
db.traffic_records.aggregate([
  { $group: {
      _id:         "$is_holiday",
      avg_traffic: { $avg: "$traffic_volume" },
      min_traffic: { $min: "$traffic_volume" },
      max_traffic: { $max: "$traffic_volume" },
      count:       { $sum: 1 }
  }},
  { $project: {
      day_type:    { $cond: ["$_id", "Holiday", "Normal Day"] },
      avg_traffic: { $round: ["$avg_traffic", 0] },
      min_traffic: 1,
      max_traffic: 1,
      count:       1,
      _id:         0
  }}
])

// Query 3: Average traffic by weather condition
db.traffic_records.aggregate([
  { $group: {
      _id:          { main: "$weather.main", description: "$weather.description" },
      avg_traffic:  { $avg: "$traffic_volume" },
      observations: { $sum: 1 }
  }},
  { $sort: { avg_traffic: -1 } },
  { $project: {
      weather_main:        "$_id.main",
      weather_description: "$_id.description",
      avg_traffic:         { $round: ["$avg_traffic", 0] },
      observations:        1,
      _id:                 0
  }}
])

// Query 4: Latest record
db.traffic_records.findOne({}, { sort: { date_time: -1 } })

// Query 5: Records by date range
db.traffic_records.find({
  date_time: {
    $gte: ISODate("2012-10-02T00:00:00Z"),
    $lte: ISODate("2012-10-02T23:59:59Z")
  }
}).sort({ date_time: 1 })