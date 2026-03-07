# Building a Pipeline for Time Series Data

A full end-to-end time-series pipeline built on the **Metro Interstate Traffic Volume** dataset (Kaggle). The project covers exploratory analysis, relational and document database design, a REST API, and an automated prediction script.

---

## Project Structure

```
.
├── dataset/
│   └── Metro_Interstate_Traffic_Volume.csv      # Source dataset
├── optimized_random_forest_model.joblib         # Pre-trained model saved from the notebook
├── traffic_volume_prediction.ipynb              # Task 1 – EDA, analysis & model experiments
├── schema.sql                                   # Task 2 – MySQL schema, seed data & queries
├── collection_design.js                         # Task 2 – MongoDB collection design & queries
├── app.py                                       # Task 3 – FastAPI REST API (MySQL + MongoDB)
├── predict.py                                   # Task 4 – Fetch → Preprocess → Predict pipeline
└── requirements.txt
```

---

## Dataset

**Metro Interstate Traffic Volume** — hourly westbound I-94 traffic data from 2012 to 2018.

| Column | Description |
|---|---|
| `date_time` | Hourly timestamp |
| `traffic_volume` | Vehicles per hour (prediction target) |
| `holiday` | Public holiday name or NaN |
| `temp` | Temperature in Kelvin |
| `rain_1h` | Rainfall in mm |
| `snow_1h` | Snowfall in mm |
| `clouds_all` | Cloud coverage (%) |
| `weather_main` | Weather category |
| `weather_description` | Detailed weather description |

Place the CSV at `dataset/Metro_Interstate_Traffic_Volume.csv` before running anything.

---

## Prerequisites

- Python 3.10+
- MySQL server running locally (database: `traffic_db`)
- MongoDB server running locally (default port 27017)

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment variables (optional)

Create a `.env` file in the project root to override the default database connections:

```env
MYSQL_URL=mysql+pymysql://root:@localhost:3306/traffic_db
MONGO_URL=mongodb://localhost:27017
MONGO_DB=traffic_db
MONGO_COLL=traffic_records
```

---

## Task 1 – EDA, Preprocessing & Model Training (Notebook)

Open and run `traffic_volume_prediction.ipynb` in Jupyter or Google Colab.

The notebook covers:

- **Understanding the dataset** — time range (Oct 2012 – Sep 2018), hourly granularity, missing value handling (holiday column filled with `"None"`), statistical distributions
- **Analytical questions** (5+), including:
  - Traffic trends over time
  - Holiday impact on traffic volume
  - 24-hour and 7-day moving averages
  - Lag effect analysis (1h, 24h, 168h, 720h lags)
  - Weather correlation with traffic
- **Model experiments** — three experiments compared:

| Model | MAE | RMSE |
|---|---|---|
| Linear Regression | 489.95 | 684.42 |
| Random Forest (initial) | 374.48 | 550.69 |
| Random Forest (tuned, GridSearchCV) | 377.81 | 544.88 |

Best hyperparameters: `n_estimators=100`, `max_depth=10`.

---

## Task 2 – Database Design

### MySQL

```bash
mysql -u root -p < schema.sql
```

This creates the `traffic_db` database with three tables:

- `holidays` — named public holidays
- `weather_conditions` — unique weather category + description pairs
- `traffic_records` — core fact table (one row per hourly observation)

Five queries are included at the bottom of `schema.sql`:
1. Average traffic by hour of day
2. Traffic by holiday
3. Average traffic by weather condition
4. Latest record
5. Records within a date range

### MongoDB

Run the commands in `collection_design.js` using `mongosh` or MongoDB Compass Shell:

```bash
mongosh < collection_design.js
```

This creates the `traffic_records` collection with schema validation, five indexes, and eight sample documents. Three aggregate queries are included:
1. Average traffic by hour of day
2. Holiday vs. normal day traffic
3. Average traffic by weather condition

---

## Task 3 – REST API

### Start the API

```bash
uvicorn app:app --reload --port 8001
```

Interactive docs: [http://localhost:8001/docs](http://localhost:8001/docs)

### Endpoints

#### MySQL CRUD (`/sql/traffic`)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/sql/traffic` | Insert a new record |
| `GET` | `/sql/traffic` | List records (paginated) |
| `GET` | `/sql/traffic/{id}` | Get record by ID |
| `PUT` | `/sql/traffic/{id}` | Update a record |
| `DELETE` | `/sql/traffic/{id}` | Delete a record |

#### MySQL Time-Series

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/sql/traffic/latest` | Most recent record |
| `GET` | `/sql/traffic/range?start=&end=` | Records within a date range |

#### MongoDB CRUD (`/mongo/traffic`)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/mongo/traffic` | Insert a new document |
| `GET` | `/mongo/traffic` | List documents (paginated) |
| `GET` | `/mongo/traffic/{id}` | Get document by ObjectId |
| `PUT` | `/mongo/traffic/{id}` | Update a document |
| `DELETE` | `/mongo/traffic/{id}` | Delete a document |

#### MongoDB Time-Series

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/mongo/traffic/latest` | Most recent document |
| `GET` | `/mongo/traffic/range?start=&end=` | Documents within a date range |

---

## Task 4 – Prediction Pipeline

The pre-trained model (`optimized_random_forest_model.joblib`) was saved directly from the notebook after GridSearchCV hyperparameter tuning. No retraining is needed.

Make sure the API is running, then:

```bash
python predict.py
```

**Pipeline:**

1. **Fetch** — pulls the 200 most recent records from `GET /mongo/traffic`
2. **Preprocess** — normalises document fields, computes `is_holiday`, `ma_24`, `ma_168`, lag features, one-hot encodes categoricals, and aligns columns with the training schema
3. **Load model** — loads `optimized_random_forest_model.joblib`; feature names are read directly from `model.feature_names_in_`
4. **Predict** — runs inference on the latest record and prints actual vs. predicted traffic volume

**Example output:**

```
=======================================================
  Task 4 - Traffic Volume Prediction Pipeline
=======================================================
[Step 1] Fetched 200 records from http://localhost:8001/mongo/traffic
[Step 3] Model loaded from 'optimized_random_forest_model.joblib'  (68 features)
[Step 2] Preprocessing 200 records...
         Feature matrix shape: (200, 68)

=======================================================
  Prediction Result
=======================================================
  Timestamp          : 2018-09-30T23:00:00
  Holiday            : None
  Weather            : Clouds - broken clouds
  Temperature (K)    : 287.15
  Actual volume      : 3241 vehicles/hour
  Predicted volume   : 3105 vehicles/hour
  Absolute error     : 136 vehicles/hour
=======================================================
```
