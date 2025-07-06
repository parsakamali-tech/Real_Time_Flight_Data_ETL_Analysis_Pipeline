# Real-Time Flight Data ETL & Analysis Pipeline

A full-cycle **Data Engineering project** that extracts live flight data via the [AviationStack API](https://aviationstack.com), transforms and cleans it using Python, stores it in a relational PostgreSQL database, and provides analytical insights using SQL and visualizations.

---

## Project Overview

This project simulates a real-world **ETL pipeline**, built entirely in Python and SQL, focused on ingesting real flight data and turning it into actionable insights.

Unlike client-based projects, this one is **self-designed** to demonstrate engineering capabilities across:

- API data ingestion
- Data cleaning and structuring
- Database schema design
- SQL-based analytics
- Outlier detection using machine learning

---

## Folder Structure

```

project/
├── database/
│   ├── sql/                    # SQL schemas and analytical queries
│   ├── tables\_data/           # CSVs used for DB load
│   ├──flights(api)\_data.csv/      # Cleaned main dataset
│   └── transformed\_data.csv
├── etl/
│   ├── extract.py             # Fetch data from API
│   ├── transform.py           # Clean and normalize
│   └── load.py                # Load to PostgreSQL
│
├── notebook/
│   ├── extract.ipynb
│   ├── transform.ipynb
│   ├── load.ipynb
│   └── analysis.ipynb
│
├── visuals/
│   ├── Airports_Highest_Avg_Delay.jpg
│   ├── Airports_Highest_Avg_Flight_Duration.jpg
│   ├──Delay_Hour.jpg
|   ├──Flight_Status_Percentage.jpg
|   └──IATA_Number_Of_Flights.jpg
├── README.md
|
└──requirements.txt

```

---

## Tech Stack

| Layer       | Tools Used                                |
|-------------|-------------------------------------------|
| Extract     | `requests`, `json`, AviationStack API     |
| Transform   | `pandas`, `datetime`, `pyod`              |
| Load        | `psycopg2`, `PostgreSQL`, `SQL`           |
| Analytics   | `SQL`, `pandas`, `matplotlib`             |
| Execution   | Manually run Python scripts / notebooks   |

---

## Execution Approach

The ETL pipeline is **modular and manually executable**. Each phase—`extract`, `transform`, and `load`—is handled via separate Python files and complementary Jupyter notebooks.

No automated scheduling system (like cron or Airflow) is used in this version; however, the structure supports easy extension in future.

---

## Why PyOD Was Used

Detecting **anomalies in flight durations** is a key part of this project. To do so, the [`PyOD`](https://pyod.readthedocs.io/en/latest/) library was employed.

Steps taken:

- Extracted `flight_duration` values post-cleaning
- Applied a `KNN` model from PyOD to detect outliers
- Added a `flight_duration_outlier` column to flag anomalies (1 = outlier)
- Used this in analysis to find airlines/routes with most suspicious patterns

This simulates a real-world anomaly detection system in monitoring pipelines.

---

## Sample Analytical Questions

- What percentage of flights are in the "active" status?
- Which departure airports (departure_airport) have the highest average delay (departure_delay)?
- Which airports have the highest average flight duration (flight_duration)?
- Flight delays at different hours.
- The largest number of flights from the departure airport with IATA code.

These were answered via optimized SQL queries over the PostgreSQL database.

---

## What This Project Demonstrates

✔️ Real-world API interaction  
✔️ End-to-end ETL logic in Python  
✔️ Normalized relational schema in PostgreSQL  
✔️ Use of PyOD for flight anomaly detection  
✔️ SQL analytical thinking  
✔️ Clean and modular code structure  
✔️ Focus on reproducibility and scalability

---

## What's Not Included

- No automated orchestration tools like cron/airflow (scripts are run manually)
- No dashboard or UI integration (focus was on backend ETL + analytics)

---

## Requirements

Install the necessary Python packages:
    ```bash pip install -r requirements.txt```

---

## Author

**Parsa Kamali Shahry**  
 Aspiring Data Engineer  
 GitHub: [github.com/yourusername](https://github.com/yourusername)  
 Email: parsakamlibsns@outlook.com

---

> ⭐ *If this project inspires or helps you, feel free to star it on GitHub!*
