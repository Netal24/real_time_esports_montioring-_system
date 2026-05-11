# Real Time Esports Monitoring System

A real-time cricket analytics pipeline built using **Apache Kafka**, **Python**, **PostgreSQL**, and **Apache Superset**.  
This project simulates live IPL match events, streams them through Kafka, stores them in PostgreSQL, and visualizes live analytics dashboards using Superset.

---

# 🚀 Project Overview

This project demonstrates how modern real-time data engineering systems work using streaming architecture.

The pipeline includes:

1. **Producer (`producer.py`)**
   - Simulates live IPL match events
   - Generates ball-by-ball cricket data
   - Sends events to a Kafka topic

2. **Kafka Broker**
   - Acts as a real-time message queue
   - Transfers cricket events from producer to consumer

3. **Consumer (`consumer.py`)**
   - Reads streaming data from Kafka
   - Inserts records into PostgreSQL database

4. **PostgreSQL Database**
   - Stores structured match statistics

5. **Apache Superset**
   - Connects with PostgreSQL
   - Creates real-time cricket dashboards and analytics

---

# 🛠 Tech Stack

- Python
- Apache Kafka
- Docker
- PostgreSQL
- Apache Superset
- psycopg2
- JSON
- kafka-python

---

# 📂 Project Structure

```bash
├── producer.py
├── consumer.py
├── docker-compose.yml
├── esports_db_jocg.sql
└── README.md
```

---

# ⚙️ Features

## ✅ Real-Time Match Simulation

- Simulates multiple IPL matches simultaneously
- Generates:
  - Runs
  - Wickets
  - Fours
  - Sixes
  - Balls faced
  - Event timestamps

---

## ✅ Team Batting Styles

Different teams have different batting behaviors:

| Style | Characteristics |
|---|---|
| Aggressive | More fours & sixes |
| Balanced | Mixed gameplay |
| Defensive | More dots & singles |

---

## ✅ Kafka Streaming

- Uses Kafka Producer & Consumer
- Streams live cricket events
- Mimics real-time sports data pipelines

---

## ✅ PostgreSQL Storage

Stores events inside:

```sql
match_stats
```

Table columns:

- match_id
- player
- team
- runs
- balls
- wickets
- fours
- sixes
- event_time

---

## ✅ Apache Superset Dashboard

Connected PostgreSQL database with Apache Superset for live analytics.

Dashboard features include:

- Live Match Score Tracking
- Team-wise Total Runs
- Player Performance Analysis
- Boundary Distribution
- Wicket Statistics
- Match Comparison
- Real-Time Charts & Graphs

---

# 🧠 System Architecture

```text
Producer.py
    ↓
Apache Kafka Topic ("cricket")
    ↓
Consumer.py
    ↓
PostgreSQL Database
    ↓
Apache Superset Dashboard
```

---

# 🐳 Docker Setup

Kafka and Zookeeper run using Docker Compose.

## Start Kafka

```bash
docker-compose up -d
```

## Verify Running Containers

```bash
docker ps
```

---

# ▶️ How to Run the Project

## 1️⃣ Install Dependencies

```bash
pip install kafka-python psycopg2
```

---

## 2️⃣ Start Kafka

```bash
docker-compose up -d
```

---

## 3️⃣ Run Producer

```bash
python producer.py
```

This starts live IPL event generation.

---

## 4️⃣ Run Consumer

Open another terminal:

```bash
python consumer.py
```

This reads Kafka events and stores them in PostgreSQL.

---

## 5️⃣ Connect PostgreSQL to Superset

- Open Apache Superset
- Add PostgreSQL Database Connection
- Connect using database credentials
- Import `match_stats` table
- Create charts and dashboards

---

# 📊 Example Streaming Output

```text
[  12] IPL_2026_M01 | Virat (RCB) 4️⃣  4 runs
[  13] IPL_2026_M02 | Gill (GT) →  1 runs
[  14] IPL_2026_M03 | Warner (DC) ❌  0 runs
```

---

# 🗄 Example Database Record

| match_id | player | team | runs | wickets | event_time |
|---|---|---|---|---|---|
| IPL_2026_M01 | Virat | RCB | 4 | 0 | 2026-05-11 18:30:01 |

---

# 📈 Future Improvements

- Spark Streaming Integration
- Real IPL API Integration
- Player Strike Rate Analytics
- Match Win Prediction
- Real-Time Leaderboards
- Airflow Scheduling
- AWS/GCP Deployment
- Machine Learning Based Match Insights

---

# 🎯 Learning Outcomes

This project helps understand:

- Real-time Data Streaming
- Event-Driven Architecture
- Kafka Producers & Consumers
- Database Integration
- Real-Time Dashboarding
- Apache Superset Visualization
- Data Engineering Basics
- Sports Analytics Pipelines

---

# 👨‍💻 Author

Built as a Real-Time Data Engineering & Analytics Project using Python, Kafka, PostgreSQL, and Apache Superset.
