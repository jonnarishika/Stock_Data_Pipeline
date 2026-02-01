#  Real-Time Stock Market Data Pipeline (Kafka → Spark → PostgreSQL → Power BI)

A complete end-to-end **real-time stock market streaming pipeline** built using modern Data Engineering tools.

This project ingests live stock prices from the **Finnhub API**, streams them through **Apache Kafka**, processes them with **Spark Structured Streaming**, stores them in **PostgreSQL** as a warehouse, and visualizes them in a **Power BI dashboard**.

---

##  Project Architecture
<img width="409" height="681" alt="image" src="https://github.com/user-attachments/assets/c8768900-8242-4797-91b9-e53a9b2d2aaa" />

##  Key Features

1. Live stock market ingestion (AAPL, MSFT, TSLA)
2. Kafka topic-based real-time streaming
3. Spark Structured Streaming transformations
4. Continuous warehouse loading into PostgreSQL
5. Power BI dashboard with hourly trend analytics
6. Fully Dockerized environment  

---

##  Tech Stack

| Layer | Tool |
|------|------|
| Data Source | Finnhub Stock API |
| Messaging | Apache Kafka |
| Processing | Apache Spark Structured Streaming |
| Storage | PostgreSQL Data Warehouse |
| Visualization | Power BI Dashboard |
| Deployment | Docker + Docker Compose |


##  How to Run the Project

###  Start All Services

Start the complete Docker-based pipeline:
```bash
docker-compose up -d
```

This launches:
* Zookeeper
* Kafka Broker
* Spark
* PostgreSQL Warehouse

Verify containers:
```bash
docker ps
```

###  Run Kafka Producer (API → Kafka)

Start the stock price producer:
```bash
python ingestion/kafka_producer.py
```

This continuously publishes live stock price data to the Kafka topic:
```
stock_prices
```

###  Run Spark Streaming Job (Kafka → PostgreSQL)

Enter the Spark container:
```bash
docker exec -it spark bash
```

Run the Spark streaming application:
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
  streaming/kafka_to_postgres.py
```

This continuously writes processed records into the PostgreSQL table:
```
stock_prices
```

###  Verify Data in PostgreSQL

Access PostgreSQL inside Docker:
```bash
docker exec -it warehouse_postgres psql -U stock_user -d stock_db
```

Run validation queries:
```sql
SELECT COUNT(*) FROM stock_prices;
SELECT * FROM stock_prices LIMIT 5;
```

---

##  Power BI Dashboard

Power BI connects directly to the PostgreSQL warehouse.

**Dashboard Features:**
* Latest stock price KPI cards
* Stock symbol filter (AAPL / MSFT / TSLA)
* Aggregated analytics tables

 **Screenshot:** <img width="1146" height="344" alt="image" src="https://github.com/user-attachments/assets/fee352b2-2275-41d4-8358-42fd09455047" />


---
