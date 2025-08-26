# 📊 Real-Time Log Analytics System  

## 🚀 Overview  

The **Real-Time Log Analytics System** ingests, processes, and analyzes logs in **real time** from **Kafka** and **MongoDB**, enriches them, and stores the results in **PostgreSQL** for downstream analytics and visualization.  

🔹 Key features:  
- ⏱️ Real-time **log monitoring**  
- 🚨 **Error tracking & alerting**  
- ⚡ **Performance analysis** (latency, request status, throughput)  
- 📊 Professional **visualizations** with **MongoDB Atlas Charts / Grafana / Superset**  

---

## 🏗️ Architecture  

```text
Kafka (Streaming Logs) ----\
                            >---- Spark Structured Streaming ----> PostgreSQL (Analytics DB) ----> Grafana ----> Alerts
MongoDB (Historical Logs) --/                      
````

![Architecture](docs/architecture.png)

---

## 📂 Project Structure

```text
src/main/scala/com/loganalytics/
  ├── controller/
  │    └── LogStreamController.scala   # Main streaming job
  ├── dao/
  │    ├── KafkaDAO.scala              # Kafka ingestion
  │    ├── MongoDAO.scala              # Mongo ingestion + metadata
  │    └── PostgresDAO.scala           # Write/Read PostgreSQL
  ├── service/
  │    ├── LogParserService.scala      # Kafka JSON → schema
  │    ├── RawLogService.scala         # Normalize schema
  │    ├── MongoLogService.scala       # Mongo schema transformer
  │    ├── EnrichmentService.scala     # Add service labels
  │    └── UnionService.scala          # Union live & historical data
  └── utils/
       └── SchemaUtils.scala           # Unified schema definitions
```

---


## ⚙️ Tech Stack

| Technology                                                                                            | Purpose                      |
| ----------------------------------------------------------------------------------------------------- | ---------------------------- |
| ![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apache-kafka\&logoColor=white)       | Real-time log ingestion      |
| ![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?logo=mongodb\&logoColor=white)                 | Cloud storage + metadata     |
| ![Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark\&logoColor=white)        | ETL + enrichment + analytics |
| ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?logo=postgresql\&logoColor=white)        | Analytics-ready sink         |
| ![Scala](https://img.shields.io/badge/Scala-DC322F?logo=scala\&logoColor=white)                       | Data pipeline implementation |
| ![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker\&logoColor=white)                    | Deployment (optional)        |
| ![Charts](https://img.shields.io/badge/MongoDB%20Atlas%20Charts-00ED64?logo=mongodb\&logoColor=white) | Visualization                |

---

## 📥 Setup & Installation

### 1️⃣ Clone the repo

```bash
git clone https://github.com/your-username/Real-Time-Log-Analytics-System.git
cd Real-Time-Log-Analytics-System
```

### 2️⃣ Configure connections

Edit `src/main/resources/application.conf`:

### 3️⃣ Start services

* ▶️ Run **Kafka broker + Zookeeper**
* 🐘 Start **PostgreSQL instance**
* ☁️ Ensure **MongoDB Atlas** cluster is reachable

### 4️⃣ Build & Run

```bash
sbt clean package
spark-submit \
  --class com.loganalytics.controller.LogStreamController \
  --master local[*] \
  target/scala-2.12/realtime-log-analytics-system_2.12-0.1.jar
```

---

## 📊 Visualization

### MongoDB Atlas Charts

* Import **logs** collection into Atlas Charts
* Example dashboard:

![Atlas Dashboard](docs/dashboard.png)


---

### Example Queries

**Logs per service by hour**

```mongodb
[{
  $group: {
    _id: { service: "$service", hour: { $hour: "$event_time" } },
    count: { $sum: 1 }
  }
}]
```

**Error rate trend**

```mongodb
[{
  $match: { level: "ERROR" }
}, {
  $group: {
    _id: { service: "$service", hour: { $hour: "$event_time" } },
    errors: { $sum: 1 }
  }
}]
```

---

## 🚨 Alerts (Optional Extension)

Define **business rules** such as:

* ❌ **Error % > 5% in 10 minutes** → trigger alert
* 🕑 **Latency > 2s** for critical services → notify Slack/email

---


## 🤝 Contributing

1. 🍴 Fork it
2. 🌱 Create your feature branch (`git checkout -b feature/foo`)
3. ✅ Commit your changes (`git commit -m 'Add foo'`)
4. 🚀 Push to branch (`git push origin feature/foo`)
5. 🔁 Open a Pull Request

---

## 📜 License

MIT License © 2025 \[Your Name]
