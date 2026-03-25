# STEDI Human Balance Analytics – Data Lakehouse Project

## 📌 Project Overview

This project builds a data lakehouse solution using AWS services to support machine learning for the STEDI Step Trainer.

The goal is to process IoT sensor data and mobile app accelerometer data, apply privacy filtering, and create a curated dataset that can be used to train a machine learning model to detect human steps.

---

## 🏗️ Architecture

The pipeline follows a **multi-layer data lakehouse design**:

### Landing Zone (Raw Data)

Raw JSON data ingested into Amazon S3:

* `customer_landing`
* `accelerometer_landing`
* `step_trainer_landing`

### Trusted Zone (Cleaned Data)

Filtered and validated datasets:

* `customer_trusted` → only users who consented to research
* `accelerometer_trusted` → accelerometer data for consenting users
* `step_trainer_trusted` → valid step trainer data joined with curated customers

### Curated Zone (Business/ML Ready)

Final datasets for analytics and machine learning:

* `customers_curated` → valid customers with accelerometer data
* `machine_learning_curated` → final dataset combining sensor + accelerometer data

---

## 🔄 Data Pipeline Workflow

1. **Ingest raw JSON data into S3 landing zone**
2. **Create Glue tables and query via Athena**
3. **Filter customer data for research consent**
4. **Join accelerometer data with trusted customers**
5. **Resolve serial number issue using curated customers**
6. **Join step trainer and accelerometer data by timestamp**
7. **Produce machine learning dataset**

---

## 🔑 Key Transformations

### Privacy Filtering

Only customers who agreed to research are included:

```sql
WHERE sharewithresearchasofdate IS NOT NULL
```

### Accelerometer Join

```sql
accelerometer_landing.user = customer_trusted.email
```

### Customer Curation

Ensures only customers with accelerometer data are retained.

### Step Trainer Fix

Uses correct serial numbers from IoT data:

```sql
step_trainer_landing.serialnumber = customers_curated.serialnumber
```

### Final ML Join

```sql
step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp
```

---

## 📊 Final Output Dataset

The `machine_learning_curated` table contains:

* `sensorreadingtime`
* `serialnumber`
* `distancefromobject`
* `timestamp`
* `x`, `y`, `z` (accelerometer values)

This dataset is used to train machine learning models for step detection.

---

## 🧪 Data Validation

| Table                    | Expected Rows |
| ------------------------ | ------------- |
| customer_landing         | 956           |
| accelerometer_landing    | 81273         |
| step_trainer_landing     | 28680         |
| customer_trusted         | 482           |
| accelerometer_trusted    | 40981         |
| customers_curated        | 482           |
| step_trainer_trusted     | 14460         |
| machine_learning_curated | 43681         |

---

## 🛠️ Technologies Used

* AWS Glue (ETL with PySpark)
* AWS S3 (data lake storage)
* AWS Athena (SQL queries)
* Apache Spark
* Python

---

## 📂 Repository Structure

```
.
├── sql/                  # Athena SQL scripts
├── glue_jobs/           # Glue ETL scripts
├── screenshots/         # Query result screenshots
├── README.md
```

---

## 📸 Screenshots

Screenshots of Athena query results are included for:

* Landing tables
* Trusted tables
* Curated tables

---

## ✅ Summary

This project demonstrates:

* Building a scalable data lakehouse architecture
* Performing ETL using AWS Glue and Spark
* Enforcing data privacy constraints
* Preparing data for machine learning pipelines

---

## 🚀 Author

STEDI Data Engineering Project
AWS Data Engineering Nanodegree
