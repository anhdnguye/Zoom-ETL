# Zoom Data Pipeline with Apache Airflow

This project provides a scalable, event-driven data pipeline that integrates **Zoom**, **AWS**, and **Dropbox** to orchestrate and manage user and meeting data and recordings. The pipeline uses **Apache Airflow** (via Docker) for scheduled ETL tasks and **AWS Lambda** for near real-time ingestion of Zoom recordings via webhooks.

## Architecture Overview

* **AWS Lambda** handles Zoom recording webhooks and uploads files to S3 in near real-time.
* **Apache Airflow** orchestrates the ETL workflow using Python scripts.
* **AWS RDS (PostgreSQL)** stores user, meeting, participant, and recording metadata.
* **AWS S3** holds raw recording files.
* **Dropbox** (optional) is used for watching recording on the fly.
* **Power BI** connects to RDS for business intelligence and reporting.


## Data Pipeline Workflow

### Real-Time Recording Ingestion (via AWS Lambda)

1. **Zoom Webhook** triggers on meeting recording completion.
2. **AWS Lambda** processes the event:
- Selects the needed recording types.
- Stores raw data in **AWS S3**.
- Builds and uploads the recording to **Dropbox**.
- Sends metadata such as recording info, S3/Dropbox links to **RDS** via direct DB insert.


### Scheduled Metadata ETL (via Apache Airflow)

1. User Email Retrieval: Retrieves all user emails from Zoom API.
2. User & Meeting Data Extraction: For each user, fetches user metadata and only meetings since the last pipeline run.
3. Meeting Details Collection: For each meeting, extracts meeting metadata and participant metadata.
4. Metadata Storage: Stores all structured data in **AWS RDS (PostgreSQL)**.

## Technology Stack

* Zoom API
* Python, Docker, Apache Airflow
* AWS Lambda, RDS (PostgreSQL), S3
* Dropbox API
* Power BI