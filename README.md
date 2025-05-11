# Zoom Data Pipeline with Apache Airflow

This project provides a scalable, containerized data pipeline built with Docker and Apache Airflow to orchestrate the extraction, transformation, and loading (ETL) of Zoom data. It integrates with AWS services such as RDS, S3, and optionally Dropbox, enabling both real-time access to recordings and daily backups of metadata for analytics.

## Architecture Overview

* **AWS Lambda** handles Zoom recording webhooks and uploads files to S3 in near real-time.
* **Apache Airflow** orchestrates the ETL workflow using Python scripts.
* **AWS RDS (PostgreSQL)** stores user, meeting, participant, and recording metadata.
* **AWS S3** holds raw recording files.
* **Dropbox** (optional) is used for watching recording on the fly.
* **Power BI** connects to RDS for business intelligence and reporting.


## Data Pipeline Workflow

# Real-Time Recording Ingestion (via AWS Lambda)

1. **Zoom Webhook** triggers on meeting recording completion.
2. **AWS Lambda** processes the event:


# Scheduled Metadata ETL (via Apache Airflow)

1. User Email Retrieval
2. User & Meeting Data Extraction
3. User Details Collection
4. Meeting Details Collection
5. Metadata Storage

## Technology Stack

* Python, Docker, Apache Airflow
* AWS Lambda, RDS (PostgreSQL), S3
* Dropbox API
* Power BI