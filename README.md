# Zoom Data Pipeline with Apache Airflow

This project provides a scalable, containerized data pipeline built with Docker and Apache Airflow to orchestrate the extraction, transformation, and loading (ETL) of Zoom data. It integrates with AWS services such as RDS, S3, and optionally Dropbox, enabling both real-time access to recordings and daily backups of metadata for analytics.

## Architecture Overview

<ul>
<li>**AWS Lambda** handles Zoom recording webhooks and uploads files to S3 in near real-time.</li>
<li>**Apache Airflow** orchestrates the ETL workflow using Python scripts.</li>
<li>**AWS RDS (PostgreSQL)** stores user, meeting, participant, and recording metadata.</li>
<li>**AWS S3** holds raw recording files.</li>
<li>**Dropbox** (optional) is used for watching recording on the fly.</li>
<li>**Power BI** connects to RDS for business intelligence and reporting.</li>
</ul>

## Data Pipeline Workflow

# Real-Time Recording Ingestion (via AWS Lambda)
<ol>
<li>**Zoom Webhook** triggers on meeting recording completion.</li>
<li>**AWS Lambda** processes the event:</li>
</ol>

# Scheduled Metadata ETL (via Apache Airflow)
<ol>
<li>User Email Retrieval</li>
<li>User & Meeting Data Extraction</li>
<li>User Details Collection</li>
<li>Meeting Details Collection</li>
<li>Metadata Storage</li>
</ol>

## Technology Stack
<ul>
<li>Python, Docker, Apache Airflow</li>
<li>AWS Lambda, RDS (PostgreSQL), S3</li>
<li>Dropbox API</li>
<li>Power BI</li>
</ul>