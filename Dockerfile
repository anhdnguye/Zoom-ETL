# Use the official Airflow base image
FROM apache/airflow:3.0.0-python3.12

# Switch to root to install system packages (if needed)
USER root

# Install system-level dependencies if needed (optional)
RUN apt-get update && apt-get install -y build-essential libpq-dev

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt