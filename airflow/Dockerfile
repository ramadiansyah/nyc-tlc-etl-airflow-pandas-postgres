# Base: Official Apache Airflow image
FROM apache/airflow:2.9.1-python3.10

# Switch to root to install system dependencies
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy Airflow requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Optional: copy custom Python site config if needed
# COPY sitecustomize.py /usr/local/lib/python3.10/site-packages/


