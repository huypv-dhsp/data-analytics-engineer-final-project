FROM apache/airflow:2.5.0-python3.10
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt --no-cache-dir

