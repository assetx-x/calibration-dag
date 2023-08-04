FROM apache/airflow:2.6.3-python3.8
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
RUN pip install --no-cache-dir -r /requirements.txt
COPY . .
