# Start from the Apache Airflow image
FROM apache/airflow:2.6.3-python3.8

COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

COPY . .

#COPY ./dags/ \${AIRFLOW_HOME}/dags/
#COPY ./plugins/ \${AIRFLOW_HOME}/plugins/
