FROM apache/airflow:2.6.3-python3.8

USER root

# Install required system libraries
RUN apt-get update && apt-get install -y \
    gcc \
    wget \
    build-essential

# Download and Install TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -r ta-lib && \
    rm ta-lib-0.4.0-src.tar.gz

USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir ta-lib
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/:/usr/local/airflow/dags:/usr/local/airflow/plugins"
RUN mkdir -p logs/scheduler logs/webserver logs/worker logs/flower logs/redis logs/postgres
COPY . .

