FROM python:3.6.10 AS python_version

COPY requirements_gan.txt .
RUN pip install -U pip && pip install -r requirements_gan.txt


FROM apache/airflow:2.6.3-python3.8 as run

COPY --from=python_version /usr/local/bin/python /usr/local/bin/python3.6
COPY --from=python_version /usr/local/bin/pip* /usr/local/bin/
COPY --from=python_version /usr/local/bin/wheel /usr/local/bin/
COPY --from=python_version /usr/local/lib/lib* /usr/local/lib/

USER root

# Install required system libraries
RUN apt-get update && apt-get install -y \
    gcc \
    wget \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

## Download and Install TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

USER airflow

## Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir ta-lib "apache-airflow==${AIRFLOW_VERSION}"

## Set environment variables
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/:/usr/local/airflow/dags:/usr/local/airflow/plugins"

# Create necessary directories
RUN mkdir -p logs/{scheduler,webserver,worker,flower,redis,postgres}

# Copy the rest of the files
COPY . .
