FROM apache/airflow:2.6.3-python3.8

USER root

# Install required system libraries
RUN apt-get update && apt-get install -y \
    gcc \
    wget \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download and Install TA-Lib
RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

USER airflow

# Copy and install requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir ta-lib "apache-airflow==${AIRFLOW_VERSION}"

COPY requirements_gan.txt .
RUN unset PIP_USER \
  && python -m venv /opt/airflow/venv1 \
  # Provide your own requirements
  && /opt/airflow/venv1/bin/python -m pip install -r requirements_gan.txt

# Set environment variables
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/:/usr/local/airflow/dags:/usr/local/airflow/plugins"

# Create necessary directories
RUN mkdir -p logs/{scheduler,webserver,worker,flower,redis,postgres}

# Copy the rest of the files
COPY . .
