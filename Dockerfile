FROM apache/airflow:2.6.3-python3.8

ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/:/usr/local/airflow/dags:/usr/local/airflow/plugins"

USER root

RUN apt-get update && apt-get install -y \
    make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget ca-certificates curl llvm \
    libncurses5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev mecab-ipadic-utf8 git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


RUN curl -L http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz | tar xvz && \
    cd ta-lib/ && ./configure --prefix=/usr && make && make install && cd .. && rm -rf ta-lib

RUN mkdir -p /.pyenv && chown airflow /.pyenv

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -q --no-cache-dir -r requirements.txt && \
    pip install -q --no-cache-dir ta-lib "apache-airflow==${AIRFLOW_VERSION}"

WORKDIR /app
RUN mkdir -p logs/{scheduler,webserver,worker,flower,redis,postgres}

COPY . .