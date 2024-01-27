# Sets base image and role
FROM apache/airflow:2.6.3-python3.8 as run

# Switches user
USER root

# Updates, installs required system libraries, and cleans up afterwards
RUN apt-get update && apt-get install -y \
    build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev curl llvm libncurses5-dev \
    libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python3-openssl git gcc mecab-ipadic-utf8 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Downloads and installs TA-Lib
RUN curl -L http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz | tar xvz && \
    cd ta-lib/ && ./configure --prefix=/usr && make && make install && cd .. && rm -rf ta-lib 

# Installs GAN requirements using pyenv
RUN curl https://pyenv.run | bash && \
    echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> /home/airflow/.bashrc && \
    echo 'eval "$(pyenv init --path)"' >> /home/airflow/.bashrc && \
    echo 'eval "$(pyenv virtualenv-init -)"' >> /home/airflow/.bashrc

RUN exec "$BASH" && source /home/airflow/.bashrc && pyenv install -v 3.6.10

RUN groupadd airflow && \
    chown airflow:airflow /home/airflow/.bashrc && \
    chmod 644 /home/airflow/.bashrc

# Switches user
USER airflow

# Copies and installs requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir ta-lib "apache-airflow==${AIRFLOW_VERSION}"

# Sets environment variables and creates necessary directories
WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/:/usr/local/airflow/dags:/usr/local/airflow/plugins"
RUN mkdir -p logs/{scheduler,webserver,worker,flower,redis,postgres}

# Copies the rest of the files
COPY . .