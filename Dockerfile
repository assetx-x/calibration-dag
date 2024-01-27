# Sets base image and role
FROM apache/airflow:2.6.3-python3.8 as run

# Switches user
USER root

# Updates, installs required system libraries, and cleans up afterwards
RUN apt-get update && apt-get install -y \
    make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget ca-certificates curl llvm \
    libncurses5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev mecab-ipadic-utf8 git && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

## Downloads and installs TA-Lib
RUN curl -L http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz | tar xvz && \
    cd ta-lib/ && ./configure --prefix=/usr && make && make install && cd .. && rm -rf ta-lib

# Switches user
USER airflow

# Installs GAN requirements using pyenv
ENV LDFLAGS ''
RUN git clone https://github.com/pyenv/pyenv.git $HOME/pyenv
ENV PYENV_ROOT $HOME/pyenv
RUN /pyenv/bin/pyenv install 3.6.10
RUN eval "$(/pyenv/bin/pyenv init -)" && /pyenv/bin/pyenv local 3.6.10
#    && pip install numpy poetry setuptools wheel six auditwheel

#RUN #curl https://pyenv.run | bash && \
#    echo 'export PATH="$HOME/.pyenv/bin:$PATH"' >> ~/.bashrc && \
#    echo 'eval "$(pyenv init --path)"' >> ~/.bashrc && \
#    echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc

# Set-up necessary Env vars for PyEnv
#ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

# Install python 3.6.10
RUN exec "$BASH" ; echo "[!] Exec BASH: $?" ; \
    source ~/.bashrc ; echo "[!] Source bashrc $?" ; \
    pyenv install -v 3.6.10 ; echo "[!] Pyenv install: $?"

# Copies and installs requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -q --no-cache-dir -r requirements.txt && \
    pip install -q --no-cache-dir ta-lib "apache-airflow==${AIRFLOW_VERSION}"

# Sets environment variables and creates necessary directories
WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:/usr/local/airflow/:/usr/local/airflow/dags:/usr/local/airflow/plugins"
RUN mkdir -p logs/{scheduler,webserver,worker,flower,redis,postgres}

# Copies the rest of the files
COPY . .