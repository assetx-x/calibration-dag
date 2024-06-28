FROM base_docker:latest

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    wget \
    build-essential && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN wget -q http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz && \
    tar -xzf ta-lib-0.4.0-src.tar.gz && \
    cd ta-lib/ && \
    ./configure --prefix=/usr && \
    make && \
    make install && \
    cd .. && \
    rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

COPY src/ .
COPY plugins/ .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements_gan.txt

ENTRYPOINT ["python", "get_raw_prices_step.py"]
