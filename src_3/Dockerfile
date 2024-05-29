FROM base_docker:latest

WORKDIR /app

COPY src_2/ .

RUN apt-get update && apt-get install -y \
    gcc \
    wget \
    build-essential  # Install necessary build tools

RUN wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz

RUN tar -xzf ta-lib-0.4.0-src.tar.gz
RUN ls -lah | grep ta-lib || echo "ta-lib not found!"
RUN cd ta-lib/ && ./configure --prefix=/usr && make && make install

RUN cd .. && rm -rf ta-lib ta-lib-0.4.0-src.tar.gz

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_ml.txt && \
    pip install --no-cache-dir ta-lib

ENTRYPOINT ["python", "rolling_model_estimation.py"]
