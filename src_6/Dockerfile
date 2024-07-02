FROM base_docker:latest

WORKDIR /app

COPY src_6/ .
COPY plugins/ .

# Install system dependencies
RUN apt-get update && \
    apt-get install -y gcc libgomp1 && \
    apt-get clean

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "factor_interpretation.py"]
