FROM base_docker:latest

WORKDIR /app

COPY src_6/ .
COPY plugins/ .

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y libgomp1

ENTRYPOINT ["python", "factor_interpretation.py"]
