FROM base_docker:latest

WORKDIR /app

COPY src_6/ .
COPY plugins/ .

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements.txt

ENTRYPOINT ["python", "factor_interpretation.py"]
