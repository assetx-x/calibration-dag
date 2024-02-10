FROM base_docker:latest

WORKDIR /app

COPY src_2/ .

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_ml.txt && \
    pip install --no-cache-dir ta-lib

ENTRYPOINT ["python", "/opt/airflow/plugins/intermediate_model_training.py"]
