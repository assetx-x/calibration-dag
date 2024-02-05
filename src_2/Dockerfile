FROM python:3.6.10

WORKDIR /app

COPY src_2/ .
RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_ml.txt

COPY plugins/ .

ENV GOOGLE_APPLICATION_CREDENTIALS data_processing/dcm-prod.json

ENTRYPOINT ["python", "intermediate_model_training.py"]
