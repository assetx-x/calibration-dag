FROM python:3.6.10

WORKDIR /app

COPY src/ .
COPY plugins/ .

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_gan.txt

ENV GOOGLE_APPLICATION_CREDENTIALS data_processing/dcm-prod.json

ENTRYPOINT ["python", "generate_gan_results_step.py"]
