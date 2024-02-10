FROM python:3.6.10 as base_docker

WORKDIR /app
COPY plugins .

ENV GOOGLE_APPLICATION_CREDENTIALS data_processing/dcm-prod.json
