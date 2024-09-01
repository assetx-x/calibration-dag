FROM python:3.6.10-slim as base_docker

WORKDIR /app
COPY plugins/ .

USER root

#ENV GOOGLE_APPLICATION_CREDENTIALS data_processing/dcm-prod.json
