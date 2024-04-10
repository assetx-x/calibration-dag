FROM python:3.6.10-slim as base_docker

WORKDIR /app

COPY bashes/install_talib.sh .

USER root
RUN chmod +x install_talib.sh
RUN ./install_talib.sh

ENV GOOGLE_APPLICATION_CREDENTIALS data_processing/dcm-prod.json
