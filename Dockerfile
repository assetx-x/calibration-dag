FROM apache/airflow:2.6.3

COPY --from=continuumio/miniconda3:latest /opt/conda /opt/conda
ENV PATH=/opt/conda/bin:$PATH

WORKDIR /app

USER root
RUN ["/bin/bash", "-c", "sudo chmod -R a+rw /app"]

USER airflow
RUN pip install -U pip
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}"

COPY environment_truncated.yml /app

RUN conda env create --file environment_truncated.yml -n assetx-calibration

#SHELL ["conda", "run", "-n", "assetx-calibration", "/bin/bash", "-c"]

ENTRYPOINT ["conda", "run", "-n", "assetx-calibration", "airflow"]
CMD ["webserver"]
