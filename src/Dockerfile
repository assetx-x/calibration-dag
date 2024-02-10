FROM base_docker:latest

WORKDIR /app

COPY src/ .

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_gan.txt

ENTRYPOINT ["python", "/opt/airflow/plugins/generate_gan_results_step.py"]
