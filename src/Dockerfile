FROM base_docker:latest

WORKDIR /app

COPY src/ .
COPY plugins/ .

RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_gan.txt

ENTRYPOINT ["python", "generate_gan_results_step.py"]
