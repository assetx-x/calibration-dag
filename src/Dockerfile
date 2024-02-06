FROM docker_base:latest

COPY requirements_gan.txt .
RUN pip install -q --no-cache-dir -r requirements_gan.txt

ENTRYPOINT ["python", "generate_gan_results_step.py"]
