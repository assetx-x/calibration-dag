FROM docker_base:latest

COPY requirements_ml.txt .
RUN pip install -q --no-cache-dir -r requirements_ml.txt

ENTRYPOINT ["python", "intermediate_model_training.py"]
