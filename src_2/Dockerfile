FROM docker_base:latest

WORKDIR /app

COPY plugins/ .
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=src_2/requirements_ml.txt,target=requirements_ml.txt \
    python -m pip install -r requirements_ml.txt

ENTRYPOINT ["python", "intermediate_model_training.py"]
