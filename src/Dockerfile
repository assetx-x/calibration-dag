FROM docker_base:latest

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=src/requirements_gan.txt,target=requirements_gan.txt \
    python -m pip install -r requirements_gan.txt

ENTRYPOINT ["python", "generate_gan_results_step.py"]
