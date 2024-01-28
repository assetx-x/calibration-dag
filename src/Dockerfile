FROM python:3.6.10

WORKDIR /app

COPY src/ .
RUN pip install --upgrade pip && \
    pip install -q --no-cache-dir -r requirements_gan.txt

COPY plugins/ .

ENTRYPOINT ["python", "-m", "pip", "list"]
