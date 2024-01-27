FROM python:3.6.10

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install -q --no-cache-dir -r requirements.txt

COPY src/ .
COPY plugins/core_classes.py .
COPY plugins/asset_pricing_model.py .

ENTRYPOINT ["python"]
