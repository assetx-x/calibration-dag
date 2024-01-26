FROM python:3.6.10

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt


COPY . .


CMD ["python", "-b"]