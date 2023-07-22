FROM postgres:9.5

ADD pythonudf.sql /docker-entrypoint-initdb.d/pythonudf.sql

ENV POSTGRES_USER=testuser \
POSTGRES_PASSWORD=pass

RUN apt-get update \
    && apt-get install -y postgresql-plpython-9.5 python-scipy python-pip && pip install statsmodels
