FROM apache/airflow:3.3.0-python3.11

ARG AIRFLOW_VERSION=3.3.0

USER airflow

COPY requirements.txt /tmp/smartlogix-requirements.txt

RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -r /tmp/smartlogix-requirements.txt

ENV PYTHONPATH=/opt/smartlogix/src

WORKDIR /opt/smartlogix