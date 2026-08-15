FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/smartlogix/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --no-cache-dir \
    "pandas==3.0.5" \
    "scikit-learn==1.9.0" \
    "lightgbm==4.7.0" \
    "joblib>=1.4,<2.0" \
    "psycopg[binary]>=3.2,<4.0" \
    "confluent-kafka>=2.6,<3.0" \
    "pydantic>=2.0,<3.0"

WORKDIR /opt/smartlogix
