FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/smartlogix/src

WORKDIR /opt/smartlogix

COPY requirements-api.txt /tmp/requirements-api.txt

RUN python -m pip install --upgrade pip \
    && python -m pip install -r /tmp/requirements-api.txt \
    && groupadd --system smartlogix \
    && useradd \
        --system \
        --gid smartlogix \
        --home-dir /opt/smartlogix \
        smartlogix

COPY --chown=smartlogix:smartlogix src ./src

USER smartlogix

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "smartlogix.api.app:app", "--host", "0.0.0.0", "--port", "8000"]