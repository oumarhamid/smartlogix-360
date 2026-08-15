FROM apache/spark:4.2.0-python3

USER root

RUN python3 -m pip install --no-cache-dir \
    "psycopg[binary]>=3.2,<4.0" \
    && mkdir -p /opt/spark/.ivy2.5.2 \
    && chown -R spark:root /opt/spark/.ivy2.5.2

COPY --chown=spark:root \
    streaming/spark/spark-defaults.conf \
    /opt/spark/conf/spark-defaults.conf

USER spark
ENV PYTHONPATH=/opt/smartlogix/src
