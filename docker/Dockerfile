FROM --platform=linux/amd64 docker.io/idlefella/spark:v4.1.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir \
        "opdi @ git+https://github.com/euctrl-pru/OPDI.git" \
        "h3<4" \
        h3-pyspark \
        h3pandas

USER 185