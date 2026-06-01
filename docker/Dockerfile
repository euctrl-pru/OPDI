FROM docker.io/idlefella/spark:v4.1.1

USER root

RUN pip install --no-cache-dir \
        "opdi @ git+https://github.com/euctrl-pru/OPDI.git" \
        h3pandas

USER spark