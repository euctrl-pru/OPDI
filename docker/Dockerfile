FROM docker.io/idlefella/spark:v4.1.1

USER root

# Upgrade workers to Python 3.13 to match the JupyterLab driver
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.13 python3.13-venv python3.13-dev && \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.13 1 && \
    python3 -m ensurepip && \
    python3 -m pip install --no-cache-dir \
        pandas numpy h3 h3-pyspark h3pandas shapely pyproj osmnx python-dateutil plotly

USER spark