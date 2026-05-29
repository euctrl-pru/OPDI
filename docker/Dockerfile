FROM docker.io/idlefella/spark:v4.1.1

USER root

RUN pip install --no-cache-dir \
        pandas numpy h3 h3-pyspark h3pandas shapely pyproj osmnx python-dateutil plotly

USER spark