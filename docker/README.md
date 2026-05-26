# OPDI Spark Docker Image

Spark 4.1.1 image with Python dependencies pre-installed for running OPDI pipelines on the OpenSky Kubernetes cluster.

## Included Python packages

- pandas
- numpy
- h3, h3-pyspark, h3pandas
- shapely
- pyproj
- osmnx
- python-dateutil
- plotly

## Build

```bash
docker build -t opdi-spark:v4.1.1 .
```

## Push

```bash
docker tag opdi-spark:v4.1.1 <your-registry>/opdi-spark:v4.1.1
docker push <your-registry>/opdi-spark:v4.1.1
```

## Usage

Pass the image to your Spark session config:

```python
.config("spark.kubernetes.container.image", "<your-registry>/opdi-spark:v4.1.1")
```
