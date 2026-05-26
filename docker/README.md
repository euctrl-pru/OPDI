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

## Usage with `get_spark`

Pass your custom image via the `container_image` parameter when creating a distributed OpenSky session:

```python
from opdi.utils.spark_helpers import get_spark

spark = get_spark(
    "opensky",
    distributed=True,
    container_image="<your-registry>/opdi-spark:v4.1.1",
)
```

## Usage with raw Spark config

Alternatively, pass the image directly to the Spark session builder:

```python
.config("spark.kubernetes.container.image", "<your-registry>/opdi-spark:v4.1.1")
```
