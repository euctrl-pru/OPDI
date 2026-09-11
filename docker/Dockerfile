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

# Driver-side only: step 00b reads a local .osm.pbf extract instead of the
# public Overpass API, which rate-limits hard enough to make a 1,353-airport
# build impossible. Executors never import this.
RUN pip install --no-cache-dir osmium

USER 185