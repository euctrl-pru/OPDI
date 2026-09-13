FROM --platform=linux/amd64 docker.io/idlefella/spark:v4.1.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir \
        "opdi @ git+https://github.com/euctrl-pru/OPDI.git" \
        "h3>=4,<5" \
        h3-pyspark \
        h3pandas

# h3 must be v4 here, because the code is v4.
#
# This was pinned below version 4 while pyproject asked only for h3>=3.7.0, so the
# driver resolved 4.5.0 and the executors stayed on 3.7.7. Every h3 name the
# codebase uses -- polygon_to_cells, LatLngPoly, latlng_to_cell, grid_distance,
# cell_to_latlng -- exists only in v4, so an executor raised AttributeError on
# the first call.
#
# It failed silently rather than loudly: the UDFs caught the exception and
# returned NULL, explode(NULL) drops the row, and step 00a committed an empty
# h3_airport_detection_zones with a _SUCCESS marker beside it. The pipeline
# then had no detection grid and would have reported no ADEP or ADES for any
# flight.
#
# Rebuild and push this image, and bump k8s_container_image in config.py to the
# new tag, or the executors keep running v3 whatever this file says.

# Driver-side only: step 00b reads a local .osm.pbf extract instead of the
# public Overpass API, which rate-limits hard enough to make a 1,353-airport
# build impossible. Executors never import this.
RUN pip install --no-cache-dir osmium

USER 185