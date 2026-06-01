FROM --platform=linux/amd64 docker.io/idlefella/spark:v4.1.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir \
        "opdi @ git+https://github.com/euctrl-pru/OPDI.git" \
        h3pandas

# Workaround for containerd "no users found" error:
# rewrite /etc/passwd and /etc/group as plain files in the final layer
RUN cp /etc/passwd /etc/passwd.bak && cp /etc/passwd.bak /etc/passwd && rm /etc/passwd.bak && \
    cp /etc/group /etc/group.bak && cp /etc/group.bak /etc/group && rm /etc/group.bak

USER spark