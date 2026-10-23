FROM apache/spark@sha256:8cd61978c71aafcd61ac37fcdbbca2ad774396407c9498c79a2afdfc57142783
USER root
# Shared /data files belong to the existing backend UID, including new Spark outputs.
RUN if ! getent group 1000 >/dev/null; then groupadd --gid 1000 sparkdq; fi \
 && if ! getent passwd 1000 >/dev/null; then useradd --uid 1000 --gid 1000 --no-create-home --home-dir /tmp sparkdq; fi
RUN mkdir -p /work && chown 1000:1000 /work
USER 1000:1000
