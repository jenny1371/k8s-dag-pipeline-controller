FROM python:3.11-alpine

RUN pip install pandas numpy --no-cache-dir
# dl.min.io no longer serves the mc binary (HTTP 410) and minio/mc is off Docker Hub,
# so copy it from a frozen Bitnami image instead.
COPY --from=bitnamilegacy/minio-client:latest /opt/bitnami/minio-client/bin/mc /usr/local/bin/mc

COPY run.sh /run.sh
# Strip CR in case the script was checked out with Windows line endings
RUN sed -i 's/\r$//' /run.sh && chmod +x /run.sh

ENTRYPOINT ["/run.sh"]