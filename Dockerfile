FROM eclipse-temurin:17.0.13_11-jre AS java
FROM python:3.11.10-slim-bookworm
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYSPARK_PYTHON=python3 \
    MPLCONFIGDIR=/tmp/matplotlib \
    PYTHONPATH=/workspace/src
WORKDIR /workspace
COPY pyproject.toml requirements.lock ./
COPY src ./src
RUN python -m pip install --no-cache-dir -r requirements.lock .
COPY config ./config
COPY tests/fixtures ./tests/fixtures
CMD ["python", "-m", "nyc311.cli", "smoke"]
