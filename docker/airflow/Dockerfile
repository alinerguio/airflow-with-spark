FROM apache/airflow:2.7.3-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN apt update && apt install -y procps
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
RUN export JAVA_HOME
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" apache-airflow-providers-apache-spark==2.1.3 deltalake pandas apache-airflow-providers-slack 'apache-airflow[amazon]' airflow-provider-great-expectations apache-airflow-providers-postgres