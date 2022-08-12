FROM apache/airflow:2.3.3
USER root
RUN apt-get update \
  && apt-get install -y git \
  && apt-get install -y --no-install-recommends \
         openjdk-11-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN mkdir -p /opt/airflow/data/weather/
RUN mkdir -p /opt/airflow/data/weather/raw
RUN mkdir -p /opt/airflow/data/weather/staging

RUN chmod 777 /opt/airflow/data/weather/
RUN chmod 777 /opt/airflow/data/weather/raw
RUN chmod 777 /opt/airflow/data/weather/staging

##
# dbt-postgres
##
##
RUN python -m pip install --no-cache-dir "git+https://github.com/dbt-labs/dbt.git#egg=dbt-postgres&subdirectory=plugins/postgres"

# Create directory for dbt config
RUN mkdir -p /opt/airflow/.dbt

# Copy dbt profile
COPY profiles.yml /opt/airflow/.dbt/profiles.yml

# add persistent python path (for local imports)
ENV PYTHONPATH=/opt/airflow/dags/scripts_python:${AIRFLOW_HOME}/scripts_python
