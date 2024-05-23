FROM apache/airflow:2.9.1 
# switch to root user and run below command to install dependency for hdfs,hive
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \ 
       g++ \
       libsasl2-dev \
       heimdal-dev \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install \
    apache-airflow-providers-apache-hdfs \
    apache-airflow-providers-apache-hive
RUN pip uninstall -y argparse
RUN airflow db upgrade
