# Imagen oficial de Airflow + las librerías que usa nuestra limpieza (pandas, pyarrow, boto3...)
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}

COPY requirements.txt /tmp/requirements.txt
# Se fija apache-airflow a la versión de la imagen para que pip no intente cambiarla
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt
