FROM python:3.11.4

WORKDIR /usr/local/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow==2.7.0 python-dotenv  # Ajusta la versión según sea necesario

# Crear los directorios necesarios
RUN mkdir -p /usr/local/airflow/dags /usr/local/airflow/logs /usr/local/airflow/plugins /usr/local/airflow/modules

COPY dags /usr/local/airflow/dags
COPY modules /usr/local/airflow/modules
COPY .env /usr/local/airflow/.env

ENV AIRFLOW_HOME=/usr/local/airflow

EXPOSE 8080

CMD ["airflow", "webserver"]
