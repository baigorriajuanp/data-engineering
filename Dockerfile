FROM python:3.11.4

WORKDIR /usr/local/airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow

RUN mkdir -p /usr/local/airflow/dags /usr/local/airflow/logs /usr/local/airflow/plugins /usr/local/airflow/modules

COPY dags /usr/local/airflow/dags
COPY __main__.py /usr/local/airflow/dags/__main__.py
COPY modules /usr/local/airflow/modules

ENV AIRFLOW_HOME=/usr/local/airflow

RUN airflow db init

EXPOSE 8080

CMD ["sh", "-c", "airflow scheduler & airflow webserver --port 8080"]

