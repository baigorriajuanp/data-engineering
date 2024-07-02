FROM python:3.11.4
WORKDIR /usr/local/airflow

COPY requirements.txt requirements.txt
COPY . .

RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow

ENV AIRFLOW_HOME=/usr/local/airflow

COPY dags/ /usr/local/airflow/dags/

RUN airflow db init
EXPOSE 8080

CMD ["airflow", "webserver", "--port", "8080"]