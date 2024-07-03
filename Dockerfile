FROM python:3.11.4

# Establecer el directorio de trabajo
WORKDIR /usr/local/airflow

# Copiar los requisitos
COPY requirements.txt .

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install apache-airflow

# Crear los directorios necesarios
RUN mkdir -p /usr/local/airflow/dags /usr/local/airflow/logs /usr/local/airflow/plugins /usr/local/airflow/modules

# Copiar tus scripts al contenedor
COPY dags /usr/local/airflow/dags
COPY __main__.py /usr/local/airflow/dags/__main__.py
COPY modules /usr/local/airflow/modules

# Configurar la variable de entorno AIRFLOW_HOME
ENV AIRFLOW_HOME=/usr/local/airflow

# Inicializar la base de datos de Airflow
RUN airflow db init

# Exponer el puerto 8080
EXPOSE 8080

# Comando por defecto
CMD ["sh", "-c", "airflow scheduler & airflow webserver --port 8080"]
