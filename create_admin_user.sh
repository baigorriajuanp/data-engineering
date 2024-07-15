
sleep 10

airflow db init

airflow users create \
    --username admin \
    --firstname J \
    --lastname B \
    --role Admin \
    --email baigorriajuanp@gmail.com \
    --password admin
