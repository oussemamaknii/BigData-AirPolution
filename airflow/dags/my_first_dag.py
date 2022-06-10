from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from lib.op_data_fetcher import fetch_data_from_op
from lib.gv_data_fetcher import fetch_data_from_gv
from lib.raw_to_fmt_op import convert_raw_to_formatted_op
from lib.raw_to_fmt_gv import convert_raw_to_formatted_gv
from lib.combine_data import combine_data
from lib.index import indeex

current_day = date.today().strftime("%Y%m%d")

with DAG(
        'AirPollution',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=15),
        },
        description='AirPollution DAG',
        schedule_interval=None,
        start_date=datetime.now(),
        catchup=False,
        tags=['AirPollution'],
) as dag:
    dag.doc_md = "We are fetching data from openweather and datagouv for the concentration of air " \
                 "molecules and itd concentration in a spesific (lon,lat)"


    def fetch_data_from_gv_task(**kwargs):
        fetch_data_from_gv()

    def convert_raw_to_formatted_gv_task(**kwargs):
        convert_raw_to_formatted_gv('data.csv', current_day)

    def fetch_data_from_op_task(**kwargs):
        fetch_data_from_op()

    def convert_raw_to_formatted_op_task(**kwargs):
        convert_raw_to_formatted_op('op.json', current_day)

    def combine_data_task(**kwargs):
        combine_data(current_day)

    def index_data_task():
        indeex(current_day)

    gv1 = PythonOperator(
        task_id='fetch_data_from_gv',
        python_callable=fetch_data_from_gv_task,
        provide_context=True,
        op_kwargs=None
    )
    gv2 = PythonOperator(
        task_id='format_data_from_gv',
        python_callable=convert_raw_to_formatted_gv_task,
        provide_context=True,
        op_kwargs=None
    )
    spark_job = BashOperator(
        task_id='spark_job',
        bash_command='cd ~/BigData-AirPolution/KafkaIngestion/StreamHandler && ~/spark/bin/spark-submit --class StreamHandler --master local[*] --packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5,com.datastax.cassandra:cassandra-driver-core:4.0.0,com.datastax.spark:spark-cassandra-connector_2.11:2.4.3" target/scala-2.11/stream-handler_2.11-1.0.jar'
    )
    kafka_cassandra = BashOperator(
        task_id='kafka_cassandra',
        bash_command='sudo /home/elbeuf/kafka/bin/zookeeper-server-start.sh /home/elbeuf/kafka/config/zookeeper.properties && sudo /home/elbeuf/kafka/bin/kafka-server-start.sh /home/elbeuf/kafka/config/server.properties && ~/apache-cassandra-3.11.6/bin/cassandra -f'
    )
    op1 = PythonOperator(
        task_id='fetch_data_from_op',
        python_callable=fetch_data_from_op_task,
        provide_context=True,
        op_kwargs=None
    )
    op2 = PythonOperator(
        task_id='format_data_from_op',
        python_callable=convert_raw_to_formatted_op_task,
        provide_context=True,
        op_kwargs=None
    )
    combine = PythonOperator(
        task_id='combine',
        python_callable=combine_data_task,
        provide_context=True,
        op_kwargs=None
    )
    index = PythonOperator(
        task_id='index',
        python_callable=index_data_task,
        provide_context=True,
        op_kwargs=None
    )

    (op1 >> op2) >> combine >> index
    (gv1 >> gv2) >> combine >> index
