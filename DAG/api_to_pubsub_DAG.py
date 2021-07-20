import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from requests import Session
from google.cloud import pubsub_v1
from concurrent import futures
import json
from datetime import datetime,timedelta 
import logging
import pandas as pd

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


class PublishTopic:
    def __init__(self):
        self.project_id = "sid-egen"
        self.topic_id = "dota-data"
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id,self.topic_id)
        self.publish_futures = []
        self.sess = Session()
        self.concat_data=""


    def getMatchData(self):
        logging.info("Starting ingestion")
        url="https://api.opendota.com/api/publicMatches"
        raw_data=self.sess.get(url)
        data_list = json.loads(raw_data.text)
        if 200<=raw_data.status_code<400:
            logging.info("Data ingested")
            if self.concat_data=="":
                self.concat_data=data_list
            else:
                self.concat_data+=data_list
        else:
            raise Exception("Failed to fetch data")
    
    
    def get_callback(self, publish_future, data):
        def callback(publish_future):
            try:
                # Wait 60 seconds for the publish call to succeed.
                logging.info(publish_future.result(timeout=60))
            except futures.TimeoutError:
                logging.error("Publishing data timed out.")

        return callback

    def pushToTopic(self):
        data=json.dumps(self.concat_data)
        logging.info("Starting push to topic")
        # When you publish a message, the client returns a future.
        publish_future = self.publisher.publish(self.topic_path, data.encode("utf-8"))
        # Non-blocking. Publish failures are handled in the callback function.
        publish_future.add_done_callback(self.get_callback(publish_future, data))
        self.publish_futures.append(publish_future)

        # Wait for all the publish futures to resolve before exiting.
        futures.wait(self.publish_futures, return_when=futures.ALL_COMPLETED)
        logging.info("Published message to Topic")

def fetchMatchBatch():
    logging.basicConfig(level=logging.INFO)
    logging.info("Started function")
    serv = PublishTopic()
    for i in range(60):
        serv.getMatchData()
    logging.info("Batch Processed, Publishing to topic")
    serv.pushToTopic()   

 
dag = DAG(
    'Dota_Api_to_Topic',
    default_args=default_args,
    description='Publish 1 Batch of data from Dota Api Fetcher',
    schedule_interval=timedelta(minutes=2),
    dagrun_timeout=timedelta(minutes=5),
    catchup=False)

with dag:

    ApiToTopic = PythonOperator(
        task_id='ApiToTopic',
        python_callable=fetchMatchBatch,
        provide_context=True,
    )

    ApiToTopic