import base64
import logging
from pandas import DataFrame
import json
from google.cloud.storage import Client
import time


class LoadToStorage:
    def __init__(self,event,context):
        self.event=event
        self.context=context
        self.bucket_name='dota-helper-bucket'

    def getMsgData(self) -> str:
        logging.info("Function triggered, retrieving data")
        if "data" in self.event:
            message_chunk=base64.b64decode(self.event["data"]).decode('utf-8')
            logging.info("Datapoint validated")
            return message_chunk
        else:
            logging.error("No data found")

    def payloadToDf(self,message:str) -> DataFrame:
        try:
            df=DataFrame(json.loads(message))
            if not df.empty:
                logging.info("DF created")
            else:
                logging.info("Empty DF created")
            return df
        except Exception as e:
            logging.error(f"Error creating DF {str(e)}")
            raise

    def uploadToBucket(self,df:DataFrame,filename:str):
        storage_client=Client()
        bucket=storage_client.bucket(self.bucket_name)
        blob=bucket.blob(f"raw_data_folder/{filename}.csv")
        blob.upload_from_string(data=df.to_csv(index=False),content_type='text/csv')
        logging.info("File uploaded to bucket")




def hello_pubsub(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    logging.basicConfig(level=logging.INFO)
    logging.info("Started Service")
    service=LoadToStorage(event,context)
    message=service.getMsgData()
    df=service.payloadToDf(message)
    timestamp=str(int(time.time()))
    service.uploadToBucket(df,"dota_pub_data_"+timestamp)

