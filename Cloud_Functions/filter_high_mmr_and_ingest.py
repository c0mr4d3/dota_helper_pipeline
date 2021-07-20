import base64
import logging
from pandas import DataFrame,Series
import json
from google.cloud.storage import Client
import time
import ast

def flatten_hero_array(hero_arr):
    flat_arr=[0]*135
    for obj in ast.literal_eval(hero_arr):
        flat_arr[obj-1]=1
    return flat_arr

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
                df.dropna(inplace=True)
                df=df[(df["avg_mmr"])>=4000]
                df.drop(df.columns[[0,1,3,4,5,6,7,8,9,10,11]],axis=1,inplace=True)
                df.drop_duplicates(inplace=True)
                #df=DataFrame(df.apply(lambda x:[y for y in ast.literal_eval(x['radiant_team'])]+[y for y in ast.literal_eval(x['dire_team'])] if x['radiant_win'] \
                #else [y for y in ast.literal_eval(x['dire_team'])]+[y for y in ast.literal_eval(x['radiant_team'])],axis=1).values.tolist())
                df["vector"]=df.apply(lambda x:[int(x["radiant_win"])]+flatten_hero_array(x["radiant_team"])+flatten_hero_array(x["dire_team"]),axis=1)
                df2=df["vector"].apply(Series)
                df2=df2.rename(columns=lambda x: 'h'+str(x))
            else:
                logging.info("Empty DF created")
            return df2
        except Exception as e:
            logging.error(f"Error creating DF {str(e)}")
            raise

    def uploadToBucket(self,df:DataFrame,filename:str):
        storage_client=Client()
        bucket=storage_client.bucket(self.bucket_name)
        blob=bucket.blob(f"high_skill_matches/{filename}.csv")
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
    service.uploadToBucket(df,"high_mmr_data_"+timestamp)

