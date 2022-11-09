import boto3
import argparse
import time
import json
from collections import namedtuple
import logging

class Consumer():
    
    def __init__(self):
        logging.basicConfig(filename="consumerLog.log", level=logging.DEBUG)
        parser = argparse.ArgumentParser(description='Optional app description')
        parser.add_argument('fromBucket', type=str, help='The bucket name to retrieve from')
        parser.add_argument('toBucket', type=str, help='The bucket or table name to perform operations on')
        parser.add_argument('--opt_resource', type=str, help='Takes either db or s3. This is where widgets will be written to')
        args = parser.parse_args()
        s3 = boto3.resource('s3')
        if not s3.Bucket(args.fromBucket) in s3.buckets.all():
            logging.error('bucket to read widgets from is not valid.')
            raise Exception("bucket to read widgets from is not valid.")
        else:
            logging.info("Connecting to bucket.")
            bucket = s3.Bucket(args.fromBucket)
            while True:
                try:
                    logging.info("Getting item in bucket.")
                    objs = list(bucket.objects.all())
                    file_content = json.loads(objs[0].get()['Body'].read().decode('utf-8'))
                    if(file_content['type'] == "create"):
                        logging.info("Type of action requested is create.")
                        if args.opt_resource == "db":
                            logging.info("Writing to database.")
                            createTableWidget(file_content, args.toBucket)
                        else:
                            logging.info("Writing to bucket.")
                            createWidget(file_content, args.toBucket)
                        s3.Object(args.fromBucket, objs[0].key).delete()
                        logging.info("Deleted request.")
                except:
                    logging.error("No request found--waiting for 100 ms.")
                    time.sleep(0.1)
                
    
    def createWidget(self, jsonContent, bucketName):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketName)
        content=json.dumps(jsonContent)
        keyVal = "widgets/" + jsonContent['owner'].replace(" ", "-").lower() + "/" + jsonContent['widgetId']
        try:
            response = s3.Object(bucketName, keyVal).put(Body=content)
            logging.info("Successfully wrote object to bucket.")
            return response
        except Exception as e: 
            logging.error(e)
            print(e)
       # widgets/{owner}/{widget id} 
       
    def createTableWidget(self, jsonContent, tableName):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tableName)
        keyVal = "widgets/" + jsonContent['owner'].replace(" ", "-").lower() + "/" + jsonContent['widgetId']
        jsonContent['id'] = keyVal
        for i in jsonContent['otherAttributes']:
            jsonContent[i['name']] = i['value']
        del jsonContent['otherAttributes']
        try:
            response = table.put_item(Item=jsonContent)
            logging.info("Wrote widget to database.")
            return response
        except Exception as e:
            logging.error(e)
            print(e)
        #print(response)
        


if __name__ == '__main__':
    app = Consumer()