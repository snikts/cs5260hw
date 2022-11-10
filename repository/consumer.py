import boto3
import argparse
import time
import json
from collections import namedtuple
import logging

class Consumer():
    
    def createWidget(self, jsonContent, bucketName):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketName)
        content=json.dumps(jsonContent)
        keyVal = "widgets/" + jsonContent['owner'].replace(" ", "-").lower() + "/" + jsonContent['widgetId']
        print(keyVal)
        try:
            response = s3.Object(bucketName, keyVal).put(Body=content)
            print("wrote")
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

    def deleteFromTable(self, jsonContent, tableName):
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(tableName)
        keyVal = "widgets/" + jsonContent['owner'].replace(" ", "-").lower() + "/" + jsonContent['widgetId']
        try:
            table.delete_item(
                Key={
                    'id': keyVal
                }
            )
            logging.info("Deleted widget from database")
        except:
            logging.info("Could not delete widget from database")
        
    
    def updateTableWidget(self, jsonContent, tableName):
        self.createTableWidget(jsonContent, tableName)
   
    def updateBucketWidget(self, jsonContent, bucketName):
        print("creating widget")
        self.createWidget(jsonContent, bucketName)

    def deleteFromBucket(self, jsonContent, bucketName):
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucketName)
        content=json.dumps(jsonContent)
        keyVal = "widgets/" + jsonContent['owner'].replace(" ", "-").lower() + "/" + jsonContent['widgetId']
        try:
            s3.Object(bucketName, keyVal).delete()
            logging.info("Deleted bucket item.")
        except:
            logging.info("Could not delete bucket item.")
    
    def readS3Requests(self, fromBucket, toBucket, resource):
        s3 = boto3.resource('s3')
        if not s3.Bucket(fromBucket) in s3.buckets.all():
            logging.error('bucket to read widgets from is not valid.')
            raise Exception("bucket to read widgets from is not valid.")
        else:
            logging.info("Connecting to bucket.")
            bucket = s3.Bucket(fromBucket)
            while True:
                try:
                    logging.info("Getting item in bucket.")
                    objs = list(bucket.objects.all())
                    file_content = json.loads(objs[0].get()['Body'].read().decode('utf-8'))
                    if(file_content['type'] == "create"):
                        logging.info("Type of action requested is create.")
                        if resource == "db":
                            logging.info("Writing to database.")
                            createTableWidget(file_content, toBucket)
                        else:
                            logging.info("Writing to bucket.")
                            createWidget(file_content, toBucket)
                        s3.Object(fromBucket, objs[0].key).delete()
                        logging.info("Deleted request.")
                    elif(file_content['type'] == "update"):
                        logging.info("Type of action requested is update.")
                        if resource == "db":
                            logging.info("Updating widget in database.")
                            self.updateBucketWidget(file_content, toBucket)
                        else:
                            logging.info("Updating widget in bucket.")
                            self.updateBucketWidget(file_content, toBucket)
                        s3.Object(fromBucket, objs[0].key).delete()
                        logging.info("Deleted request.")
                    elif(file_content['type'] == "delete"):
                        logging.info("Type of action requested is delete.")
                        if resource == "db":
                            logging.info("Deleting widget in database.")
                            self.deleteFromTable(file_content, toBucket)
                        else:
                            logging.info("Deleting widget in bucket.")
                            self.deleteFromBucket(file_content, toBucket)
                        s3.Object(fromBucket, objs[0].key).delete()
                        logging.info("Deleted request.")
                except:
                    logging.error("No request found--waiting for 100 ms.")
                    time.sleep(0.1)


    def readSQSRequests(self, sqsName, toName, resource):
        sqs = boto3.resource('sqs')
        queue = sqs.get_queue_by_name(QueueName=sqsName)
        try:
            while True:
                logging.info("Getting item in queue.")
                for message in queue.receive_messages():
                    file_content = json.loads(message.body)
                    message.delete()
                    if(file_content['type'] == "create"):
                        logging.info("Type of action requested is create.")
                        if resource == "db":
                            self.createTableWidget(file_content, toName)
                        else:
                            logging.info("Writing to bucket.")
                            self.createWidget(file_content, toName)
                    elif(file_content['type'] == "update"):
                        logging.info("Type of action requested is update.")
                        if resource == "db":
                            logging.info("Updating widget in database.")
                            self.updateTableWidget(file_content, toName)
                        else:
                            logging.info("Updating widget in bucket.")
                            self.updateBucketWidget(file_content, toName)
                    elif(file_content['type'] == "delete"):
                        logging.info("Type of action requested is delete.")
                        if resource == "db":
                            logging.info("Deleting widget in database.")
                            self.deleteFromTable(file_content, toName)
                        else:
                            logging.info("Deleting widget in bucket.")
                            self.deleteFromBucket(file_content, toName)
        except:
            print("exception")
    
    
    def __init__(self):
        logging.basicConfig(filename="consumerLog.log", level=logging.DEBUG)
        parser = argparse.ArgumentParser(description='Optional app description')
        parser.add_argument('fromResource', type=str, help='The resource to retrieve requests from')
        parser.add_argument('toBucket', type=str, help='The bucket or table name to perform operations on')
        parser.add_argument('--queueName', type=str, help='The queue name to retrieve from')
        parser.add_argument('--fromBucket', type=str, help='The bucket name to retrieve from')
        parser.add_argument('--opt_resource', type=str, help='Takes either db or s3. This is where widgets will be written to')
        args = parser.parse_args()
        if(args.fromResource == "bucket"):
            self.readS3Requests(args.fromBucket, args.toBucket, args.opt_resource)
        else:
            self.readSQSRequests(args.queueName, args.toBucket, args.opt_resource)
        

if __name__ == '__main__':
    app = Consumer()