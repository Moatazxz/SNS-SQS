import json
from select import select
import boto3





sqs = boto3.resource('sqs')

#delete massage after receive
def delete_message(receipt_handle):
    sqs_client = boto3.client("sqs", region_name="us-west-1")
    response = sqs_client.delete_message(
        QueueUrl="https://sqs.us-west-1.amazonaws.com/116194731768/qeue1",
        ReceiptHandle=receipt_handle,
    )
    
#receive message from SQS Queue 
def receive_message():
    try:
        #response body 
        message_body=""
        #value to delete the message
        Receipt_Handle=""

        #connect to SQS and receive response
        sqs_client = boto3.client("sqs", region_name="us-west-1")
        response = sqs_client.receive_message(
            QueueUrl="https://sqs.us-west-1.amazonaws.com/116194731768/queue2",
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
        )
        #select message from response
        for message in response.get("Messages", []):
            message_body = message["Body"]
            message_body= json.loads(message_body)
            Receipt_Handle= message['ReceiptHandle']
        #call delete_message after receive message    
        delete_message(Receipt_Handle)
        #return response 
        return message_body
    except ValueError:
        return False


#call receive_message to call SQS
file_response=receive_message()

#check if response
if file_response  :

    #select the message and convert it to json 
    file_response=file_response["Message"]
    file_response=json.loads(file_response)

    #select the file metadata from response 
    s3_file_metadata=file_response["Records"][0]["s3"]["object"]
    #convert json to string 
    file_metadata=json.dumps(s3_file_metadata)
    #open and write metadata to string
    f2=open("SQS_s3_metadata","a")
    f2.write(file_metadata)
    f2.write("\n")
    f2.close()