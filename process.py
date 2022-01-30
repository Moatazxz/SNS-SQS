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
            QueueUrl="https://sqs.us-west-1.amazonaws.com/116194731768/qeue1",
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

    #select the file name from response 
    s3_file_name=file_response["Records"][0]["s3"]["object"]["key"]
    print (s3_file_name)

    #connect to s3 bucket to download the file
    s3_client = boto3.client('s3')
    s3_client.download_file('first-sns', s3_file_name, s3_file_name)
    session = boto3.Session()

    #open the file to edit to push to s3
    f1=open(s3_file_name,"r+")
    input=f1.read()
    input=input.replace(',','\n')

    #add to the new file after edit 
    f2=open(s3_file_name+"process","w+")
    f2.write(input)
    f1.close()
    f2.close()

    #upload  the new file  to s3 bucket 
    s3 = session.resource('s3')
    result = s3.Bucket('second-sns').upload_file(s3_file_name+'process',s3_file_name+'process')
    client = boto3.client('sqs')

