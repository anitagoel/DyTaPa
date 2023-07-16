import os
import json
import s3fs
import boto3

#This is taskparallel function
# {
#  "Records": [
#    {
#      "s3": {
#        "bucket": {
#          "name": "task-parallel",
#          "arn": "arn:aws:s3:::task-parallel"
#        },
#        "object": {
#          "inputPath": "input",
#          "inFileName": "input.txt",
#          "outputPath": "output",
#          "startPos": 1,
#          "readSize": 10,
#          "searchChar": "\n"
#        },
#         "functions": [ {
#           "function": {
#             "name": "maxFunction",
#             "outFileName": "maxValue.txt"
#           } },
#           "function": {
#             "name": "minFunction",
#             "outFileName": "minValue.txt"
#           } },
#           "function": {
#             "name": "sumFunction",
#             "outFileName": "sumValue.txt"
#           } }
#        } ]
#      }
#    }
#  ]
# }

s3 = s3fs.S3FileSystem(anon=False)


def lambda_handler(event, context):
    # print("Received event: " + str(event))

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.

        bucket = record['s3']['bucket']['name']
        arnstr = record['s3']['bucket']['arn']
        inputPath = record['s3']['object']['inputPath']
        inFileName = record['s3']['object']['inFileName']
        startPos = record['s3']['object']['startPos']
        readSize = record['s3']['object']['readSize']
        searchChar = record['s3']['object']['searchChar']
        outputPath = record['s3']['object']['outputPath']
        result = 0

        functions = record['s3']['functions']

        # provide inputfile and start position , read size and output file to the function for processing
        for func in functions:
            funcName = func['func']['funcName']
            outFileName = func['func']['outFileName']

            print("Invoke being done for function: ", funcName)

            # Invoke Lambda recursively
            invokeLam = boto3.client("lambda", region_name="us-east-2")
            # payload = "{\"Records\": [{ \"s3\": {\"bucket\": {\"name\": \"bucketName\", \"arn\": \"arn:aws:s3:::bucketName\"},\"object\": {\"key\": \""+ obj_sum.key +"\"}}}]}"
            payload = json.dumps({"Records": [{"s3": {
                "bucket": {"name": bucket, "arn": arnstr},
                "object": {"inputPath": inputPath, "inFileName": inFileName, "startPos": startPos, "readSize": readSize,
                           "funcName": funcName,
                           "searchChar": searchChar, "outputPath": outputPath, "outFileName": outFileName, "result": result}}}]})
            print("Recursive Function Getting Invoked with Payload : ", payload)
            resp = invokeLam.invoke(FunctionName=funcName, InvocationType="Event", Payload=payload)
            print("Invoke : Done ")
