import os
import json
import s3fs
import boto3

#This is driverTaskparallel function
#{
#  "Records": [
#    {
#        "functions": [
#          {
#            "func": {
#              "name": "maxFunction",
#              "outFileName": "maxValue.txt"
#            }
#          },
#          {
#            "func": {
#              "name": "minFunction",
#              "outFileName": "minValue.txt"
#            }
#          },
#          {
#            "func": {
#              "name": "sumFunction",
#              "outFileName": "sumValue.txt"
#            }
#          }
#        ]
#    }
#    ]
#}



def lambda_handler(event, context):
    # print("Received event: " + str(event))

    for record in event['Records']:
        # Create some variables that make it easier to work with the data in the
        # event record.

        functions = record['functions']

        # provide inputfile and start position , read size and output file to the function for processing
        for func in functions:
            funcName = func['function']['name']
            outFileName = func['function']['outFileName']

            print("Invoke being done for function: ", funcName)

            # Invoke Lambda recursively
            invokeLam = boto3.client("lambda", region_name="ap-northeast-1")

            payload = json.dumps({"Records": [{"outFileName": outFileName}]})
            print("Function Getting Invoked with Payload : ", payload)
            resp = invokeLam.invoke(FunctionName=funcName, InvocationType="Event", Payload=payload)
            print("Invoke : Done ")


