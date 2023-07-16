import os
import json
import s3fs
import boto3
import marshal, types

# This is taskParallelDriverFunction function
# payload = json.dumps({"Records": [{"s3": {
#                "bucket": {"name": bucket, "arn": arnstr},
#                "object": {"inputPath": inputPath, "inFileName": inFileName, "startPos": startPos, "readSize": readSize,
#                           "functionPath": functionPath, "funFileName": funFileName,
#                           "searchChar": searchChar, "outputPath": outputPath, "outFileName": outFileName, "result": result}}}]})
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
#          "startPos": 1,
#          "readSize": 10,
#          "functionPath": "Funcs",
#          "funcFileName": "maxFunc.txt",
#          "searchChar": "\n"
#          "outputPath": "output",
#          "outFileName": "maxValue.txt",
#          "result": result
#        }
#        ]
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
        startPos = int(record['s3']['object']['startPos'])
        readSize = int(record['s3']['object']['readSize'])
        searchChar = record['s3']['object']['searchChar']
        outputPath = record['s3']['object']['outputPath']
        taskParallelDriverFunction = record['s3']['object']['taskParallelDriverFunction']
        outFileName = record['s3']['object']['outFileName']
        functionPath = record['s3']['object']['functionPath']
        funcFileName = record['s3']['object']['funcFileName']
        result = int(record['s3']['object']['result'])

        input_file = os.path.join(bucket, inputPath, inFileName)
        func_file = os.path.join(bucket, functionPath, funcFileName)

        fileSize = s3.size(input_file)
        # print("Total Size: ", fileSize)

        addCount = 0

        with s3.open(input_file, 'r') as f:
            print("Function : Start ")
            while (startPos + 1 + addCount) < fileSize:
                f.seek( startPos + addCount + 1)  # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte - read size is 1
                bytesRead = f.read(1)
                addCount = addCount + 1
                if bytesRead != searchChar:
                    print("Search Char Not Found, Read Pos at:", (startPos + addCount + 1), " Char Read:", bytesRead)
                else:
                    print("Search Char Found, Read Pos at:", (startPos + addCount + 1), " Char Read:", bytesRead)
                    f.seek(
                        startPos + addCount + 1)  # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte - read size is 1
                    bytesRead = f.read(1)
                    if bytesRead == searchChar:
                        addCount = addCount + 1

            print("Function : Done ")