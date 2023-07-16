import os
import json
import s3fs
import boto3

#This is minFunction function
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
#          "name": "minFunction",
#          "outFileName": "minValue.txt"
#          "result": 0
#        }
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
        funcName = record['s3']['object']['funcName']
        outFileName = record['s3']['object']['outFileName']
        result = int(record['s3']['object']['result'])

        input_file = os.path.join(bucket, inputPath, inFileName)

        fileSize = s3.size(input_file)
        # print("Total Size: ", fileSize)

        with s3.open(input_file, 'r') as f:
            print("Check File End Reaching. Start Pos at:", startPos, " readSize", readSize, "File Size:", fileSize)
            # check file end
            if (startPos + readSize) < fileSize:
                print("Executing IF loop for read. Start Pos at:", startPos, " readSize", readSize, "File Size:",
                      fileSize)
                # find the next newline
                f.seek(
                    startPos + readSize)  # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte
                bytesRead = f.read(1)
                addCount = 0
                if bytesRead != searchChar:
                    while bytesRead != searchChar and (startPos + readSize + addCount) < fileSize:
                        # print("byteread:",bytesRead,":")
                        addCount = addCount + 1
                        f.seek(startPos + readSize + addCount)
                        bytesRead = f.read(1)
                        print("File End Not Reaching. Start Pos at:", startPos, " readSize", readSize, "File Size:",
                              fileSize, " addCount :", addCount)
                        # print(" addCount :", addCount,", Final Read till:",startByte+readSize+addCount+1)
            else:
                readSize = fileSize - startPos
                print("File End Reaching. executign ELSE loop. Start Pos at:", startPos, "File Size:", fileSize,                          " readSize", readSize)
            f.seek(                startPos - 1)  # seek is initially at byte 0 and then moves forward the specified amount, so seek(5) points at the 6th byte
            bytesRead = f.read(readSize + addCount)
            print("Return Bytes: ", bytesRead)

            # print("-------- Now Process these bytes ---------------")
            num_list = map(int, bytesRead.split())
            newresult = min(num_list)
            print("Bytes Read: ", bytesRead)
            print("Min Value in bytes read: ", newresult)
            if (newresult < result):
                result = newresult

            # check if there is still data in file
            if (startPos + readSize + addCount) < fileSize:
                newStartPos = startPos + readSize + addCount
                # Invoke Lambda recursively
                invokeLam = boto3.client("lambda", region_name="us-east-2")
                # payload = "{\"Records\": [{ \"s3\": {\"bucket\": {\"name\": \"bucketName\", \"arn\": \"arn:aws:s3:::bucketName\"},\"object\": {\"key\": \""+ obj_sum.key +"\"}}}]}"
                payload = json.dumps({"Records": [{"s3": {
                    "bucket": {"name": bucket, "arn": arnstr},
                    "object": {"inputPath": inputPath, "inFileName": inFileName, "startPos": newStartPos,
                               "readSize": readSize,
                               "funcName": funcName,
                               "searchChar": searchChar, "outputPath": outputPath, "outFileName": outFileName,
                               "result": result}}}]})
                print("Recursive Function Getting Invoked with Payload : ", payload)
                resp = invokeLam.invoke(FunctionName=funcName, InvocationType="Event", Payload=payload)
            else:
                # write output to file and close
                output_file = os.path.join(bucket, outputPath, outFileName)
                fw = s3.open(output_file, "w")
                fw.write(str(result))
                fw.close()
                print("Function : Done ")