import os
import json
import s3fs
import boto3

#This is minFunction function

s3 = s3fs.S3FileSystem(anon=False)


def lambda_handler(event, context):
    # print("Received event: " + str(event))

    bucket = 'bucket-parallel'
    arnstr = 'arn:aws:s3:::bucket-parallel'
    inputPath = 'input'
    inFileName = 'input.txt'
    outputPath = 'output'
    outFileName = 'intrMinOutput.txt'


    input_file = os.path.join(bucket, inputPath, inFileName)

    with s3.open(input_file, 'r') as f:
        print("Reading File", input_file)
        bytesRead = f.read()
        print("Return Bytes: ", bytesRead)

        # print("-------- Now Process these bytes ---------------")
        num_list = map(int, bytesRead.split())
        result = min(num_list)
        print("Bytes Read: ", bytesRead)
        print("Max Value in bytes read: ", result)
        # write output to file and close
        output_file = os.path.join(bucket, outputPath, outFileName)
        fw = s3.open(output_file, "w")
        fw.write(str(result))
        fw.close()
        print("Function : Done ")
