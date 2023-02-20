import json
import boto3
import time

def lambda_handler(event, context):
    print(event)
    
    print(event["Records"][0]["s3"]["object"])
    object_key=event["Records"][0]["s3"]["object"]["key"]
    object_size=event["Records"][0]["s3"]["object"]["size"]
    
    #trigger only for event including folder
    if object_size==0:
        print("Lambda skipped")
        return {
            'statusCode': 201,
            'body': json.dumps('Lambda Skipped!')
        }
        
    else:
        time.sleep(10)
        client = boto3.client('sagemaker')
        folder_name=object_key.split("/")[0]
        print(f"folder_name: {folder_name}")
        print("Lambda Triggering pipeline.......")
        response = client.start_pipeline_execution(
            PipelineName=f"AbalonePipeline",
        #     PipelineExecutionDisplayName='string',
            PipelineParameters=[
                {
                    'Name': 'InputData',
                    'Value': f's3://my-bucket-for-ml-usecases/{folder_name}'
                },
            ],
        #     PipelineExecutionDescription='string',
        #     ClientRequestToken='string',
        #     ParallelismConfiguration={
        #         'MaxParallelExecutionSteps': 123
        #     }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
