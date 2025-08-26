import json
import boto3
import os
import orchestrator

def handler(event, context):
    """
    Monitor DynamoDB streams for tasks with status=PENDING and check if they are ready to execute.
    If all dependencies are satisfied, invoke the execute function.
    """
    records = event.get('Records', [])
    task_function_name = os.environ['TASK_FUNCTION_NAME']

    return orchestrator.review_dynamodb_stream_records(records, task_function_name)
