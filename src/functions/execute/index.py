import json
import boto3
import os
import traceback
import logging
import time
from orchestrator import get_task, update_task_completion, update_task_error, unlock_task

lambda_client = boto3.client('lambda')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    function_name = event.get("function_name")
    payload = event.get("payload")
    run_id = payload.get("run_id")
    task_id = payload.get("task_id")
    retry_enabled = payload.get("retry_enabled", True)  # Default to True for backward compatibility
    max_retries = 3 if retry_enabled else 1
    for attempt in range(max_retries):
        try:
            start_time = time.time()
            # Invoke the task function
            response = lambda_client.invoke(
                FunctionName=f"{function_name}",
                InvocationType='RequestResponse', #RequestResponse for sync, Event for async
                Payload=json.dumps(payload)
            )
            duration = time.time() - start_time
            
            lambda_execution_status_code = response['StatusCode']
            if lambda_execution_status_code != 200:
                raise Exception(f"Lambda execution failed with status code: {lambda_execution_status_code}")
            
            # Check if there's a payload
            if 'Payload' in response:
                execute_payload = json.loads(response['Payload'].read().decode('utf-8'))
                status_code = execute_payload.get("statusCode")
                if status_code != 200:
                    raise Exception(f"Function execution failed with status code: {status_code}, payload: {execute_payload}")
                else:
                    task, error = get_task(run_id, task_id)

                    # Proceed with successful execution
                    out_dependencies = task.get("out_dependencies", [])
                    update_task_completion(run_id, task_id, out_dependencies, execute_payload, duration)

                    # Unlock the task
                    unlock_task(run_id, task_id)
                    return response
            else:
                raise Exception("No payload in the response.")

        except Exception as ex:
            # Update task status to ERROR
            update_task_error(run_id, task_id, f"{ex} - {traceback.format_exc()}")
            logger.error(f"Run ID:{run_id}/ Task ID:{task_id}/ Attempt:{attempt+1} - failed with error: {ex} - {traceback.format_exc()}")
            
    # Unlock the task
    unlock_task(run_id, task_id)
    return response

    
    
