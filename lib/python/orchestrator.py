import json
import uuid
import boto3
import os
import time
import urllib.parse
import logging
import functools
import random
from botocore.exceptions import ClientError
from typing import Dict, List, Tuple, Optional, Any, Union
import traceback
from decimal import Decimal

# Configure logging
logger = logging.getLogger('orchestrator')

# Constants for task status
TASK_STATUS_PENDING = 'PENDING'
TASK_STATUS_COMPLETED = 'COMPLETED'
TASK_STATUS_ERROR = 'ERROR'

# Constants for lock status
LOCK_STATUS_LOCKED = 'YES'
LOCK_STATUS_UNLOCKED = 'NO'

# Constants for DynamoDB attribute names
ATTR_RUN_ID = 'run_id'
ATTR_TASK_ID = 'task_id'
ATTR_STATUS = 'status'
ATTR_VERSION = 'version'
ATTR_LOCKED = 'locked'
ATTR_PAYLOAD = 'payload'
ATTR_TASK_OUTPUT = 'task_output'
ATTR_ERROR_MESSAGE = 'error_message'
ATTR_IN_DEPENDENCIES = 'in_dependencies'
ATTR_OUT_DEPENDENCIES = 'out_dependencies'
ATTR_FUNCTION_NAME = 'function_name'
ATTR_TTL = 'ttl'

# TTL configuration (10 days x 24 hours x 60 mins x 60 seconds)
TTL_DURATION_SECONDS = 10 * 24 * 60 * 60

# Constants for additional DynamoDB attribute names
ATTR_TASK_DURATION = 'task_duration'
ATTR_RETRY_ENABLED = 'retry_enabled'

# Error handling decorator
def handle_exceptions(func):
    """Decorator to handle exceptions in a consistent way"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ClientError as e:
            import traceback
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            stack_trace = traceback.format_exc()
            logger.error(f"Error in {func.__name__}: {error_code} - {error_message}\nStack trace:\n{stack_trace}")
            if 'run_id' in kwargs and 'task_id' in kwargs:
                logger.error(f"Task {kwargs['run_id']}/{kwargs['task_id']} failed")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': f"Error: {error_code} - {error_message}",
                    'stack_trace': stack_trace
                })
            }
        except Exception as e:
            import traceback
            stack_trace = traceback.format_exc()
            logger.error(f"Error in {func.__name__}: {str(e)}\nStack trace:\n{stack_trace}")
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'error': str(e),
                    'stack_trace': stack_trace
                })
            }
    return wrapper

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
lambda_client = boto3.client('lambda')
s3 = boto3.client('s3')
ssm = boto3.client('ssm')

# Get configuration from SSM Parameter Store
def get_config():
    """
    Get configuration values from SSM Parameter Store or environment variables.
    
    Retrieves the S3 bucket name and DynamoDB table name from SSM Parameter Store.
    Falls back to environment variables if SSM parameters are not available.
    
    Returns:
        tuple: (bucket_name, table_name) - The names of the S3 bucket and DynamoDB table
    """
    try:
        # Get bucket name from SSM Parameter Store
        bucket_response = ssm.get_parameter(
            Name='/on-demand-orchestrator/payload-bucket',
            WithDecryption=True
        )
        bucket_name = bucket_response['Parameter']['Value']
        
        # Get table name from SSM Parameter Store
        table_response = ssm.get_parameter(
            Name='/on-demand-orchestrator/workflow-table',
            WithDecryption=True
        )
        table_name = table_response['Parameter']['Value']
        
        return bucket_name, table_name
    except Exception as e:
        logger.warning(f"Error retrieving configuration from SSM Parameter Store: {str(e)}")
        logger.info("Falling back to environment variables")
        # Fallback to environment variables if parameters are not available
        return os.environ.get('PAYLOAD_BUCKET'), os.environ.get('WORKFLOW_TABLE')

# Get configuration
PAYLOAD_BUCKET, WORKFLOW_TABLE = get_config()

@handle_exceptions
def get_final_task_output(run_id: str) -> Optional[Dict[str, Any]]:
    """
    Get the output of the final task in a workflow if its status is COMPLETED.
    
    Args:
        run_id (str): The workflow run ID
        
    Returns:
        Optional[Dict[str, Any]]: The output of the final task if completed, None otherwise
    """
    table = dynamodb.Table(WORKFLOW_TABLE)
    
    # Query all tasks for the given run_id
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key(ATTR_RUN_ID).eq(run_id)
    )
    
    if not response.get('Items'):
        logger.warning(f"No tasks found for run_id: {run_id}")
        return None
        
    tasks = response['Items']
    
    # Find tasks with no out_dependencies (final tasks)
    final_tasks = [task for task in tasks if not task.get(ATTR_OUT_DEPENDENCIES) or len(task.get(ATTR_OUT_DEPENDENCIES)) == 0]
    
    if not final_tasks:
        logger.warning(f"No final task found for run_id: {run_id}")
        return None
    
    # Check if the final task is completed
    for final_task in final_tasks:
        if final_task.get(ATTR_STATUS) == TASK_STATUS_COMPLETED and ATTR_TASK_OUTPUT in final_task:
            # Get the output from S3
            return get_s3_object(final_task[ATTR_TASK_OUTPUT])
    
    return None

@handle_exceptions
def submit_workflow(workflow_definition: List[Dict[str, Any]], run_id: Optional[str] = None, function_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Submit a workflow and wait for its completion.
    
    This function submits a workflow and waits synchronously until all tasks are completed
    or an error occurs. It polls the workflow status periodically and returns the final result.
    
    Args:
        workflow_definition (dict): The workflow definition containing tasks and dependencies
        run_id (str, optional): A unique identifier for the workflow run. If not provided, a UUID will be generated.
    
    Returns:
        dict: Response with status code and message indicating workflow completion status
    
    The workflow definition should be a JSON array of tasks with the following structure:
    [
        {
            "task_id": "1",
            "payload": { "message": "..." },
            "in_dependencies": [
                { "in_dependency_task_id": "1" }
            ],
            "out_dependencies": [
                { "out_dependency_task_id": "2" }
            ]
        },
        ...
    ]
    """
    response = submit_workflow_async(workflow_definition, run_id, function_name)
    if response["statusCode"] == 200:
        run_id = json.loads(response["body"])["run_id"]
        print(f"Workflow submitted with run_id: {run_id}")
        while True:
            status = get_workflow_status(run_id)
            print(f"Run status: {status}")
            if status == TASK_STATUS_COMPLETED:
                final_output = get_final_task_output(run_id)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Workflow completed successfully',
                        'run_id': run_id,
                        'output': final_output
                    }),
                }
            if status == TASK_STATUS_ERROR:
                return {
                    'statusCode': 500,
                    'body': json.dumps({
                        'message': 'Workflow encountered error',
                        'run_id': run_id
                    }),
                }
            time.sleep(1)
    else:
        return response

@handle_exceptions
def submit_workflow_async(workflow_definition: List[Dict[str, Any]], run_id: Optional[str] = None, function_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Submit a workflow by processing the workflow definition.
    
    This function submits a workflow asynchronously and returns immediately without waiting
    for the workflow to complete.
    
    Args:
        workflow_definition (dict): The workflow definition containing tasks and dependencies
        run_id (str, optional): A unique identifier for the workflow run. If not provided, a UUID will be generated.
    
    Returns:
        dict: Response with status code and message indicating submission status
    
    The workflow definition should be a JSON array of tasks with the following structure:
    [
        {
            "task_id": "1",
            "payload": { "message": "..." },
            "in_dependencies": [
                { "in_dependency_task_id": "1" }
            ],
            "out_dependencies": [
                { "out_dependency_task_id": "2" }
            ]
        },
        ...
    ]
    """
    # Generate a new UUID if run_id is not provided
    if run_id is None:
        run_id = str(uuid.uuid4())
        
    table = dynamodb.Table(WORKFLOW_TABLE)
    
    # Process each task in the workflow definition
    for task in workflow_definition:
        # Save payload to S3
        if ATTR_PAYLOAD in task and task[ATTR_PAYLOAD]:
            payload_key = f"{run_id}/{task[ATTR_TASK_ID]}/payload.json"
            s3path = put_s3_object(PAYLOAD_BUCKET, payload_key, task[ATTR_PAYLOAD])
            
            # Replace payload with S3 path
            task[ATTR_PAYLOAD] = s3path
        
        # Add status, version, locked, and TTL attributes
        task[ATTR_STATUS] = TASK_STATUS_PENDING
        task[ATTR_VERSION] = 1
        task[ATTR_LOCKED] = LOCK_STATUS_UNLOCKED
        task[ATTR_RUN_ID] = run_id
        task[ATTR_TTL] = int(time.time()) + TTL_DURATION_SECONDS  # Set TTL to current time + 24 hours
        
        if function_name:
            task[ATTR_FUNCTION_NAME] = function_name
        
        # Insert task into DynamoDB
        table.put_item(Item=task)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Workflow submitted successfully',
            'run_id': run_id
        }),
    }

def get_s3_object(s3_path: str) -> Dict[str, Any]:
    """
    Get an object from S3 given its S3 path (s3://bucket/key)
    
    Args:
        s3_path (str): The S3 path in the format s3://bucket/key
        
    Returns:
        dict: The parsed JSON content of the S3 object
    """
    parsed_url = urllib.parse.urlparse(s3_path)
    bucket = parsed_url.netloc
    key = parsed_url.path.lstrip('/')
    
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def put_s3_object(bucket_name: str, key: str, data: Dict[str, Any]) -> str:
    """
    Save data to an S3 object
    
    Args:
        bucket_name (str): The S3 bucket name
        key (str): The S3 object key
        data (dict): The data to save (will be converted to JSON)
        
    Returns:
        str: The S3 path to the saved object
    """
    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(data),
        ContentType='application/json'
    )
    return f"s3://{bucket_name}/{key}"

@handle_exceptions
def review_dynamodb_stream_records(records: List[Dict[str, Any]], task_function_name: str) -> Dict[str, Any]:
    """
    Process DynamoDB stream records to identify tasks ready for execution.
    
    Args:
        records (list): List of DynamoDB stream records
        task_function_name (str): Name of the Lambda function to execute tasks
        
    Returns:
        dict: Response with status code and message
    """
    # Group records by run_id and task_id to avoid duplicate processing
    tasks_to_process = {}
    
    # Process DynamoDB stream records
    for record in records:
        # Only process INSERT or MODIFY events
        if record['eventName'] not in ['INSERT', 'MODIFY']:
            continue
            
        # Get the new image (the updated/inserted item)
        if 'NewImage' not in record['dynamodb']:
            continue
            
        # Parse the DynamoDB stream record
        new_image = record['dynamodb']['NewImage']
        
        # Only process tasks with status=PENDING or locked=NO
        if 'status' not in new_image or new_image['status']['S'] != TASK_STATUS_PENDING or new_image['locked']['S'] != LOCK_STATUS_UNLOCKED :
            continue
            
        # Extract task identifiers
        run_id = new_image['run_id']['S']
        task_id = new_image['task_id']['S']
        
        # Add to tasks to process (deduplicating by run_id/task_id)
        tasks_to_process[(run_id, task_id)] = True
    
    # Process each unique task
    for run_id, task_id in tasks_to_process.keys():
        # Get the task details
        task, error = get_task(run_id, task_id)
        if not task:
            logger.warning(error)
            continue
        
        # Check if all dependencies are satisfied
        dependencies_satisfied = True
        
        # If there are no dependencies, the task is ready to execute
        if 'in_dependencies' not in task or not task['in_dependencies']:
            dependencies_satisfied = True
        else:
            # Check each dependency
            for dependency in task['in_dependencies']:
                if dependency.get('in_dependency_status') != TASK_STATUS_COMPLETED:
                    dependencies_satisfied = False
                    break
        
        # If all dependencies are satisfied, invoke the execute function
        if dependencies_satisfied:
            logger.info(f"All dependencies satisfied for task {run_id}/{task_id}, invoking execute function")
            execute_task(run_id, task_id, task_function_name)
                    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Monitor function executed successfully'})
    }

def prepare_task_execution_input(task: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare the execution input for a task by retrieving payload and dependency outputs.
    
    Args:
        task: The task data from DynamoDB
        
    Returns:
        Dict containing payload and dependency outputs
    """
    # Get the payload from S3
    payload = None
    if ATTR_PAYLOAD in task and task[ATTR_PAYLOAD]:
        payload = get_s3_object(task[ATTR_PAYLOAD])
    
    # Get the outputs from dependencies
    dependency_outputs = []
    if ATTR_IN_DEPENDENCIES in task and task[ATTR_IN_DEPENDENCIES]:
        for dependency in task[ATTR_IN_DEPENDENCIES]:
            dep_task_id = dependency.get('in_dependency_task_id')
            
            task_output_path = dependency.get(ATTR_TASK_OUTPUT)
            if task_output_path:
                output = get_s3_object(task_output_path)
                dependency_outputs.append({
                    ATTR_TASK_ID: dep_task_id,
                    ATTR_TASK_OUTPUT: output
                })
    
    # Prepare the execution input
    return {
        'payload': payload,
        'dependency_outputs': dependency_outputs
    }


@handle_exceptions
def execute_task(run_id: str, task_id: str, task_function_name: str) -> bool:
    """
    Execute a task and update its status and dependencies.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        task_function_name (str): The name of the Lambda function to execute
    
    Returns:
        bool: True if successful, False otherwise
    """
    max_retries = 10
    retry_count = 0
    backoff_time = 1  # Start with 1 second backoff
    
    while retry_count < max_retries:
        # Try to lock the task
        if lock_task(run_id, task_id):
            try:
                # Get the task from DynamoDB
                task, error = get_task(run_id, task_id)
                if not task:
                    logger.error(error)
                    unlock_task(run_id, task_id)
                    return False
                
                # Override execute function name if available
                function_name = task.get(ATTR_FUNCTION_NAME, None)
                if function_name:
                    task_function_name = function_name

                # Prepare execution input
                execution_input = {
                    "function_name": task_function_name, 
                    "payload": {
                        "run_id": run_id,
                        "task_id": task_id,
                        "payload": task["payload"],
                        "retry_enabled": task.get(ATTR_RETRY_ENABLED, True),  # Default to True for backward compatibility
                        "in_dependencies": task["in_dependencies"]
                    }
                }
                
                # Invoke the execute function
                response = lambda_client.invoke(
                    FunctionName=f"ExecuteHandlerFunction",
                    InvocationType='Event', #RequestResponse for sync, Event for async
                    Payload=json.dumps(execution_input)
                )
                
                return True
                
            except Exception as e:
                logger.error(f"Error in execute_task: {str(e)}")
                unlock_task(run_id, task_id)
                return False
        else:
            # Exponential backoff with jitter
            sleep_time = backoff_time * (0.9 + 0.2 * random.random())  # Add 10% jitter
            time.sleep(sleep_time)
            backoff_time = min(backoff_time * 2, 5)  # Double the backoff time with a maximum of 5 seconds
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"Failed to lock task {run_id}/{task_id} after {max_retries} attempts")
                return False
    
    return False


def get_task(run_id: str, task_id: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Get a task from DynamoDB by run_id and task_id.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        
    Returns:
        tuple: (task_item, None) if successful, (None, error_message) if failed
    """
    table = dynamodb.Table(WORKFLOW_TABLE)
    
    response = table.get_item(
        Key={
            'run_id': run_id,
            'task_id': task_id
        }
    )
    
    if 'Item' not in response:
        return None, f"Task {run_id}/{task_id} not found in DynamoDB"
        
    return response['Item'], None

@handle_exceptions
def update_task_with_condition(run_id: str, task_id: str, update_expression: str, 
                             condition_expression: str, expression_names: Dict[str, str], 
                             expression_values: Dict[str, Any], operation_name: str) -> bool:
    """
    Update a task in DynamoDB with optimistic locking.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        update_expression (str): DynamoDB update expression
        condition_expression (str): DynamoDB condition expression
        expression_names (dict): DynamoDB expression attribute names
        expression_values (dict): DynamoDB expression attribute values
        operation_name (str): Name of the operation for logging
        
    Returns:
        bool: True if successful, False otherwise
    """
    table = dynamodb.Table(WORKFLOW_TABLE)
    
    try:
        table.update_item(
            Key={
                'run_id': run_id,
                'task_id': task_id
            },
            UpdateExpression=update_expression,
            ConditionExpression=condition_expression,
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values
        )
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.warning(f"Failed to {operation_name} task {run_id}/{task_id} due to condition check failure")
        else:
            logger.error(f"Error {operation_name} task {run_id}/{task_id}: {str(e)}")
        return False

@handle_exceptions
def set_task_lock_state(run_id: str, task_id: str, lock: bool) -> bool:
    """
    Set the lock state of a task.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        lock (bool): True to lock, False to unlock
        
    Returns:
        bool: True if successful, False otherwise
    """
    task, error = get_task(run_id, task_id)
    if not task:
        logger.warning(error)
        return False
    
    current_state = task.get('locked')
    expected_state = 'NO' if lock else 'YES'
    new_state = 'YES' if lock else 'NO'
    operation = 'lock' if lock else 'unlock'
    
    if current_state != expected_state:
        logger.warning(f"Task {run_id}/{task_id} is {'already locked' if lock else 'not locked'}")
        return False
    
    current_version = task.get('version', 1)
    
    return update_task_with_condition(
        run_id, 
        task_id,
        "SET #locked = :new_state, #version = :new_version",
        "#locked = :current_state AND #version = :curr_version",
        {
            '#locked': 'locked',
            '#version': 'version'
        },
        {
            ':new_state': new_state,
            ':current_state': expected_state,
            ':curr_version': current_version,
            ':new_version': current_version + 1
        },
        operation
    )

@handle_exceptions
def lock_task(run_id: str, task_id: str) -> bool:
    """
    Lock a task for processing.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    return set_task_lock_state(run_id, task_id, True)

@handle_exceptions
def unlock_task(run_id: str, task_id: str) -> bool:
    """
    Unlock a task after processing.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        
    Returns:
        bool: True if successful, False otherwise
    """
    return set_task_lock_state(run_id, task_id, False)

@handle_exceptions
def update_task_error(run_id: str, task_id: str, error_message: str) -> bool:
    """
    Update task status to ERROR and store the error message.
    
    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        error_message (str): The error message to store
    """
    task, error = get_task(run_id, task_id)
    if not task:
        logger.error(error)
        return False
        
    current_version = task.get('version', 1)
    
    return update_task_with_condition(
        run_id,
        task_id,
        "SET #status = :status, #error_message = :error_message, #version = :new_version",
        "#version = :curr_version",
        {
            '#status': 'status',
            '#error_message': 'error_message',
            '#version': 'version'
        },
        {
            ':status': TASK_STATUS_ERROR,
            ':error_message': error_message,
            ':curr_version': current_version,
            ':new_version': current_version + 1
        },
        "update error status"
    )

@handle_exceptions
def update_task_completion(run_id: str, task_id: str, out_dependencies: List[Dict[str, str]], task_output: Dict[str, Any], task_duration: float = None) -> bool:
    """
    Save task_output into s3 object and get its s3 path.

    For each task in out_dependencies, 
        Get the out_dependency_task_id
        call function update_out_dependency_task(run_id, out_dependency_task_id, task_id, s3_path)
        
    Update DynamoDB record using run_id, and task_id, set status = "COMPLETED", task_output = s3_path,
    and task_duration if provided

    Args:
        run_id (str): The workflow run ID
        task_id (str): The task ID
        out_dependencies (List[Dict[str, str]]): List of output dependencies
        task_output (Dict[str, Any]): The task output
        task_duration (float, optional): The task execution duration in seconds
    """
    output_key = f"{run_id}/{task_id}/output.json"
    s3_path = put_s3_object(PAYLOAD_BUCKET, output_key, task_output)
    
    # Update dependencies
    if out_dependencies:
        for out_dep in out_dependencies:
            out_dep_task_id = out_dep.get('out_dependency_task_id')
            if out_dep_task_id:
                update_out_dependency_task(run_id, out_dep_task_id, task_id, s3_path)
    
    # Get current version for optimistic locking
    task, error = get_task(run_id, task_id)
    if not task:
        logger.error(error)
        return False
        
    current_version = task.get('version', 1)
    
    # Build update expression and attribute values based on whether task_duration is provided
    update_expression = "SET #status = :status, #task_output = :output, #version = :new_version"
    expression_attribute_names = {
        '#status': 'status',
        '#task_output': 'task_output',
        '#version': 'version'
    }
    expression_attribute_values = {
        ':status': TASK_STATUS_COMPLETED,
        ':output': s3_path,
        ':curr_version': current_version,
        ':new_version': current_version + 1
    }
    
    # Add task_duration if provided
    if task_duration is not None:
        update_expression += ", #task_duration = :task_duration"
        expression_attribute_names['#task_duration'] = ATTR_TASK_DURATION
        expression_attribute_values[':task_duration'] = Decimal(str(round(task_duration, 3)))  # Convert to Decimal
    
    return update_task_with_condition(
        run_id,
        task_id,
        update_expression,
        "#version = :curr_version",
        expression_attribute_names,
        expression_attribute_values,
        "update task completion"
    )

@handle_exceptions
def update_out_dependency_task(run_id: str, task_id: str, in_dependency_task_id: str, s3_path: str) -> bool:
    """
    lock task
    retrieve DynamoDB record using run_id and task_id
    for each in_dependencies tasks, search for the record matching with in_dependency_task_id
        update that particular in_dependencies task with in_dependency_status = COMPLETED and task_output = s3_path
    unlock task
    """
    max_retries = 10
    retry_count = 0
    backoff_time = 1  # Start with 1 second backoff
    
    while retry_count < max_retries:
        if lock_task(run_id, task_id):
            try:
                # Get the dependent task
                dep_task, error = get_task(run_id, task_id)
                if not dep_task:
                    logger.error(error)
                    unlock_task(run_id, task_id)
                    return False
                    
                current_version = dep_task.get(ATTR_VERSION, 1)
                
                # Find the dependency to update
                if ATTR_IN_DEPENDENCIES in dep_task:
                    updated = False
                    in_dependencies = dep_task[ATTR_IN_DEPENDENCIES]
                    
                    for i, in_dep in enumerate(in_dependencies):
                        if in_dep.get('in_dependency_task_id') == in_dependency_task_id:
                            # Update the dependency status and add output path
                            update_expression = f"SET {ATTR_IN_DEPENDENCIES}[{i}].in_dependency_status = :completed_status, {ATTR_IN_DEPENDENCIES}[{i}].{ATTR_TASK_OUTPUT} = :task_output_path, #{ATTR_VERSION} = :new_version"
                            success = update_task_with_condition(
                                run_id,
                                task_id,
                                update_expression,
                                f"#{ATTR_VERSION} = :curr_version",
                                {
                                    f'#{ATTR_VERSION}': ATTR_VERSION
                                },
                                {
                                    ':completed_status': TASK_STATUS_COMPLETED,
                                    ':task_output_path': s3_path,
                                    ':curr_version': current_version,
                                    ':new_version': current_version + 1
                                },
                                "update dependency"
                            )
                            if not success:
                                unlock_task(run_id, task_id)
                                return False
                            updated = True
                            break
                    
                    if not updated:
                        logger.warning(f"Dependency {in_dependency_task_id} not found in task {run_id}/{task_id}")
                
                unlock_task(run_id, task_id)
                return True
                
            except Exception as e:
                logger.error(f"Error in update_out_dependency_task: {str(e)}")
                unlock_task(run_id, task_id)
                return False
        else:
            # Exponential backoff with jitter
            sleep_time = backoff_time * (0.9 + 0.2 * random.random())  # Add 10% jitter
            time.sleep(sleep_time)
            backoff_time = min(backoff_time * 2, 5)  # Double the backoff time with a maximum of 5 seconds
            retry_count += 1
            if retry_count >= max_retries:
                logger.error(f"Failed to lock task {run_id}/{task_id} after {max_retries} attempts")
                return False
    
    return False


@handle_exceptions
def get_workflow_status(run_id: str) -> str:
    """
    Check the status of all task records under a specific run_id.
    
    Args:
        run_id (str): The workflow run ID
        
    Returns:
        str: 'ERROR' if any task has status=ERROR, 
             'SUCCESS' if all tasks have status=SUCCESS,
             'PENDING' otherwise
    """
    table = dynamodb.Table(WORKFLOW_TABLE)
    
    # Query all tasks for the given run_id
    response = table.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key(ATTR_RUN_ID).eq(run_id)
    )
    
    if not response.get('Items'):
        logger.warning(f"No tasks found for run_id: {run_id}")
        return TASK_STATUS_ERROR
        
    tasks = response['Items']
    
    # Check if any task has ERROR status
    for task in tasks:
        if task.get(ATTR_STATUS) == TASK_STATUS_ERROR:
            return TASK_STATUS_ERROR
    
    # Check if all tasks have SUCCESS status
    all_success = all(task.get(ATTR_STATUS) == TASK_STATUS_COMPLETED for task in tasks)
    if all_success:
        return TASK_STATUS_COMPLETED
        
    # Otherwise, return PENDING
    return TASK_STATUS_PENDING