import json
from orchestrator import prepare_task_execution_input # Optional: to handle payload retrieval 

def handler(event, content):
    # Extract payload and dependency outputs
    event = prepare_task_execution_input(event)    
    payload = event.get('payload', {})
    dependency_outputs = event.get('dependency_outputs', [])

    operation = payload["operation"]
    num1 = payload.get("num1", None)
    num2 = payload.get("num2", None)

    results = [ dep_output.get('task_output', {}) for dep_output in dependency_outputs ]
    
    if num1 is None and num2 is None:
        num1 = results[0]["result"]
        num2 = results[1]["result"]
    elif num1 is None:
        num1 = results[0]["result"]
    elif num2 is None:
        num2 = results[0]["result"]

    if operation == "add":
        return { "statusCode": 200, "result": num1 + num2 }
    elif operation == "subtract":
        return { "statusCode": 200, "result": num1 - num2 }
    elif operation == "multiply":
        return { "statusCode": 200, "result": num1 * num2 }
    elif operation == "divide":
        return { "statusCode": 200, "result": num1 / num2 }
    else:
        return { "statusCode": 500, "result": "Invalid operation" }


