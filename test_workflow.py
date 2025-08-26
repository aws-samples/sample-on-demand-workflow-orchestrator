import json
import boto3
import uuid
import os
import sys
import argparse
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
lib_python_path = os.path.join(current_dir, 'lib', 'python')
sys.path.insert(0, lib_python_path)
import orchestrator as orchestrator
from agents import workflow_agent

def main():
    function_name = "ExecuteMathFunction" # default to math example
    if len(sys.argv) <= 2:
        if len(sys.argv) == 2:
            function_name = sys.argv[1] 
        if function_name not in ("ExecuteMathFunction", "ExecuteAgentFunction"):
            print("Function name must be ExecuteMathFunction or ExecuteAgentFunction")
            exit()
    else:
        print("Program takes in only 1 parameter.  Usage: python test-workflow.py <ExecuteMathFunction or ExecuteAgentFunction>")
        exit()

    if function_name == "ExecuteAgentFunction":       

        request = "perform an analysis of AMZN"

        print("\n\n--------workflow_agent (start)--------")
        start_time = time.perf_counter()
        response = workflow_agent(request)
        end_time = time.perf_counter()
        workflow_agent_elapsed_time = end_time - start_time
        print("\n--------workflow_agent (end)--------")

        print(f"\nWorkflow agent elapsed time: {workflow_agent_elapsed_time:.6f} seconds")


    elif function_name == "ExecuteMathFunction":
            
        # Define the workflow to calculate (((5 + 3) / (6 - 3)) - 2) * 40
        # Note: retry_enabled defaults to True for all tasks (backward compatibility)
        workflow_definition = [
            {
                "task_id": "1",
                "payload": {
                    "operation": "add",
                    "num1": 5,
                    "num2": 3
                },
                # retry_enabled: True (default - retries enabled)
                "in_dependencies": [],
                "out_dependencies": [
                    {"out_dependency_task_id": "3"}
                ]
            },
            {
                "task_id": "2",
                "payload": {
                    "operation": "subtract",
                    "num1": 6,
                    "num2": 3
                },
                "in_dependencies": [],
                "out_dependencies": [
                    {"out_dependency_task_id": "3"}
                ]
            },
            {
                "task_id": "3",
                "payload": {
                    "operation": "divide",
                    "num1": None,
                    "num2": None
                },
                "in_dependencies": [
                    {"in_dependency_task_id": "1"},
                    {"in_dependency_task_id": "2"}
                ],
                "out_dependencies": [
                    {"out_dependency_task_id": "4"}
                ]
            },
            {
                "task_id": "4",
                "payload": {
                    "operation": "subtract",
                    "num1": None,
                    "num2": 2
                },
                "in_dependencies": [
                    {"in_dependency_task_id": "3"}
                ],
                "out_dependencies": [
                    {"out_dependency_task_id": "5"}
                ]
            },
            {
                "task_id": "5",
                "payload": {
                    "operation": "multiply",
                    "num1": None,
                    "num2": 40
                },
                "retry_enabled": False,  # Example: Disable retries for final calculation
                "in_dependencies": [
                    {"in_dependency_task_id": "4"}
                ],
                "out_dependencies": []
            }
        ]


        print("\n\n--------math example (start)--------")
        start_time = time.perf_counter()
        result = orchestrator.submit_workflow(workflow_definition, function_name=function_name)
        print(result)
        end_time = time.perf_counter()
        math_example_elapsed_time = end_time - start_time
        print(f"\nMath example elapsed time: {math_example_elapsed_time:.6f} seconds")
        print("\n--------math example (end)--------")
    
if __name__ == "__main__":
    main()
