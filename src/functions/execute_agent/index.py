import json
import agents
from orchestrator import prepare_task_execution_input # Optional: to handle payload retrieval 
  
def handler(event, content):
    # Extract payload and dependency outputs
    event = prepare_task_execution_input(event)    
    payload = event.get('payload', {})
    dependency_outputs = event.get('dependency_outputs', [])

    agent = payload["agent"]
    message = payload["message"]
    
    upstream_responses = "\n\n".join([ json.dumps(dep_output.get('task_output', {})) for dep_output in dependency_outputs ])

    if agent == 'web-search-agent':
        return { "statusCode": 200, "result": agents.use_web_search_agent(upstream_responses + "\n\n" + message) }

    elif agent == 'financial-statements-agent':
        return { "statusCode": 200, "result": agents.use_financial_statements_agent(upstream_responses + "\n\n" + message) }
    
    elif agent == 'general-stock-agent':
        return { "statusCode": 200, "result": agents.use_general_stock_agent(upstream_responses + "\n\n" + message) }
    
    elif agent == 'stock-analyst-agent':
        return { "statusCode": 200, "result": agents.use_stock_analyst_agent(upstream_responses + "\n\n" + message) }

    else:
        return { "statusCode": 200, "result": agents.use_generic_agent(upstream_responses + "\n\n" + message) }
