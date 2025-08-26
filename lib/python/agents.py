from strands import Agent, tool
from strands.models import BedrockModel
import agent_tools


# ███████ ██    ██ ██████       █████   ██████  ███████ ███    ██ ████████ ███████ 
# ██      ██    ██ ██   ██     ██   ██ ██       ██      ████   ██    ██    ██      
# ███████ ██    ██ ██████      ███████ ██   ███ █████   ██ ██  ██    ██    ███████ 
#      ██ ██    ██ ██   ██     ██   ██ ██    ██ ██      ██  ██ ██    ██         ██ 
# ███████  ██████  ██████      ██   ██  ██████  ███████ ██   ████    ██    ███████ 

@tool
def use_general_stock_agent(query: str):
    """Creates an agent specialized in retrieving general stock information.
    
    Args:
        query: Natural language query about stock information
        
    Returns:
        String response with relevant stock information
    """
    agent = Agent(
        model=BedrockModel(model_id="us.anthropic.claude-3-5-haiku-20241022-v1:0"),
        tools=[agent_tools.get_ticker_info, agent_tools.get_ticker_news]
    )  # Initialize agent with stock info tools
    response = agent(query)
    return str(response)

@tool
def use_stock_analyst_agent(query: str):
    """Creates an agent specialized in analyst data and stock recommendations.
    
    Args:
        query: Natural language query about analyst opinions
        
    Returns:
        String response with relevant analyst information
    """
    agent = Agent(
        model=BedrockModel(model_id="us.anthropic.claude-3-5-haiku-20241022-v1:0"),
        tools=[agent_tools.get_analyst_price_targets, agent_tools.get_upgrades_downgrades, agent_tools.get_recommendations_summary, agent_tools.get_revenue_estimate]
    )
    response = agent(query)
    return str(response)

@tool
def use_financial_statements_agent(query: str):
    """Creates an agent specialized in financial statements analysis.
    
    Args:
        query: Natural language query about financial statements
        
    Returns:
        String response with relevant financial statement information
    """
    agent = Agent(
        model=BedrockModel(model_id="us.amazon.nova-pro-v1:0"),
        tools=[agent_tools.get_balance_sheet, agent_tools.get_cash_flow, agent_tools.get_income_stmt]
    )
    response = agent(query)
    return str(response)

@tool
def use_web_search_agent(query: str):
    """Creates an agent specialized in web searches.
    
    Args:
        query: The search query string to process
        
    Returns:
        String response from the web search agent
    """
    agent = Agent(
        model=BedrockModel(model_id="us.amazon.nova-pro-v1:0"),
        tools=[agent_tools.text_search]
    )
    response = agent(query)
    return str(response)

@tool
def use_generic_agent(query: str):
    """Creates a generic agent that can handle any query.

    Args:
        query: The query string to process

    Returns:
        String response from the generic agent
    """
    agent = Agent(model=BedrockModel(model_id="us.amazon.nova-premier-v1:0")) 
    response = agent(query)
    return str(response)


# ███    ███  █████  ██ ███    ██      █████   ██████  ███████ ███    ██ ████████ ███████ 
# ████  ████ ██   ██ ██ ████   ██     ██   ██ ██       ██      ████   ██    ██    ██      
# ██ ████ ██ ███████ ██ ██ ██  ██     ███████ ██   ███ █████   ██ ██  ██    ██    ███████ 
# ██  ██  ██ ██   ██ ██ ██  ██ ██     ██   ██ ██    ██ ██      ██  ██ ██    ██         ██ 
# ██      ██ ██   ██ ██ ██   ████     ██   ██  ██████  ███████ ██   ████    ██    ███████ 

def orchestrator_agent(query: str):
    """Creates a higher-level agent that can delegate to specialized agents.
    
    This orchestrator can choose between web search and financial data tools
    based on the nature of the query.
    
    Args:
        query: The user query string to process
        
    Returns:
        String response from the appropriate specialized agent
    """
    agent = Agent(
        system_prompt="You are an financial orchestrator agent that can delegate to specialized agents.  When provided a request, you will break it down into sub tasks that will be executed by the sub agents you have available.  Show your plan then delegate the tasks to the sub agents.",
        tools=[use_web_search_agent, use_financial_statements_agent, use_general_stock_agent, use_stock_analyst_agent]
    )
    response = agent(query)  
    return str(response)  

def workflow_agent(query: str):
    agent = Agent(
        system_prompt="""
        # Overview
        You are an financial workflow agent that will break down a task into a series of sub tasks defined by a directed workflow with dependencies.
        The sub tasks will be assigned to specialized sub agents with an instruction for them to follow.
        Once the workflow has been defined, you will submit the workflow for execution and print out its response.
        
        # Sub Agents
        - "web-search-agent" for searching the internet, and have access to tools: text_search
        - "financial-statements-agent" for accessing financial statements, and have access to tools: get_balance_sheet, get_cash_flow, get_income_stmt
        - "general-stock-agent" for accessing general financial information on a stock, and have access to tools: get_ticker_info, get_ticker_news
        - "stock-analyst-agent" for accessing information published by stock analysts, and have access to tools: get_analyst_price_targets, get_upgrades_downgrades, get_recommendations_summary, get_revenue_estimate
        - "generic-agent" does not have access to any tools, but can be used for reasoning and/or preparing responses.

        # Workflow
        The workflow will be a valid JSON formatted array of tasks.
        Each task will have:
        1) task_id = ID of the task. e.g. "1" 
        2) payload = Payload containing the agent & instruction to agent. e.g. { "agent": "general-stock-agent", "message": "Find out the latest set of financial ratios on AMZN." }
        3) in_dependencies = Tasks that must be completed before this task can run.  e.g. [ { "in_dependency_task_id": "1" } ]
        4) out_dependencies = Tasks that will execute after this task is completed.  e.g. [ { "out_dependency_task_id": "2" } ]
        
        ## Workflow design considerations
        1) Run as many tasks in parallel as possible to speed up processing.
        2) Break down data gathering into separate tasks (if it makes sense) to facilitate sharing of data to other sub agents via dependencies.

        ## Example of a workflow
        The following is a valid workflow definition:
        [
            {
                "task_id": "1",
                "payload": { "agent": "general-stock-agent", "message": "Find out the latest set of financial ratios on AMZN." },
                "in_dependencies": [],
                "out_dependencies": [
                    { "out_dependency_task_id": "4" }
                ]
            },
            {
                "task_id": "2",
                "payload": { "agent": "web-search-agent", "message": "Get the list of AMZN peers." },
                "in_dependencies": [],
                "out_dependencies": [
                    { "out_dependency_task_id": "3" }
                ]
            },
            {
                "task_id": "3",
                "payload": { "agent": "general-stock-agent", "message": "Get the latest set of financial ratios for each of AMZN's peers." },
                "in_dependencies": [
                    { "in_dependency_task_id": "2" }
                ],
                "out_dependencies": [
                    { "out_dependency_task_id": "4" }
                ]
            },
            {
                "task_id": "4",
                "payload": { "agent": "generic-agent", "message": "Perform AMZN peer analysis based on the information." },
                "in_dependencies": [
                    { "in_dependency_task_id": "1" },
                    { "in_dependency_task_id": "3" }
                ],
                "out_dependencies": [
                    { "out_dependency_task_id": "9" }
                ]
            },
            {
                "task_id": "5",
                "payload": { "agent": "financial-statements-agent", "message": "Analyze the latest AMZN balance sheet." },
                "in_dependencies": [],
                "out_dependencies": [
                    { "out_dependency_task_id": "8" }
                ]
            },
            {
                "task_id": "6",
                "payload": { "agent": "financial-statements-agent", "message": "Analyze the latest AMZN income statement." },
                "in_dependencies": [],
                "out_dependencies": [
                    { "out_dependency_task_id": "8" }
                ]
            },
            {
                "task_id": "7",
                "payload": { "agent": "financial-statements-agent", "message": "Analyze the latest AMZN cashflow." },
                "in_dependencies": [],
                "out_dependencies": [
                    { "out_dependency_task_id": "8" }
                ]
            },
            {
                "task_id": "8",
                "payload": { "agent": "generic-agent", "message": "Analyze the financial statements of AMZN and identify insights regarding its performance and highlight any concerns." },
                "in_dependencies": [
                    { "in_dependency_task_id": "5" },
                    { "in_dependency_task_id": "6" },
                    { "in_dependency_task_id": "7" },
                ],
                "out_dependencies": [
                    { "out_dependency_task_id": "9" }
                ]
            },
            {
                "task_id": "9",
                "payload": { "agent": "generic-agent", "message": "Combine the information gathered and present them as a report." },
                "in_dependencies": [
                    { "in_dependency_task_id": "4" },
                    { "in_dependency_task_id": "8" }
                ],
                "out_dependencies": []
            }
        ]
        """,
        tools=[agent_tools.submit_workflow]
    )
    response = agent(query)
    return str(response)