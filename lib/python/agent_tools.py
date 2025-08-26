from strands import tool
import orchestrator
import json
import web_search_tools
import yfinance as yf

@tool
def text_search(query: str):
    """Performs a web search for a given query.

    Args:
        query: The search query string

    Returns:
        Dictionary containing search results
    """
    return web_search_tools.text_search(query, max_results=5)

# General Stock Agent
@tool
def get_ticker_info(ticker: str):
    """Retrieves comprehensive information about a stock ticker.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        Dictionary containing various stock information and metrics
    """
    return yf.Ticker(ticker).info

@tool
def get_ticker_news(ticker: str):
    """Fetches recent news articles related to a stock ticker.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        List of news items with headlines, sources, and links
    """
    return yf.Ticker(ticker).get_news()


# Stock Analyst Agent - Tools and agent for analyst recommendations and forecasts
@tool
def get_analyst_price_targets(ticker: str):
    """Retrieves analyst price targets for a stock.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame with analyst price targets and recommendations
    """
    return yf.Ticker(ticker).get_analyst_price_targets()

@tool
def get_upgrades_downgrades(ticker: str):
    """Fetches recent stock upgrades and downgrades from analysts.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame with upgrade/downgrade information and dates
    """
    return yf.Ticker(ticker).get_upgrades_downgrades()

@tool
def get_recommendations_summary(ticker: str):
    """Gets a summary of analyst recommendations for a stock.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame with recommendation counts (buy, hold, sell)
    """
    return yf.Ticker(ticker).get_recommendations_summary()

@tool
def get_revenue_estimate(ticker: str):
    """Retrieves revenue estimates for a stock.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame with revenue forecasts and estimates
    """
    return yf.Ticker(ticker).get_revenue_estimate()

# Financial Statements Agent - Tools and agent for company financial statements
@tool
def get_balance_sheet(ticker: str):
    """Retrieves the balance sheet for a company.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame containing balance sheet data
    """
    return yf.Ticker(ticker).get_balance_sheet()

@tool
def get_cash_flow(ticker: str):
    """Retrieves the cash flow statement for a company.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame containing cash flow statement data
    """
    return yf.Ticker(ticker).get_cash_flow()

@tool
def get_income_stmt(ticker: str):
    """Retrieves the income statement for a company.
    
    Args:
        ticker: Stock symbol (e.g., 'AAPL', 'MSFT')
        
    Returns:
        DataFrame containing income statement data
    """
    return yf.Ticker(ticker).get_income_stmt()

@tool
def submit_workflow(workflow_definition: str):
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
            "payload": { "agent": "<agent_name">, "message": "<task delegated to agent>" },
            "retry_enabled": true,  // Optional: Enable/disable retries (default: true)
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
    print(
        json.dumps( json.loads(workflow_definition), indent=4 )
    )
    return orchestrator.submit_workflow(json.loads(workflow_definition), function_name="ExecuteAgentFunction")




