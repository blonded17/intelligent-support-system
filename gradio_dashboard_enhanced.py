# gradio_dashboard_enhanced.py
# Python Gradio UI for LLM query analysis with downloadable reports

import gradio as gr
import json
import pandas as pd
import io
import tempfile
import os
from bson import ObjectId
from datetime import datetime

# Import our enhanced customer query handler
from customer_query_handler_enhanced import handle_customer_query, serialize_mongo_result

def query_handler(query, download_format):
    make_downloadable = download_format != "None"
    result = handle_customer_query(query, make_downloadable)
    
    # Format the response for display
    output = {
        "User query": query,
        "LLM query analysis": result.get("analysis"),
        "MongoDB query executed": json.dumps(result.get("mongo_query", "No query available"), indent=2),
        "LLM query and DB result analysis": result.get("final_answer")
    }
    
    formatted_output = json.dumps(output, indent=2)
    
    print(f"Result structure: {list(result.keys())}")
    print(f"Download format selected: {download_format}")
    print(f"raw_data in result: {'raw_data' in result}")
    
    # If downloadable requested, prepare the download files
    if make_downloadable and download_format != "None" and "raw_data" in result:
        raw_data = result.get("raw_data")
        
        if download_format == "JSON":
            # Create JSON download
            json_str = json.dumps(raw_data, indent=2)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_results_{timestamp}.json"
            
            # Create a temporary file and return its path
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as f:
                f.write(json_str)
                temp_path = f.name
            
            return formatted_output, temp_path
        
        elif download_format == "CSV":
            # Convert data to pandas DataFrame for CSV export
            if isinstance(raw_data, list) and len(raw_data) > 0:
                # For lists of dictionaries, convert to dataframe directly
                df = pd.DataFrame(raw_data)
            elif isinstance(raw_data, dict):
                # For single dictionaries or count results, convert to a single row dataframe
                df = pd.DataFrame([raw_data])
            else:
                # Empty or other formats
                df = pd.DataFrame([{"result": "No data available"}])
            
            # Convert DataFrame to CSV and save to temp file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"query_results_{timestamp}.csv"
            
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".csv") as f:
                df.to_csv(f, index=False)
                temp_path = f.name
                
            return formatted_output, temp_path
        
        elif download_format == "HTML":
            # Generate HTML from the markdown response
            md_content = result.get("final_answer")
            
            # Create simple HTML content
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Medical Device Report</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 40px; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #f2f2f2; }}
                    h1 {{ color: #2c3e50; }}
                    h2 {{ color: #34495e; border-bottom: 1px solid #eee; padding-bottom: 5px; }}
                    .timestamp {{ color: #7f8c8d; font-size: 0.8em; }}
                </style>
            </head>
            <body>
                <h1>Medical Device Report</h1>
                <p class="timestamp">Generated on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                <h2>Query</h2>
                <p>{query}</p>
                <h2>Results</h2>
                <div>
                    {md_content.replace("\n", "<br>")}
                </div>
            </body>
            </html>
            """
            
            # Save to temp file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"medical_device_report_{timestamp}.html"
            
            import tempfile
            with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".html") as f:
                f.write(html_content)
                temp_path = f.name
                
            return formatted_output, temp_path
    
    # If no download requested, return just the formatted output
    return formatted_output, None  # None is acceptable for gr.File when no file should be returned

def serialize_mongo_result(result):
    if isinstance(result, list):
        return [serialize_mongo_result(doc) for doc in result]
    elif isinstance(result, dict):
        return {k: serialize_mongo_result(v) for k, v in result.items()}
    elif isinstance(result, ObjectId):
        return str(result)
    else:
        return result

# Create the Gradio interface
iface = gr.Interface(
    fn=query_handler,
    inputs=[
        gr.Textbox(label="Enter a customer query", lines=2, placeholder="Example: Show me all unique wards"),
        gr.Radio(
            ["None", "CSV", "JSON", "HTML"], 
            label="Download Format", 
            info="Select a format to download the results or 'None' for no download",
            value="None"
        )
    ],
    outputs=[
        gr.Textbox(label="Query Results"),
        gr.File(label="Download Results")
    ],
    title="Medical Device Log Analytics Dashboard",
    description="Enter a query about medical device logs and optionally download the results in your preferred format.",
    article="""
    ### How to use this dashboard
    1. Type your query in natural language (e.g., "Show me all unique wards", "Count logs from yesterday")
    2. Select a download format if you want to save the results
    3. Click Submit to run the query
    4. View the results and download them if needed
    
    The HTML format provides a professionally formatted report that you can open in any web browser and share with colleagues.
    """
)

if __name__ == "__main__":
    iface.launch()
