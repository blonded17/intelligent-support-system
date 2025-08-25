# Helper to serialize MongoDB results (ObjectId to str)
from bson import ObjectId

def serialize_mongo_result(result):
    if isinstance(result, list):
        return [serialize_mongo_result(doc) for doc in result]
    elif isinstance(result, dict):
        return {k: serialize_mongo_result(v) for k, v in result.items()}
    elif isinstance(result, ObjectId):
        return str(result)
    else:
        return result
# customer_query_handler.py
# Python version of the customer query handler using Ollama and LangChain


from langchain_community.chat_models import ChatOllama
from pymongo import MongoClient
import json
import pprint

ollama = ChatOllama(base_url="http://localhost:11434", model="phi3")
# Create MongoDB client at startup (read-only)
MONGO_CLIENT = MongoClient(
    "mongodb+srv://shubhambhatia2103:blonded17@cluster0.tdkyiq6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    # tlsCAFile="/Users/shubham/code/intelligent-support-system/atlas-ca.pem",
    tls=True,
    tlsAllowInvalidCertificates=True,   # only for dev
    serverSelectionTimeoutMS=5000,      # 5s to connect
    socketTimeoutMS=5000 
)
DB = MONGO_CLIENT["logs"]  # Replace with your DB name
COLLECTION = DB["device_logs"]  # Replace with your collection name

SCHEMA_FIELDS = [
    "OrganizationId", "DeviceId", "UserId", "TagId", "Timestamp", "Date", "Hour", "Month", "Year", "Index", "AppName", "LogLevel", "LogLabel", "LogSummary", "CreatedAt",
    "LogData.DeviceName", "LogData.DeviceType", "LogData.Model", "LogData.LoggedEvent", "LogData.MessageType", "LogData.State", "LogData.StateCode", "LogData.Tag", "LogData.Description", "LogData.Ward", "LogData.EpochCount", "LogData.ExecutionDuration", "LogData.Timezone", "LogData.RequestId",
    "LogData.TagDetail.AlertCode", "LogData.TagDetail.AlertLevel", "LogData.TagDetail.HeaderTime", "LogData.TagDetail.Key", "LogData.TagDetail.Message", "LogData.TagDetail.RequestId"
]



def analyze_query_with_llm(user_query):
    prompt = f"""
    You are a helpful assistant for a medical device log system.
    When extracting filters and fields from the user query, use ONLY the exact field names from this schema:
    {SCHEMA_FIELDS}
    Given the following user query, extract:
    - intent (lookup, report, list, count, aggregate, etc.)
    - filters (key-value pairs for database search, using exact schema field names)
    - fields (which fields to return, using exact schema field names)
    - aggregation (if the query requests an aggregate operation, specify the type, e.g., count, sum, avg, min, max and the field to aggregate on)
    User query: "{user_query}"
    Respond ONLY in valid JSON format, with NO comments, explanations, or pseudo field names. The response must be strictly parseable JSON, for example:
    Respond in JSON format as:
    {{
      "intent": "...",
      "filters": {{ ... }},
      "fields": [ ... ],
      :"aggregation": {{ "type": "count/sum/avg/min/max", "field": "..." }}
    }}
    If no aggregation is requested, set aggregration to null or omit it. Do not include any comments, explanations, or pseudo field name in the JSON.
    """
    try:
        response = ollama.invoke(prompt)
    except Exception as e:
        return {"error": f"LLM invocation failed: {str(e)}"}
    # Extract text from AIMessage
    response_text = response.content if hasattr(response, 'content') else str(response)
    try:
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        json_string = response_text[json_start:json_end]
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        return {"error": f"JSON parsing failed: {str(e)}", "raw": response_text}
    except Exception as e:
        return {"error": f"Unexpected error during JSON parsing: {str(e)}", "raw": response_text}

# RAG: Query MongoDB using LLM analysis (reuse client)
def rag_query_database(analysis):
    filters = analysis.get("filters", {})
    fields = analysis.get("fields", [])
    aggregation = analysis.get ("aggregation")
    # Handle aggregation if requested
    if aggregation and aggregation.get ("type") == "count":
        try:
            count = COLLECTION. count_documents(filters)
            return {"count": count}
        except Exception as e:
            return {"error": f"MongoDB count query failed: {str(e)}"}
    # You can add more aggregation types here (sum, avg, etc.)
    projection = {field: 1 for field in fields} if fields else None
    try:
        results = list(COLLECTION.find(filters, projection))
        return results
    except Exception as e:
        return {"error": f"MongoDB query failed: {str(e)}"}


# Main handler with hybrid logic

# Generate a final answer using LLM and retrieved database results (true RAG)
def generate_contextual_answer_with_llm(user_query, db_results, analysis=None):
    # Summarize results for prompt (limit to first 5 for brevity)
    if isinstance(db_results, list) and db_results:
        safe_results = serialize_mongo_result(db_results[:20])
        summary = json.dumps(safe_results, indent=2)
    elif isinstance(db_results, dict) and "error" in db_results:
        summary = db_results["error"]
    else:
        summary = "No results found."
    analysis_str = json.dumps(analysis, indent=2) if analysis else None
    prompt = f"""
    You are a helpful assistant for a medical device log system.
    The user asked: \"{user_query}\"
    Query analysis (intent, filters, fields, aggregation):
    {analysis_str}
    Here are the top relevant database results (JSON):
    {summary}
    Please generate a human-readable summary/description of the results, and also render the results as a markdown table (with appropriate headers and values). The table should be suitable for business users and downloadable as a report. If no results are found, say so. Your response should include both the description and the markdown table.
    """
    try:
        response = ollama.invoke(prompt)
        return response.content if hasattr(response, 'content') else str(response)
    except Exception as e:
        return f"LLM invocation failed: {str(e)}"

def handle_customer_query(user_query):
    analysis = analyze_query_with_llm(user_query)
    db_results = rag_query_database(analysis)
    final_answer = generate_contextual_answer_with_llm(user_query, db_results, analysis)
    return {
        "status": "analyzed",
        "analysis": analysis,
        "db_results": db_results,
        "final_answer": final_answer
    }

if __name__ == "__main__":
    user_query = input("Enter a customer query: ")
    result = handle_customer_query(user_query)
    print("\nLLM Query Analysis Output:")
    pprint.pprint(result)