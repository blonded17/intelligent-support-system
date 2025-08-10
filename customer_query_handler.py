# customer_query_handler.py
# Python version of the customer query handler using Ollama and LangChain


from langchain_community.chat_models import ChatOllama
from pymongo import MongoClient
import json
import pprint

ollama = ChatOllama(base_url="http://localhost:11434", model="phi3")

# Create MongoDB client at startup (read-only)
MONGO_CLIENT = MongoClient("mongodb+srv://shubhambhatia2103:blonded17@cluster0.tdkyiq6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&tls=true", readPreference='secondaryPreferred')
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
    - intent (lookup, report, list, etc.)
    - filters (key-value pairs for database search, using exact schema field names)
    - fields (which fields to return, using exact schema field names)
    User query: "{user_query}"
    Respond in JSON format as:
    {{
      "intent": "...",
      "filters": {{ ... }},
      "fields": [ ... ]
    }}
    """
    response = ollama.invoke(prompt)
    # Extract text from AIMessage
    response_text = response.content if hasattr(response, 'content') else str(response)
    try:
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        json_string = response_text[json_start:json_end]
        return json.loads(json_string)
    except Exception:
        return {"error": "Failed to parse LLM response", "raw": response_text}

# RAG: Query MongoDB using LLM analysis (reuse client)
def rag_query_database(analysis):
    filters = analysis.get("filters", {})
    fields = analysis.get("fields", [])
    projection = {field: 1 for field in fields} if fields else None
    results = list(COLLECTION.find(filters, projection))
    return results


# Main handler with hybrid logic
def handle_customer_query(user_query):
    analysis = analyze_query_with_llm(user_query)
    db_results = rag_query_database(analysis)
    return {"status": "analyzed", "analysis": analysis, "db_results": db_results}

if __name__ == "__main__":
    user_query = input("Enter a customer query: ")
    result = handle_customer_query(user_query)
    print("\nLLM Query Analysis Output:")
    pprint.pprint(result)