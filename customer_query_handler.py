# Helper to serialize MongoDB results (ObjectId to str)
from bson import ObjectId
from llm_provider import LLMProvider

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
from langchain_community.chat_models import ChatOllama
from pymongo import MongoClient
import json
import pprint

llm_provider = LLMProvider(provider="ollama", model="mistral:7b", base_url="http://localhost:11434")

# Create MongoDB client at startup (read-only)
MONGO_CLIENT = MongoClient(
    "mongodb+srv://shubhambhatia2103:blonded17@cluster0.tdkyiq6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0",
    tls=True,
    tlsAllowInvalidCertificates=True,   # only for dev
    serverSelectionTimeoutMS=5000,
    socketTimeoutMS=5000 
)
DB = MONGO_CLIENT["logs"]
COLLECTION = DB["device_logs"]

# Canonical schema fields
SCHEMA_FIELDS = [
    "OrganizationId", "DeviceId", "UserId", "TagId", "Timestamp", "Date", "Hour", "Month", "Year", "Index", "AppName", "LogLevel", "LogLabel", "LogSummary", "CreatedAt",
    "LogData.DeviceName", "LogData.DeviceType", "LogData.Model", "LogData.LoggedEvent", "LogData.MessageType", "LogData.State", "LogData.StateCode", "LogData.Tag", "LogData.Description", "LogData.Ward", "LogData.EpochCount", "LogData.ExecutionDuration", "LogData.Timezone", "LogData.RequestId",
    "LogData.TagDetail.AlertCode", "LogData.TagDetail.AlertLevel", "LogData.TagDetail.HeaderTime", "LogData.TagDetail.Key", "LogData.TagDetail.Message", "LogData.TagDetail.RequestId"
]

# Flat-to-nested field map
FIELD_MAP = {
    "OrganizationId": "OrganizationId",
    "DeviceId": "DeviceId",
    "UserId": "UserId",
    "TagId": "TagId",
    "Timestamp": "Timestamp",
    "Date": "Date",
    "Hour": "Hour",
    "Month": "Month",
    "Year": "Year",
    "Index": "Index",
    "AppName": "AppName",
    "LogLevel": "LogLevel",
    "LogLabel": "LogLabel",
    "LogSummary": "LogSummary",
    "CreatedAt": "CreatedAt",
    "DeviceName": "LogData.DeviceName",
    "DeviceType": "LogData.DeviceType",
    "Model": "LogData.Model",
    "LoggedEvent": "LogData.LoggedEvent",
    "MessageType": "LogData.MessageType",
    "State": "LogData.State",
    "StateCode": "LogData.StateCode",
    "Tag": "LogData.Tag",
    "Description": "LogData.Description",
    "Ward": "LogData.Ward",
    "EpochCount": "LogData.EpochCount",
    "ExecutionDuration": "LogData.ExecutionDuration",
    "Timezone": "LogData.Timezone",
    "RequestId": "LogData.RequestId",
    "AlertCode": "LogData.TagDetail.AlertCode",
    "AlertLevel": "LogData.TagDetail.AlertLevel",
    "HeaderTime": "LogData.TagDetail.HeaderTime",
    "Key": "LogData.TagDetail.Key",
    "Message": "LogData.TagDetail.Message",
    "TagDetail.RequestId": "LogData.TagDetail.RequestId"
}

def map_fields(filters, fields, aggregation):
    """Map LLM-extracted fields to canonical schema fields."""
    mapped_filters = {FIELD_MAP.get(k, k): v for k, v in filters.items()}
    mapped_fields = [FIELD_MAP.get(f, f) for f in fields] if fields else []
    mapped_aggregation = None
    if aggregation:
        mapped_aggregation = {"type": aggregation.get("type")}
        # Map 'field' if present
        if "field" in aggregation:
            mapped_aggregation["field"] = FIELD_MAP.get(aggregation.get("field"), aggregation.get("field"))
        # Map 'group_by' if present
        if "group_by" in aggregation:
            mapped_aggregation["group_by"] = FIELD_MAP.get(aggregation.get("group_by"), aggregation.get("group_by"))
        # Map 'count_field' if present
        if "count_field" in aggregation:
            mapped_aggregation["count_field"] = FIELD_MAP.get(aggregation.get("count_field"), aggregation.get("count_field")) if aggregation.get("count_field") != "*" else "*"
        # Map 'sort_order' if present
        if "sort_order" in aggregation:
            mapped_aggregation["sort_order"] = aggregation.get("sort_order")
        # Map 'limit' if present
        if "limit" in aggregation:
            mapped_aggregation["limit"] = aggregation.get("limit")
    return mapped_filters, mapped_fields, mapped_aggregation


def analyze_query_with_llm(user_query):
    prompt = f"""
    You are a helpful assistant for a medical device log system.
    Your task is to translate natural language questions into valid MongoDB aggregation pipeline instructions.
    You must follow the schema exactly and output structured JSON.
    When extracting filters and fields from the user query, use ONLY the exact field names from this schema:
    {SCHEMA_FIELDS}

    IMPORTANT RULES (follow strictly):
    1. For any range query (date, time, numeric, etc.), ALWAYS extract filters as separate fields using MongoDB-compatible operators (e.g., $gte, $lte, $gt, $lt). NEVER use combined string conditions (e.g., ">=... AND ...<...").
    Example: For "How many logs per day in June 2025?", output:
    "filters": {{"Timestamp": {{"$gte": "2025-06-01T00:00:00", "$lte": "2025-06-30T23:59:59"}}}}, "fields": ["Date"], "aggregation": {{"type": "count", "group_by": "Date"}}
    Example: For "Show logs with ExecutionDuration between 10 and 100", output:
    "filters": {{"LogData.ExecutionDuration": {{"$gte": 10, "$lte": 100}}}}, "fields": ["LogData.ExecutionDuration"]
    Example: For "Show logs with Timestamp >= '2025-06-01T00:00:00Z' AND Timestamp < '2026-06-01T00:00:00Z'", output:
    "filters": {{"Timestamp": {{"$gte": "2025-06-01T00:00:00Z", "$lt": "2026-06-01T00:00:00Z"}}}}
    1a. NEVER use combined string conditions in filters. ALWAYS use MongoDB operator format for all range filters.
    2. If the user asks for "unique" values (e.g., "unique alert codes", "distinct device types"), set aggregation to: {{"type": "distinct", "field": <exact schema field>}} OR {{"type": "count", "group_by": <exact schema field>}}.
    Example: For "Give me unique alert codes", output:
    "fields": ["LogData.TagDetail.AlertCode"], "aggregation": {{"type": "distinct", "field": "LogData.TagDetail.AlertCode"}}
    Alternatively, you may use: "aggregation": {{"type": "count", "group_by": "LogData.TagDetail.AlertCode"}}
    3. If the user asks for the "most common", "most frequent", "top", "least common", "least frequent", or "bottom" value of a field, you MUST:
    For "most common", "most frequent", or "top":
    - Set aggregation to: {{"type": "count", "group_by": <exact schema field>, "count_field": "*", "sort_order": "desc", "limit": 1}}
    - Set fields to: [<exact schema field>]
    - DO NOT use "max", "highest value", or similar aggregation types for these queries.
    For "least common", "least frequent", or "bottom":
    - Set aggregation to: {{"type": "count", "group_by": <exact schema field>, "count_field": "*", "sort_order": "asc", "limit": 1}}
    - Set fields to: [<exact schema field>]
    - DO NOT use "min", "lowest value", or similar aggregation types for these queries.
    4. If the user asks for the "highest value" (e.g., "What is the highest alert code?"), use aggregation type "max" for that field.
    5. For queries like "Which X had the highest number of Y?", set fields to only the group_by field, and aggregation to include type, group_by, and count_field ("*").
    6. For queries asking for an average, sum, min, or max per group (e.g., "average execution duration per device type"), ALWAYS include a `group_by` field in the aggregation, set to the grouping field (e.g., "LogData.DeviceType").
    Example: For "Which device type had the highest average alert level?", output:
    "fields": ["LogData.DeviceType"], "aggregation": {{"type": "avg", "field": "LogData.TagDetail.AlertLevel", "group_by": "LogData.DeviceType", "sort_order": "desc", "limit": 1}}
    7. If user specifies fields, return only those fields.
    8. If user does not specify fields and intent is "list", return ALL fields.
    9. If user asks for "count", set aggregation = "count" and fields = [].
    10. Always use exact schema names (e.g., LogData.Ward, not Ward).
    11. Respond ONLY in valid JSON, no text, no comments.

    Given the following user query, extract:
    - intent (lookup, report, list, count, aggregate, etc.)
    - filters (key-value pairs for database search, using exact schema field names and MongoDB operators)
    - fields (which fields to return, using exact schema field names)
    - aggregation (if the query requests an aggregate operation, specify the type, e.g., count, sum, avg, min, max and the field to aggregate on)
    User query: "{user_query}"
    Respond ONLY in valid JSON format, with NO comments, explanations, or pseudo field names. The response must be strictly parseable JSON, for example:
    {{
        "intent": "...",
        "filters": {{ ... }},
        "fields": [ ... ],
        "aggregation": {{ "type": "count/sum/avg/min/max", "field": "..." }}
    }}
    If no aggregation is requested, set aggregation to null or omit it.
    """
    try:
        response = llm_provider.invoke(prompt)
    except Exception as e:
        return {"error": f"LLM invocation failed: {str(e)}"}

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


def rag_query_database(analysis):
    filters, fields, aggregation = map_fields(
        analysis.get("filters", {}),
        analysis.get("fields", []),
        analysis.get("aggregation")
    )

    agg_type = aggregation.get("type")
    group_by = aggregation.get("group_by")
    field = aggregation.get("field")
    sort_order = aggregation.get("sort_order", "desc")
    sort_direction = -1 if sort_order == "desc" else 1
    limit = aggregation.get("limit", 1)

    print(f"[DEBUG] Aggregation type: {agg_type}, group_by: {group_by}, field: {field}, sort_order: {sort_order}, limit: {limit}")

    if agg_type == "count" and group_by:
        pipeline = []
        if filters:
            pipeline.append({"$match": filters})
        pipeline.extend([
            {"$group": {"_id": f"${group_by}", "count": {"$sum": 1}}},
            {"$sort": {"count": sort_direction}},
            {"$limit": limit}
        ])
        print(f"[DEBUG] Executing COUNT aggregation pipeline: {pipeline}")
        try:
            result = list(COLLECTION.aggregate(pipeline))
            print(f"[DEBUG] COUNT aggregation result: {result}")
            return result
        except Exception as e:
            print(f"[DEBUG] COUNT aggregation error: {e}")
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}"}
    elif agg_type == "avg" and group_by and field:
        pipeline = []
        if filters:
            pipeline.append({"$match": filters})
        pipeline.extend([
            {"$group": {"_id": f"${group_by}", "average": {"$avg": f"${field}"}}},
            {"$sort": {"average": sort_direction}},
            {"$limit": limit}
        ])
        print(f"[DEBUG] Executing AVG aggregation pipeline: {pipeline}")
        try:
            result = list(COLLECTION.aggregate(pipeline))
            print(f"[DEBUG] AVG aggregation result: {result}")
            return result
        except Exception as e:
            print(f"[DEBUG] AVG aggregation error: {e}")
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}"}
    elif agg_type == "count":
        print(f"[DEBUG] Executing COUNT (no group_by) query with filters: {filters}")
        try:
            count = COLLECTION.count_documents(filters)
            print(f"[DEBUG] COUNT (no group_by) result: {count}")
            return {"count": count}
        except Exception as e:
            print(f"[DEBUG] COUNT (no group_by) error: {e}")
            return {"error": f"MongoDB count query failed: {str(e)}"}
    else:
        # Only run find if no aggregation type matched
        projection = {field: 1 for field in fields} if fields else None
        print(f"[DEBUG] Executing FIND query with filters: {filters}, projection: {projection}")
        try:
            results = list(COLLECTION.find(filters, projection))
            print(f"[DEBUG] FIND query result: {results}")
            return results
        except Exception as e:
            print(f"[DEBUG] FIND query error: {e}")
            return {"error": f"MongoDB query failed: {str(e)}"}


def generate_contextual_answer_with_llm(user_query, db_results, analysis=None):
    # If db_results is a list and not empty, show as table
    if isinstance(db_results, list) and db_results:
        safe_results = serialize_mongo_result(db_results[:20])
        # Render as markdown table
        headers = safe_results[0].keys() if safe_results else []
        table = "| " + " | ".join(headers) + " |\n"
        table += "|" + "|".join(["---"] * len(headers)) + "|\n"
        for row in safe_results:
            table += "| " + " | ".join(str(row[h]) for h in headers) + " |\n"
        summary = table
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
    Please render the results as a markdown table (with appropriate headers and values). 
    If no results are found, say so. 
    """
    try:
        response = llm_provider.invoke(prompt)
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
