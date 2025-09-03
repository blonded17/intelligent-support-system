# Helper to serialize MongoDB results (ObjectId to str)
from bson import ObjectId
from llm_provider import LLMProvider
import json
import pprint
from datetime import datetime

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
    2. If the user asks for "unique" values (e.g., "unique alert codes", "distinct device types", "list all unique wards"), OR if the query is to list a single field like "LogData.Ward", ALWAYS set aggregation to: {{"type": "group_by", "group_by": <exact schema field>}}.
    Example: For "Give me unique alert codes", output:
    "fields": ["LogData.TagDetail.AlertCode"], "aggregation": {{"type": "group_by", "group_by": "LogData.TagDetail.AlertCode"}}
    Example: For "list all unique wards", output:
    "fields": ["LogData.Ward"], "aggregation": {{"type": "group_by", "group_by": "LogData.Ward"}}
    Example: For "list all wards", output:
    "fields": ["LogData.Ward"], "aggregation": {{"type": "group_by", "group_by": "LogData.Ward"}}
    Example: For "show wards", output:
    "fields": ["LogData.Ward"], "aggregation": {{"type": "group_by", "group_by": "LogData.Ward"}}
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
    5. For queries like "Which X had the highest number of Y?" or "Which day had the maximum number of warnings?", ALWAYS set aggregation to:
    "type": "count", "group_by": <group field>, "count_field": "*", "sort_order": "desc", "limit": 1
    Example: For "Which day in July 2025 had the maximum number of warnings?", output:
    "filters": {{"Month": "2025-07", "LogData.Tag": "warning"}}, "fields": ["Date"], "aggregation": {{"type": "count", "group_by": "Date", "count_field": "*", "sort_order": "desc", "limit": 1}}
    6. For queries asking for an average, sum, min, or max per group (e.g., "average execution duration per device type"), ALWAYS include a `group_by` field in the aggregation, set to the grouping field (e.g., "LogData.DeviceType").
    Example: For "Which device type had the highest average alert level?", output:
    "fields": ["LogData.DeviceType"], "aggregation": {{"type": "avg", "field": "LogData.TagDetail.AlertLevel", "group_by": "LogData.DeviceType", "sort_order": "desc", "limit": 1}}

    6a. For queries asking for the count of unique/distinct devices per group (e.g., "Show the count of unique devices used in each ward"), ALWAYS use:
    "type": "count_distinct", "group_by": <group field>, "distinct_field": <device field>, "sort_order": "desc"
    Example: For "Show the count of unique devices used in each ward", output:
    "fields": ["LogData.Ward"], "aggregation": {{"type": "count_distinct", "group_by": "LogData.Ward", "distinct_field": "LogData.DeviceName", "sort_order": "desc"}}
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
    # Defensive: If analysis is None or not a dict, return error early
    if not isinstance(analysis, dict):
        return {"error": "Analysis object is missing or malformed.", "query": "No query generated"}

    # Default initialization (prevents UnboundLocalError)
    agg_type = None
    group_by = None
    field = None
    sort_order = "desc"
    sort_direction = -1
    limit = 1

    # Map filters, fields, aggregation safely
    filters, fields, aggregation = map_fields(
        analysis.get("filters", {}),
        analysis.get("fields", []),
        analysis.get("aggregation")
    )

    if aggregation:
        agg_type = aggregation.get("type")
        group_by = aggregation.get("group_by")
        field = aggregation.get("field")
        sort_order = aggregation.get("sort_order", "desc")
        sort_direction = -1 if sort_order == "desc" else 1
        limit = aggregation.get("limit", 1)

    print(f"[DEBUG] Aggregation type: {agg_type}, group_by: {group_by}, field: {field}, sort_order: {sort_order}, limit: {limit}")

    # Now safe to check agg_type
    if agg_type == "distinct" and field:
        pipeline = []
        if filters:
            pipeline.append({"$match": filters})
        pipeline.extend([
            {"$group": {"_id": f"${field}"}},
            {"$project": {field: "$_id", "_id": 0}}
        ])
        print(f"[DEBUG] Executing DISTINCT aggregation pipeline: {pipeline}")
        try:
            result = list(COLLECTION.aggregate(pipeline))
            print(f"[DEBUG] DISTINCT aggregation result: {result}")
            # Include the MongoDB query in the response
            return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
        except Exception as e:
            print(f"[DEBUG] DISTINCT aggregation error: {e}")
            return {"error": f"MongoDB distinct aggregation failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}

    if agg_type == "count_distinct" and group_by and aggregation.get("distinct_field"):
        distinct_field = aggregation.get("distinct_field")
        pipeline = []
        if filters:
            pipeline.append({"$match": filters})
        pipeline.extend([
            {"$group": {"_id": f"${group_by}", "uniqueDevices": {"$addToSet": f"${distinct_field}"}}},
            {"$project": {"ward": "$_id", "deviceCount": {"$size": "$uniqueDevices"}}},
            {"$sort": {"deviceCount": sort_direction}}
        ])
        print(f"[DEBUG] Executing COUNT_DISTINCT aggregation pipeline: {pipeline}")
        try:
            result = list(COLLECTION.aggregate(pipeline))
            print(f"[DEBUG] COUNT_DISTINCT aggregation result: {result}")
            # Include the MongoDB query in the response
            return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
        except Exception as e:
            print(f"[DEBUG] COUNT_DISTINCT aggregation error: {e}")
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}

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
            # Include the MongoDB query in the response
            return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
        except Exception as e:
            print(f"[DEBUG] COUNT aggregation error: {e}")
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}

    if agg_type == "avg" and group_by and field:
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
            # Include the MongoDB query in the response
            return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
        except Exception as e:
            print(f"[DEBUG] AVG aggregation error: {e}")
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}

    if agg_type == "count":
        print(f"[DEBUG] Executing COUNT (no group_by) query with filters: {filters}")
        try:
            count = COLLECTION.count_documents(filters)
            print(f"[DEBUG] COUNT (no group_by) result: {count}")
            # Include the MongoDB query in the response
            return {"count": count, "query": {"filters": filters, "type": "count_documents"}}
        except Exception as e:
            print(f"[DEBUG] COUNT (no group_by) error: {e}")
            return {"error": f"MongoDB count query failed: {str(e)}", "query": {"filters": filters, "type": "count_documents"}}

    # Only run find if no aggregation type matched
    projection = {field: 1 for field in fields} if fields else None
    print(f"[DEBUG] Executing FIND query with filters: {filters}, projection: {projection}")
    try:
        results = list(COLLECTION.find(filters, projection))
        # Include the MongoDB query in the response
        return {"data": results, "query": {"filters": filters, "projection": projection, "type": "find"}}
    except Exception as e:
        print(f"[DEBUG] FIND query error: {e}")
        return {"error": f"MongoDB query failed: {str(e)}"}


def generate_contextual_answer_with_llm(user_query, db_results, analysis=None, make_downloadable=False):
    # Store raw results for downloadable report
    raw_results = serialize_mongo_result(db_results) if db_results else None
    
    # If db_results is a list and not empty, create enhanced presentation
    if isinstance(db_results, list) and db_results:
        safe_results = serialize_mongo_result(db_results[:20])
        
        # Handle various result formats for better presentation
        if len(safe_results) > 15:
            # For large result sets, show summary stats and sample
            total_count = len(db_results)
            sample_size = min(10, total_count)
            summary_intro = f"Found {total_count} results. Showing top {sample_size} as a sample:\n\n"
            safe_results = safe_results[:sample_size]
        else:
            summary_intro = ""
        
        # Check if results are aggregated (like count by group)
        if len(safe_results) > 0 and len(safe_results[0].keys()) <= 3 and any(k in str(safe_results[0].keys()) for k in ['count', 'average', 'sum', '_id']):
            # Format aggregation results nicely
            headers = safe_results[0].keys()
            table = summary_intro + "| " + " | ".join(headers) + " |\n"
            table += "|" + "|".join(["---"] * len(headers)) + "|\n"
            for row in safe_results:
                table += "| " + " | ".join(str(row[h]) for h in headers) + " |\n"
            summary = table
        else:
            # Regular table with all fields
            headers = safe_results[0].keys() if safe_results else []
            table = summary_intro + "| " + " | ".join(headers) + " |\n"
            table += "|" + "|".join(["---"] * len(headers)) + "|\n"
            for row in safe_results:
                table += "| " + " | ".join(str(row[h]) for h in headers) + " |\n"
            summary = table
    elif isinstance(db_results, dict) and "count" in db_results:
        # Special handling for count queries
        summary = f"Total count: {db_results['count']}"
    elif isinstance(db_results, dict) and "error" in db_results:
        summary = f"Error: {db_results['error']}"
    else:
        summary = "No results found."

    analysis_str = json.dumps(analysis, indent=2) if analysis else None
    
    # Use different prompts for regular responses vs downloadable reports
    if make_downloadable:
        prompt = f"""
        You are a professional report generator for a medical device log system.
        
        # Medical Device Log Analytics Report
        
        ## Query Information
        User Query: "{user_query}"
        Date Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        
        Please format your response as a concise report with the following sections:
        
        1. ## Executive Summary
           - Provide a concise summary of what was asked and the key findings
        
        2. ## Results
           - Present the data in a clean markdown table
           - For large datasets, summarize key patterns or trends
        
        3. ## Insights
           - Provide 2-3 key analytical insights derived from the data
        
        If the query produced no results, clearly state this in the Executive Summary.
        
        Query analysis details (for reference only):
        {analysis_str}

        Database results:
        {summary}
        """
    else:
        prompt = f"""
        You are a helpful assistant for a medical device log system.

        The user asked: "{user_query}"

        Here's how you should respond:
        1. Start by rephrasing the user's question in plain language to confirm understanding.
        2. Present the direct answer clearly (e.g., the least common alert code, most frequent ward, etc.).
        3. Then, show the supporting database results in a clean markdown table with appropriate headers.
        4. If the query produced no results, clearly say: "No results found for this query."
        5. Add a short human-style explanation that connects the results back to the user's original question, like ChatGPT would when reasoning things out.

        Query analysis (intent, filters, fields, aggregation):
        {analysis_str}

        Database results (JSON):
        {summary}
        """

    try:
        response = llm_provider.invoke(prompt)
        llm_response = response.content if hasattr(response, 'content') else str(response)
        
        # For downloadable responses, include both formatted text and raw data
        if make_downloadable:
            return {
                "formatted_response": llm_response,
                "raw_data": raw_results,
                "is_downloadable": True,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "query": user_query
            }
        else:
            return llm_response
    except Exception as e:
        return f"LLM invocation failed: {str(e)}"


def handle_customer_query(user_query, make_downloadable=False):
    import re
    from datetime import datetime, timedelta

    def preprocess_time_phrases(query):
        now = datetime.now()
        today = now.date()
        # Handle 'yesterday'
        if re.search(r"\byesterday\b", query, re.IGNORECASE):
            yest = today - timedelta(days=1)
            start = datetime.combine(yest, datetime.min.time()).isoformat()
            end = datetime.combine(yest, datetime.max.time()).isoformat()
            query = re.sub(r"\byesterday\b", f"Timestamp between '{start}' and '{end}'", query, flags=re.IGNORECASE)
        # Handle 'last week' or 'last one week'
        if re.search(r"\blast (one )?week\b", query, re.IGNORECASE):
            last_week_start = today - timedelta(days=today.weekday() + 7)
            last_week_end = last_week_start + timedelta(days=6)
            start = datetime.combine(last_week_start, datetime.min.time()).isoformat()
            end = datetime.combine(last_week_end, datetime.max.time()).isoformat()
            query = re.sub(r"\blast (one )?week\b", f"Timestamp between '{start}' and '{end}'", query, flags=re.IGNORECASE)
        # Handle 'last 7 days'
        if re.search(r"\blast 7 days\b", query, re.IGNORECASE):
            start = datetime.combine(today - timedelta(days=7), datetime.min.time()).isoformat()
            end = datetime.combine(today, datetime.max.time()).isoformat()
            query = re.sub(r"\blast 7 days\b", f"Timestamp between '{start}' and '{end}'", query, flags=re.IGNORECASE)
        return query

    processed_query = preprocess_time_phrases(user_query)
    analysis = analyze_query_with_llm(processed_query)
    db_result_obj = rag_query_database(analysis)
    
    # Extract the actual data and MongoDB query
    mongo_query = db_result_obj.get("query", "No query information available")
    
    # Extract the data based on the structure returned
    if "data" in db_result_obj:
        db_results = db_result_obj["data"]
    elif "count" in db_result_obj:
        db_results = db_result_obj  # Keep the whole object for count queries
    elif "error" in db_result_obj:
        db_results = db_result_obj  # Keep the whole error object
    else:
        db_results = db_result_obj  # Fallback
    
    final_answer = generate_contextual_answer_with_llm(processed_query, db_results, analysis, make_downloadable)
    
    response = {
        "status": "analyzed",
        "analysis": analysis,
        "db_results": db_results,
        "mongo_query": mongo_query
    }
    
    # Add debug prints
    print(f"make_downloadable: {make_downloadable}")
    print(f"final_answer type: {type(final_answer)}")
    
    if make_downloadable and isinstance(final_answer, dict):
        print("Processing as downloadable")
        response["final_answer"] = final_answer["formatted_response"]
        response["raw_data"] = final_answer["raw_data"] if "raw_data" in final_answer else serialize_mongo_result(db_results)
        response["is_downloadable"] = True
        response["timestamp"] = final_answer.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        response["final_answer"] = final_answer
        # Even for non-downloadable format, include raw_data for debugging
        response["raw_data"] = serialize_mongo_result(db_results)
        
    return response


if __name__ == "__main__":
    user_query = input("Enter a customer query: ")
    result = handle_customer_query(user_query)
    print("\nLLM Query Analysis Output:")
    pprint.pprint(result)
