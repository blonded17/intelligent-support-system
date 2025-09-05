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
COLLECTION = DB["Alerts"]

# Canonical schema fields for Alerts collection
SCHEMA_FIELDS = [
    "OrganizationId", "DeviceId", "UserId", "Timestamp", "TimestampTz", 
    "Ward", "ParameterKey", "ParameterValue", "LowerThreshold", "UpperThreshold", 
    "Bound", "SmartAlertId", "Source", "Stage", "Status",
    "_id.UserId", "_id.Timestamp", "_id.Source"
]

# Flat-to-nested field map for Alerts schema
FIELD_MAP = {
    "OrganizationId": "OrganizationId",
    "DeviceId": "DeviceId",
    "UserId": "UserId",
    "Timestamp": "Timestamp",
    "TimestampTz": "TimestampTz",
    "Ward": "Ward",
    "ParameterKey": "ParameterKey",
    "ParameterValue": "ParameterValue",
    "LowerThreshold": "LowerThreshold",
    "UpperThreshold": "UpperThreshold",
    "Bound": "Bound",
    "SmartAlertId": "SmartAlertId",
    "Source": "Source",
    "Stage": "Stage",
    "Status": "Status",
    # Nested _id fields
    "IdUserId": "_id.UserId",
    "IdTimestamp": "_id.Timestamp",
    "IdSource": "_id.Source"
}

def map_fields(filters, fields, aggregation):
    """Map LLM-extracted fields to canonical schema fields."""
    # Defensive: handle filters=None or not a dict
    if not isinstance(filters, dict):
        filters = {}
    mapped_filters = {FIELD_MAP.get(k, k): v for k, v in filters.items()}
    # Defensive: handle fields as list of strings or list of dicts
    mapped_fields = []
    if fields:
        for f in fields:
            if isinstance(f, str):
                mapped_fields.append(FIELD_MAP.get(f, f))
            elif isinstance(f, dict):
                # If dict, try to extract field name from known keys (e.g., $dateToString)
                # Use the key as the field name, or fallback to str(f)
                key = next(iter(f.keys()), None)
                if key:
                    mapped_fields.append(key)
                else:
                    mapped_fields.append(str(f))
            else:
                mapped_fields.append(str(f))
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
    You are an expert translator that converts natural-language questions into a strict, machine-readable JSON analysis used to build MongoDB aggregation pipelines for a patient-alerts dataset.
    Respond only with one JSON object (no text, no comments, no explanation). Follow these rules exactly.
    
    SCHEMA (use exact field names)
    - Strings: OrganizationId, DeviceId, UserId, Ward, Status, ParameterKey, SmartAlertId, Timestamp, TimestampTz, Bound
    - Integers: LowerThreshold, UpperThreshold, Stage, Source
    - Floating-point: ParameterValue
    - ObjectId: _id (may contain nested fields: UserId, Timestamp, Source)
    - Ward is a string (do not cast to number unless numeric logic is explicitly required)
    - Use OrganizationId for all organization-based filters. Never use _id.Source or Source when mapping organizations.
    
    CORE RULES:
    1. Always use exact schema field names. No aliases or pseudo-fields. Use exact paths for nested fields if applicable.

    2. Organization Scope:
        - If query is organization-scoped, always include "OrganizationId" in filters.
        - If missing, add it to "missingFilters".
        - Never use "_id.Source" or "Source" as an organization filter.

    3. Intent Mapping:
        - Listing unique values → "intent": "aggregate" + "type": "distinct" or "count_distinct"
        - Total counts → "intent": "aggregate" + "type": "count"
        - Numeric metrics → "intent": "aggregate" + "type": "avg|min|max|sum"
        - Trends over time → "intent": "aggregate" + "type": "count" + group_by "Timestamp" (with $dateToString)
        - Top/bottom entities → "intent": "aggregate" + "type": "count" + "sort_order" + "limit"
        - Multi-metric queries → "intent": "aggregate" + "type": "facet"

    4. Grouping Rules:
        - By "UserId" → patient-level counts or trends
        - By "Ward" → ward-level distributions
        - By "Status" → status-wise counts
        - By "Timestamp" → trends over time
        - Combined groupings → include all relevant filters (e.g., patient in ward: filters: OrganizationId + Ward, group_by UserId)

    5. Aggregation Rules:
        - "count" → $sum:1
        - "count_distinct" → count unique values
        - "distinct" → list unique values
        - Numeric metrics → "avg|min|max|sum"
        - Multi-metrics → use "facet" object; shared filters come first

    6. Time Handling:
        - Always parse dates if query involves time:
            - ISO → "$toDate": "$Timestamp"
            - Non-ISO → "$dateFromString" with explicit format
        - Filters:
            - Absolute → {{ "Timestamp": {{ "$gte": "<ISO>", "$lt": "<ISO>" }} }}
            - Relative → {{ "timeRange": {{ "relative": "<last_30_days>" }} }}
        - Trends → use $dateToString with format based on granularity (day/month/year)

    7. Limits and Sorting:
        - Include "limit" only if query explicitly asks top/N, most/least, highest/bottom
        - Default sorting:
            - Desc → top, most, highest
            - Asc → least, bottom

    8. Status Handling:
        - For alerts by status:
            - group_by "Status", type "count"
            - Only add $in filter if user explicitly lists statuses
            - Default includes all statuses

    9. Parameter Queries:
        - Metrics on alert parameters (ParameterValue, Thresholds):
            - Filter by OrganizationId + ParameterKey
            - Aggregation type: avg|min|max|sum
            - If missing, list in missingFilters

    10. Facets / Multi-Metric Queries:
        - Use "facet" object
        - Each facet is a valid aggregation object
        - Shared filters first

    11. Output Rules:
        - JSON only
        - Include:
            - "intent"
            - "filters"
            - "fields"
            - "aggregation" (null if not aggregate)
            - "missingFilters"
        - No explanatory text or pseudo-fields

    12. Pattern Recognition:
        - Queries recognized by patterns:
            1. List/count patients → group_by UserId
            2. Status-wise count → group_by Status
            3. Ward-wise metrics → group_by Ward
            4. Patient in ward → filters: Ward + OrganizationId, group_by UserId
            5. Time trends → group_by Timestamp
            6. Parameter metrics → ParameterKey + numeric aggregation
            7. Top/bottom → group_by + sort_order + limit
            8. Multi-metric → facet with each aggregation

    13. Default Behavior:
        - Match query → detect pattern → apply filters → apply group_by → select aggregation type → sort/limit if required → return fields + missingFilters

   Example Queries and JSON Intents:
    1. Query: Which ward has the most alerts in OrgX?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX" }}, "fields": ["Ward"], "aggregation": {{ "type": "count", "group_by": "Ward", "sort_order": "desc", "limit": 1 }} }}
    2. Query: Show me ward-wise distribution of alerts in OrgZ
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgZ" }}, "fields": ["Ward"], "aggregation": {{ "type": "count", "group_by": "Ward" }} }}
    3. Query: Which patient triggered the most alerts in OrgY last month?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgY", "Timestamp": {{ "$gte": "2024-08-01", "$lte": "2024-08-31" }} }}, "fields": ["UserId"], "aggregation": {{ "type": "count", "group_by": "UserId", "sort_order": "desc", "limit": 1 }} }}
    4. Query: How many alerts were generated per patient in Pediatric Ward in OrgX?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX", "Ward": "Pediatric Ward" }}, "fields": ["UserId"], "aggregation": {{ "type": "count", "group_by": "UserId", "sort_order": "desc" }} }}
    5. Query: Trend of alerts by day for OrgX
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX" }}, "fields": ["Timestamp"], "aggregation": {{ "type": "count", "group_by": "Timestamp" }} }}
    6. Query: How many unique devices generated alerts in OrgX?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX" }}, "fields": ["DeviceId"], "aggregation": {{ "type": "count_distinct", "field": "DeviceId" }} }}
    7. Query: Show me the average ParameterValue of HR in OrgY
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgY", "ParameterKey": "HR" }}, "fields": ["ParameterValue"], "aggregation": {{ "type": "avg", "field": "ParameterValue" }} }}
    8. Query: What is the minimum ParameterValue recorded for BP in OrgX?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX", "ParameterKey": "BP" }}, "fields": ["ParameterValue"], "aggregation": {{ "type": "min", "field": "ParameterValue" }} }}
    9. Query: What is the maximum ParameterValue recorded for SPO2 in OrgY?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgY", "ParameterKey": "SPO2" }}, "fields": ["ParameterValue"], "aggregation": {{ "type": "max", "field": "ParameterValue" }} }}
    10. Query: What is the minimum and maximum ParameterValue recorded for HR in OrgZ?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgZ", "ParameterKey": "HR" }}, "fields": ["ParameterValue"], "aggregation": {{ "type": ["min", "max"], "field": "ParameterValue" }} }}
    11. Query: Which ward generated the most and least alerts in OrgX?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX" }}, "fields": ["Ward"], "aggregation": {{ "type": "facet", "facets": {{ "most": {{ "type": "count", "group_by": "Ward", "sort_order": "desc", "limit": 1 }}, "least": {{ "type": "count", "group_by": "Ward", "sort_order": "asc", "limit": 1 }} }} }} }}
    12. Query: Show me both the daily alert trend and the top ward in OrgY
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgY" }}, "aggregation": {{ "type": "facet", "add_fields": {{ "parsedTimestamp": {{ "$toDate": "$Timestamp" }} }}, "facets": {{ "dailyAlertTrend": [ {{ "type": "count", "group_by": {{ "$dateToString": {{ "format": "%Y-%m-%d", "date": "$parsedTimestamp" }} }} }} ], "topWard": {{ "type": "count", "group_by": "Ward", "sort_order": "desc", "limit": 1 }} }} }} }}
    13. Query: Give me the count of alerts grouped by ParameterKey in OrgZ
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgZ" }}, "fields": ["ParameterKey"], "aggregation": {{ "type": "count", "group_by": "ParameterKey" }} }}
    14.Query: Which Source generated the highest number of alerts in OrgX?
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX" }}, "fields": ["Source"], "aggregation": {{ "type": "count", "group_by": "Source", "sort_order": "desc", "limit": 1 }} }}
    15. Query: Give me monthly trend of alerts in OrgY between Jan 2024 and Jun 2024
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgY", "Timestamp": {{ "$gte": "2024-01-01", "$lte": "2024-06-30" }} }}, "aggregation": {{ "type": "count", "group_by": {{ "$dateToString": {{ "format": "%Y-%m", "date": "$Timestamp" }} }} }} }}
    16. Query: List unique patients with their total number of alerts in OrgX
    JSON intent: {{ "intent": "aggregate", "filters": {{ "OrganizationId": "OrgX" }}, "fields": ["UserId"], "aggregation": {{ "type": "count", "group_by": "UserId", "sort_order": "desc" }} }}


    Given the following user query, extract and return a single JSON object with these fields:
    - intent: one of [lookup, report, list, count, aggregate]
    - filters: key-value pairs using only exact schema field names and valid MongoDB operators ($gte, $lte, $gt, $lt, etc.)
    - fields: array of schema field names to return
    - aggregation: object specifying the type (count, sum, avg, min, max, count_distinct, facet), the field(s) to aggregate on, and group_by if applicable
User query: "{user_query}"

    Response requirements:
    - Output must be a single valid JSON object.
    - No comments, no explanations, no extra text.
    - Must be strictly parseable by json.loads().
    
    Example format:
    {{
        "intent": "aggregate",
        "filters": {{ "OrganizationId": "OrgX" }},
        "fields": ["Ward"],
        "aggregation": {{ "type": "count", "group_by": "Ward", "sort_order": "desc", "limit": 1 }}
    }}

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
        import re
        # Remove C-style comments (/* ... */)
        json_string = re.sub(r'/\*.*?\*/', '', json_string, flags=re.DOTALL)
        # Remove single-line comments (// ...)
        json_string = re.sub(r'//.*?(\n|$)', '', json_string)
        return json.loads(json_string)
    except json.JSONDecodeError as e:
        print(f"[DEBUG] Failed to parse JSON: {json_string}")
        return {"error": f"JSON parsing failed: {str(e)}", "raw": response_text}
    except Exception as e:
        return {"error": f"Unexpected error during JSON parsing: {str(e)}", "raw": response_text}



def process_nested_id_fields(filters):
    """
    Process filters that target the nested _id structure in the Alerts collection.
    MongoDB requires special handling for queries on nested fields within _id.
    """
    processed_filters = {}
    for key, value in filters.items():
        if key.startswith("_id."):
            field_name = key.split(".", 1)[1]
            if "_id" not in processed_filters:
                processed_filters["_id"] = {}
            if isinstance(value, dict) and all(k.startswith("$") for k in value.keys()):
                for op, op_value in value.items():
                    if field_name not in processed_filters["_id"]:
                        processed_filters["_id"][field_name] = {}
                    processed_filters["_id"][field_name][op] = op_value
            else:
                processed_filters["_id"][field_name] = value
        else:
            processed_filters[key] = value
    return processed_filters

def rag_query_database(analysis):
    # Defensive: If analysis is None or not a dict, return error early
    if not isinstance(analysis, dict):
        return {"error": "Analysis object is missing or malformed.", "query": "No query generated"}

    # Default initialization (prevents UnboundLocalError)
    pipeline = []  # Always initialize pipeline at the start
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
    
    # Process any nested _id fields in the filters
    filters = process_nested_id_fields(filters)

    if aggregation:
        agg_type = aggregation.get("type")
        group_by = aggregation.get("group_by")
        field = aggregation.get("field")
        sort_order = aggregation.get("sort_order", "desc")
        sort_direction = -1 if sort_order == "desc" else 1
        limit = aggregation.get("limit", None)  # <-- CHANGED: default to None

    print(f"[DEBUG] Aggregation type: {agg_type}, group_by: {group_by}, field: {field}, sort_order: {sort_order}, limit: {limit}")

    # Now safe to check agg_type
    if agg_type == "distinct" and field:
    # pipeline already initialized at the top
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
    # pipeline already initialized at the top
        if filters:
            pipeline.append({"$match": filters})
        
        # Determine the appropriate output field name based on the group_by field
        output_field = "deviceCount"
        id_field = group_by.split(".")[-1].lower() if "." in group_by else group_by.lower()
        
        pipeline.extend([
            {"$group": {"_id": f"${group_by}", "uniqueValues": {"$addToSet": f"${distinct_field}"}}},
            {"$project": {id_field: "$_id", output_field: {"$size": "$uniqueValues"}}},
            {"$sort": {output_field: sort_direction}}
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
    # pipeline already initialized at the top
        if filters:
            pipeline.append({"$match": filters})
        pipeline.append({"$group": {"_id": f"${group_by}", "alertCount": {"$sum": 1}}})
        pipeline.append({"$sort": {"alertCount": sort_direction}})
        if limit is not None:
            pipeline.append({"$limit": limit})
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
    # pipeline already initialized at the top
        if filters:
            pipeline.append({"$match": filters})
        pipeline.extend([
            {"$group": {"_id": f"${group_by}", "average": {"$avg": f"${field}"}}},
            {"$sort": {"average": sort_direction}},
        ])
        if limit is not None:
            pipeline.append({"$limit": limit})
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

    # Handle min aggregation
    if agg_type == "min" and field:
    # pipeline already initialized at the top
        if filters:
            pipeline.append({"$match": filters})
        pipeline.append({"$group": {"_id": None, "minValue": {"$min": {"$toDouble": f"${field}"}}}})
        print(f"[DEBUG] Executing MIN aggregation pipeline: {pipeline}")
        try:
            result = list(COLLECTION.aggregate(pipeline))
            return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
        except Exception as e:
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}

    # Handle max aggregation
    if agg_type == "max" and field:
    # pipeline already initialized at the top
        if filters:
            pipeline.append({"$match": filters})
        pipeline.append({"$group": {"_id": None, "maxValue": {"$max": {"$toDouble": f"${field}"}}}})
        print(f"[DEBUG] Executing MAX aggregation pipeline: {pipeline}")
        try:
            result = list(COLLECTION.aggregate(pipeline))
            return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
        except Exception as e:
            return {"error": f"MongoDB aggregation pipeline failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}
        

    # Apply global filters first
    if filters:
        pipeline.append({"$match": filters})

    # Global addFields before facets
    if aggregation and aggregation.get("add_fields"):
        pipeline.append({"$addFields": aggregation["add_fields"]})

    # Only process facets if present in aggregation
    facet_stage = {}  # Always initialize to avoid UnboundLocalError
    if aggregation and "facets" in aggregation:
        raw_facets = aggregation["facets"]
        # Convert list of facets to dict if needed
        if isinstance(raw_facets, list):
            raw_facets = {f"facet_{i}": facet for i, facet in enumerate(raw_facets)}
        elif isinstance(raw_facets, dict):
            raw_facets = raw_facets
        else:
            raw_facets = {}
        for facet_name, facet_agg in raw_facets.items():
            facet_pipeline = []
            # Apply per-facet filters
            facet_filters = facet_agg.get("filters")
            if facet_filters:
                facet_pipeline.append({"$match": facet_filters})
            # Apply addFields inside facet if present
            if facet_agg.get("add_fields"):
                facet_pipeline.append({"$addFields": facet_agg["add_fields"]})
            # Determine aggregation type
            f_type = facet_agg.get("type")
            f_group = facet_agg.get("group_by")
            f_field = facet_agg.get("field")
            f_sort_order = facet_agg.get("sort_order", "desc")
            f_sort_dir = -1 if f_sort_order == "desc" else 1
            f_limit = facet_agg.get("limit")
            # COUNT
            if f_type == "count":
                # If group_by is not provided, but a field is present in fields, use that for grouping
                group_field = f_group
                if not group_field and fields:
                    group_field = fields[0]
                if group_field:
                    facet_pipeline.append({"$group": {"_id": f"${group_field}", "count": {"$sum": 1}}})
                    facet_pipeline.append({"$sort": {"count": f_sort_dir}})
                    if f_limit:
                        facet_pipeline.append({"$limit": f_limit})
                else:
                    facet_pipeline.append({"$count": "count"})
            # MIN / MAX / AVG / SUM
            elif f_type in ["min", "max", "avg", "sum"] and f_field:
                op_map = {"min": "$min", "max": "$max", "avg": "$avg", "sum": "$sum"}
                if f_group:
                    facet_pipeline.append({
                        "$group": {
                            "_id": f"${f_group}",
                            f_type: {op_map[f_type]: {"$toDouble": f"${f_field}"}}
                        }
                    })
                    facet_pipeline.append({"$sort": {f_type: f_sort_dir}})
                    if f_limit:
                        facet_pipeline.append({"$limit": f_limit})
                else:
                    facet_pipeline.append({
                        "$group": {
                            "_id": None,
                            f_type: {op_map[f_type]: {"$toDouble": f"${f_field}"}}
                        }
                    })
            # DISTINCT / COUNT_DISTINCT can be added here per facet if needed
            facet_stage[facet_name] = facet_pipeline
        # Always add $facet if any facet is present
        if facet_stage:
            pipeline.append({"$facet": facet_stage})

    print(f"[DEBUG] Executing FACET aggregation pipeline: {pipeline}")
    try:
        result = list(COLLECTION.aggregate(pipeline))
        return {"data": result, "query": {"pipeline": pipeline, "type": "aggregate"}}
    except Exception as e:
        return {"error": f"MongoDB facet aggregation failed: {str(e)}", "query": {"pipeline": pipeline, "type": "aggregate"}}

    # ...rest of your aggregation handlers...

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
        safe_results = db_results
        
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
        You are a professional report generator for a patient alert analysis system.
        
        # Patient Alert Analytics Report
        
        ## Query Information
        User Query: "{user_query}"
        Date Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        
        Please format your response as a concise report with the following sections:
        
        1. ## Executive Summary
           - Provide a concise summary of what was asked and the key findings
        
        2. ## Results
           - Present the data in a clean markdown table
           - For large datasets, summarize key patterns or trends
        
        3. ## Alert Analysis
           - When relevant, explain the significance of ParameterValue in relation to LowerThreshold and UpperThreshold
           - Comment on any patterns in the types of parameters triggering alerts (e.g., if blood pressure alerts are more common)
        
        4. ## Insights
           - Provide 2-3 key analytical insights derived from the data
           - Focus on actionable information that could help improve patient care or reduce unnecessary alerts
        
        If the query produced no results, clearly state this in the Executive Summary.
        
        Query analysis details (for reference only):
        {analysis_str}

        Database results:
        {summary}
        """
    else:
        prompt = f"""
        You are a helpful assistant for a patient alert analysis system.

        The user asked: "{user_query}"

        Here's how you should respond:
        1. Start by rephrasing the user's question in plain language to confirm understanding.
        2. Present the direct answer clearly (e.g., the most frequent alert type, most common ward generating alerts, etc.).
        3. Then, show the supporting database results in a clean markdown table with appropriate headers.
        4. When relevant, provide context about the alerts by explaining the relationship between ParameterValue and threshold values (LowerThreshold/UpperThreshold).
        5. If the query produced no results, clearly say: "No results found for this query."
        6. Add a short human-style explanation that connects the results back to the user's original question, including potential clinical relevance of the findings when appropriate.

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

    # Fix LLM filter key for organization queries
    if isinstance(analysis, dict) and "filters" in analysis:
        filters = analysis["filters"]
        # If LLM used '_id.$oid' for organization, convert to 'OrganizationId'
        if "_id.$oid" in filters:
            filters["OrganizationId"] = filters.pop("_id.$oid")
        # If LLM used '_id.$OrganizationId', convert to 'OrganizationId'
        if "_id.$OrganizationId" in filters:
            filters["OrganizationId"] = filters.pop("_id.$OrganizationId")
        analysis["filters"] = filters

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
