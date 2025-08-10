# gradio_dashboard.py
# Python Gradio UI for LLM query analysis

import gradio as gr
from customer_query_handler import handle_customer_query
from pymongo import MongoClient
from bson import ObjectId

MONGO_CLIENT = MongoClient(
    "mongodb+srv://shubhambhatia2103:blonded17@cluster0.tdkyiq6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&tls=true",
    readPreference='secondaryPreferred'
)

def query_handler(query):
    result = handle_customer_query(query)
    import json
    output = {
        "LLM Analysis": result.get("analysis"),
        "RAG Database Results": serialize_mongo_result(result.get("db_results"))
    }
    return json.dumps(output, indent=2)

def serialize_mongo_result(result):
    if isinstance(result, list):
        return [serialize_mongo_result(doc) for doc in result]
    elif isinstance(result, dict):
        return {k: serialize_mongo_result(v) for k, v in result.items()}
    elif isinstance(result, ObjectId):
        return str(result)
    else:
        return result

iface = gr.Interface(
    fn=query_handler,
    inputs=gr.Textbox(label="Enter a customer query", lines=2),
    outputs=gr.Textbox(label="LLM & RAG Output"),
    title="LLM + RAG Query Dashboard",
    description="Enter a customer query and view both the LLM analysis and database results."
)

if __name__ == "__main__":
    iface.launch()
