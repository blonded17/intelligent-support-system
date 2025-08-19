# Intelligent Support System

## Overview

This project is an intelligent support system for medical device logs, combining Large Language Models (LLMs) with Retrieval-Augmented Generation (RAG) to provide accurate, up-to-date answers to customer queries. The system uses LangChain, Ollama, MongoDB, and Gradio to deliver a seamless experience for both users and developers.

## Tech Stack

- **Python**: Core programming language.
- **LangChain**: Framework for LLM orchestration and RAG.
- **Ollama**: Local LLM inference (using the `phi3` model).
- **MongoDB**: Stores device logs and supports fast retrieval.
- **Gradio**: Interactive web UI for customer queries.

## Concepts Used

- **Retrieval-Augmented Generation (RAG)**: The LLM is always provided with fresh, relevant data from the database before generating a response, ensuring answers are grounded in real data.
- **Schema-Guided Query Analysis**: The LLM extracts intent and filters from user queries using a strict schema, ensuring precise database searches.
- **Secure Database Access**: MongoDB connection uses TLS (with options for development flexibility).

## How Customer Queries Are Processed

1. **User Input**: The user enters a query via the Gradio dashboard (`gradio_dashboard.py`).
2. **LLM Analysis**: The query is sent to the function `handle_customer_query` in `customer_query_handler.py`, which internally uses `analyze_query_with_llm` to:
   - Analyze the query using Ollama (LangChain integration)
   - Extract intent (e.g., lookup, report, list)
   - Extract filters (key-value pairs matching the database schema)
3. **Database Search**: The extracted filters are used in `customer_query_handler.py` to query MongoDB for relevant device logs (using the `MongoClient` and the specified collection).
4. **Response Generation**: The LLM combines its analysis with the retrieved data to generate a grounded, up-to-date answer. Serialization of results is handled by `serialize_mongo_result` in both `customer_query_handler.py` and `gradio_dashboard.py`.
5. **Result Display**: The Gradio UI (`gradio_dashboard.py`, function `query_handler`) displays both the LLM’s analysis and the database results.

**Note:**
- The LLM does not “check for data” in its own knowledge first. Instead, it always gets augmented with fresh, relevant data from your database before generating the answer. This ensures the response is up-to-date and grounded in your actual data, not just the LLM’s training.
- If you want a fallback where the LLM tries to answer from its own knowledge first, and only uses RAG if it can’t, that’s a hybrid approach, but not standard RAG.

---

## Configuration

- **MongoDB Connection:** Update the connection string in `customer_query_handler.py` and `gradio_dashboard.py` if using a different database.
- **Schema Fields:** Defined in `customer_query_handler.py` (`SCHEMA_FIELDS`).
- **Static Data:** Device types, wards, and organization info are in `static_data.json`.
- **Environment Variables:** For production, store sensitive credentials (like MongoDB URI) in environment variables or a `.env` file.

## Local Setup

1. **Install Python and pip**
   - Download and install Python (includes pip) from the official website: [https://www.python.org/downloads/](https://www.python.org/downloads/)
   - On macOS, you can also use Homebrew:
     ```bash
     brew install python
     ```
   - Verify installation:
     ```bash
     python3 --version
     pip3 --version
     ```

2. **Clone the Repository**
   ```bash
   git clone <your-repo-url>
   cd intelligent-support-system
   ```

3. **Create and Activate a Python Virtual Environment**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

4. **Install Dependencies**
   ```bash
   pip install langchain_community pymongo gradio bson
   ```

5. **Start Ollama**
   - Download and install Ollama from [https://ollama.com/](https://ollama.com/)
   - Start Ollama and pull the required model:
     ```bash
     ollama pull phi3
     ollama serve
     ```

6. **Run the Gradio Dashboard**
   ```bash
   python gradio_dashboard.py
   ```
   - Access the dashboard in your browser at the provided local URL.

7. **MongoDB Configuration**
   - Ensure your MongoDB instance is running and accessible.
   - Update the connection string in `customer_query_handler.py` if needed.

## Project Structure

```
intelligent-support-system/
│
├── customer_query_handler.py   # LLM analysis and database query logic
├── gradio_dashboard.py         # Gradio UI for user interaction
├── static_data.json            # Device types, wards, organization info
├── README.md                   # Project documentation
```

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Submit a pull request with a clear description of your changes.
4. Ensure your code is well-documented and tested.
