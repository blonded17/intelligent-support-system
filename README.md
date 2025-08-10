
# Intelligent Support System (ISA)

## Description

Intelligent Support System (ISA) is an AI-powered dashboard for analyzing customer queries related to medical device logs. It leverages Large Language Models (LLMs) and Retrieval-Augmented Generation (RAG) to interpret user queries, extract relevant filters and fields, and fetch precise results from a MongoDB database. The system provides an intuitive Gradio-based web interface for seamless interaction.

## Features

- **Natural Language Query Analysis:** Uses LLMs to understand and extract intent, filters, and fields from user queries.
- **RAG Database Search:** Automatically queries MongoDB for relevant device logs based on LLM analysis.
- **Interactive Dashboard:** Gradio UI for entering queries and viewing both LLM analysis and database results.
- **Schema-Aware Filtering:** Ensures only valid schema fields are used for database searches.
- **Support for Multiple Device Types and Wards:** Configurable via `static_data.json`.

## Tech Stack

- **Languages:** Python
- **Frameworks/Libraries:** 
  - [Gradio](https://gradio.app/) (UI)
  - [LangChain](https://python.langchain.com/) (LLM integration)
  - [Ollama](https://ollama.com/) (LLM backend)
  - [PyMongo](https://pymongo.readthedocs.io/) (MongoDB client)
- **Database:** MongoDB (Cloud)
- **Other:** BSON (for MongoDB ObjectId handling), JSON

## Installation

1. **Clone the repository:**
	```bash
	git clone https://github.com/blonded17/intelligent-support-system.git
	cd intelligent-support-system
	```

2. **Set up Python environment (recommended):**
	```bash
	python3 -m venv venv
	source venv/bin/activate
	```

3. **Install dependencies:**
	```bash
	pip install gradio pymongo langchain-community
	```

4. **Start Ollama server (for LLM):**
	- Follow [Ollama installation guide](https://ollama.com/download) and ensure it is running at `http://localhost:11434`.

## Usage

1. **Launch the Gradio dashboard:**
	```bash
	python gradio_dashboard.py
	```
2. **Open the provided local URL in your browser.**
3. **Enter a customer query** (e.g., "Show all device logs for ICU in July 2025") and view the LLM analysis and database results.

## Configuration

- **MongoDB Connection:** Update the connection string in `customer_query_handler.py` and `gradio_dashboard.py` if using a different database.
- **Schema Fields:** Defined in `customer_query_handler.py` (`SCHEMA_FIELDS`).
- **Static Data:** Device types, wards, and organization info are in `static_data.json`.
- **Environment Variables:** For production, store sensitive credentials (like MongoDB URI) in environment variables or a `.env` file.

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

## License

This project is currently unlicensed. If you wish to use or contribute, please contact the repository owner.
