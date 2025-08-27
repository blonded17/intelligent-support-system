# llm_provider.py

class LLMProvider:
    def __init__(self, provider="ollama", model="phi3", base_url="http://localhost:11434", **kwargs):
        self.provider = provider
        self.model = model
        self.base_url = base_url
        self.kwargs = kwargs
        if provider == "ollama":
            from langchain_community.chat_models import ChatOllama
            self.llm = ChatOllama(base_url=base_url, model=model)
        elif provider == "grok":
            # Placeholder for Grok API client
            # from grok_client import GrokLLM
            # self.llm = GrokLLM(model=model, api_key=kwargs.get("api_key"))
            raise NotImplementedError("Grok provider integration not implemented.")
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    def invoke(self, prompt):
        return self.llm.invoke(prompt)
