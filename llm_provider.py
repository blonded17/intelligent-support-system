# llm_provider.py


class LLMProvider:
    def __init__(self, provider="ollama", model="phi3", base_url="http://localhost:11434", system_prompt=None, **kwargs):
        self.provider = provider
        self.model = model
        self.base_url = base_url
        self.kwargs = kwargs
        self.system_prompt = system_prompt
        self.chat_history = []
        if provider == "ollama":
            from langchain_community.chat_models import ChatOllama
            self.llm = ChatOllama(base_url=base_url, model=model)
        elif provider == "grok":
            raise NotImplementedError("Grok provider integration not implemented.")
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    def set_system_prompt(self, prompt):
        self.system_prompt = prompt
        self.chat_history = []

    def reset_chat(self):
        self.chat_history = []

    def invoke(self, user_message):
        # Compose chat history: system prompt + previous turns + current user message
        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})
        for turn in self.chat_history:
            messages.append(turn)
        messages.append({"role": "user", "content": user_message})
        response = self.llm.invoke(messages)
        # Store user and assistant turns for context persistence
        self.chat_history.append({"role": "user", "content": user_message})
        if hasattr(response, 'content'):
            self.chat_history.append({"role": "assistant", "content": response.content})
        else:
            self.chat_history.append({"role": "assistant", "content": str(response)})
        return response
