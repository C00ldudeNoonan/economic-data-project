"""Ollama resource for local LLM inference via DSPy.

Provides a ConfigurableResource wrapping DSPy configured with an Ollama
endpoint for both chat completions and embeddings.
"""

import os

import dagster as dg
import dspy
import requests
from pydantic import Field


class OllamaResource(dg.ConfigurableResource):
    """Resource for local LLM inference using Ollama + DSPy."""

    ollama_base_url: str = Field(
        default="http://localhost:11434",
        description=(
            "Ollama API base URL. Use http://ollama:11434 in Docker, "
            "http://localhost:11434 for local development."
        ),
    )

    model_name: str = Field(
        default="llama3.2:3b",
        description="Ollama chat model name (small, fast, runs on CPU)",
    )

    embedding_model: str = Field(
        default="nomic-embed-text",
        description="Ollama embedding model (768-dim, good quality)",
    )

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """Initialize DSPy LM with Ollama backend."""
        base_url = self.ollama_base_url
        if isinstance(base_url, dg.EnvVar):
            base_url = base_url.get_value() or "http://localhost:11434"
        elif not base_url:
            base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")

        model = self.model_name
        if isinstance(model, dg.EnvVar):
            model = model.get_value() or "llama3.2:3b"

        self._base_url = base_url
        self._model = model

        embed_model = self.embedding_model
        if isinstance(embed_model, dg.EnvVar):
            embed_model = embed_model.get_value() or "nomic-embed-text"
        self._embedding_model = embed_model

        log = context.log
        if log:
            log.info(
                f"Initializing Ollama resource: {base_url} "
                f"(chat={model}, embed={embed_model})"
            )

    def get_dspy_lm(self) -> dspy.LM:
        """Get a configured DSPy LM pointing at Ollama."""
        lm = dspy.LM(
            model=f"ollama_chat/{self._model}",
            api_base=self._base_url,
            api_key="",  # Ollama doesn't need an API key
        )
        return lm

    def get_embeddings(self, texts: list[str]) -> list[list[float]]:
        """Generate embeddings using the Ollama embeddings API.

        Args:
            texts: List of text strings to embed.

        Returns:
            List of embedding vectors (each 768-dim for nomic-embed-text).
        """
        embeddings = []
        for text in texts:
            response = requests.post(
                f"{self._base_url}/api/embed",
                json={"model": self._embedding_model, "input": text},
                timeout=120,
            )
            response.raise_for_status()
            data = response.json()
            # Ollama returns {"embeddings": [[...], ...]} for /api/embed
            embeddings.append(data["embeddings"][0])
        return embeddings


ollama_resource = OllamaResource(
    ollama_base_url=dg.EnvVar("OLLAMA_BASE_URL"),
    model_name=dg.EnvVar("OLLAMA_MODEL"),
    embedding_model=dg.EnvVar("OLLAMA_EMBEDDING_MODEL"),
)
