"""Tests for OllamaResource."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from macro_agents.defs.resources.ollama import OllamaResource


class TestOllamaResource:
    """Test cases for OllamaResource."""

    def test_initialization_defaults(self):
        """Resource should have sensible defaults."""
        resource = OllamaResource()
        assert resource.ollama_base_url == "http://localhost:11434"
        assert resource.model_name == "llama3.2:3b"
        assert resource.embedding_model == "nomic-embed-text"

    def test_initialization_custom(self):
        """Resource should accept custom values."""
        resource = OllamaResource(
            ollama_base_url="http://ollama:11434",
            model_name="mistral:7b",
            embedding_model="mxbai-embed-large",
        )
        assert resource.ollama_base_url == "http://ollama:11434"
        assert resource.model_name == "mistral:7b"
        assert resource.embedding_model == "mxbai-embed-large"

    def test_setup_for_execution(self):
        """setup_for_execution should set internal attributes."""
        resource = OllamaResource(
            ollama_base_url="http://test:11434",
            model_name="test-model",
            embedding_model="test-embed",
        )
        context = MagicMock()

        resource.setup_for_execution(context)

        assert resource._base_url == "http://test:11434"
        assert resource._model == "test-model"
        assert resource._embedding_model == "test-embed"

    @patch("macro_agents.defs.resources.ollama.dspy.LM")
    def test_get_dspy_lm(self, mock_lm_class):
        """get_dspy_lm should create a DSPy LM with Ollama config."""
        resource = OllamaResource()
        resource._base_url = "http://localhost:11434"
        resource._model = "llama3.2:3b"

        resource.get_dspy_lm()

        mock_lm_class.assert_called_once_with(
            model="ollama_chat/llama3.2:3b",
            api_base="http://localhost:11434",
            api_key="",
        )

    @patch("macro_agents.defs.resources.ollama.requests.post")
    def test_get_embeddings(self, mock_post):
        """get_embeddings should call Ollama API and return vectors."""
        mock_response = Mock()
        mock_response.json.return_value = {"embeddings": [[0.1, 0.2, 0.3] * 256]}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        resource = OllamaResource()
        resource._base_url = "http://localhost:11434"
        resource._embedding_model = "nomic-embed-text"

        result = resource.get_embeddings(["test text"])

        assert len(result) == 1
        assert len(result[0]) == 768
        mock_post.assert_called_once_with(
            "http://localhost:11434/api/embed",
            json={"model": "nomic-embed-text", "input": "test text"},
            timeout=120,
        )

    @patch("macro_agents.defs.resources.ollama.requests.post")
    def test_get_embeddings_multiple_texts(self, mock_post):
        """get_embeddings should handle multiple texts."""
        mock_response = Mock()
        mock_response.json.return_value = {"embeddings": [[0.1] * 768]}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        resource = OllamaResource()
        resource._base_url = "http://localhost:11434"
        resource._embedding_model = "nomic-embed-text"

        result = resource.get_embeddings(["text one", "text two", "text three"])

        assert len(result) == 3
        assert mock_post.call_count == 3

    @patch("macro_agents.defs.resources.ollama.requests.post")
    def test_get_embeddings_api_error(self, mock_post):
        """get_embeddings should propagate API errors."""
        import requests

        mock_post.side_effect = requests.exceptions.ConnectionError("refused")

        resource = OllamaResource()
        resource._base_url = "http://localhost:11434"
        resource._embedding_model = "nomic-embed-text"

        with pytest.raises(requests.exceptions.ConnectionError):
            resource.get_embeddings(["test"])
