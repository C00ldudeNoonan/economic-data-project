import dagster as dg
from typing import Dict, List, Optional, Any
from datetime import datetime
import os
import polars as pl

from macro_agents.defs.resources.motherduck import MotherDuckResource

try:
    import openai
except ImportError:
    openai = None

try:
    import anthropic
except ImportError:
    anthropic = None

try:
    import google.generativeai as genai
except ImportError:
    genai = None


def _get_openai_models(api_key: Optional[str] = None) -> List[str]:
    """Fetch available chat/completion models from OpenAI API.

    Dynamically discovers all chat/completion models suitable for economic analysis.
    Excludes embeddings, fine-tuning, and other non-chat models.
    """
    if openai is None:
        return []

    if api_key is None:
        api_key = os.getenv("OPENAI_API_KEY")

    if not api_key:
        return []

    try:
        client = openai.OpenAI(api_key=api_key)
        models = client.models.list()

        # Filter for chat/completion models by excluding known non-chat types
        chat_models = []
        for model in models.data:
            model_id = model.id.lower()

            # Exclude embeddings, fine-tuned models, and other non-chat models
            exclude_patterns = [
                "text-embedding",
                "embedding",
                "ft:",  # Fine-tuned models
                "ft-",  # Fine-tuned models (alternative format)
                "ada-",  # Old ada models
                "babbage-",  # Old babbage models
                "curie-",  # Old curie models
                "davinci-",  # Old davinci models
            ]

            # Check if model should be excluded
            should_exclude = any(pattern in model_id for pattern in exclude_patterns)

            # Include all models that aren't excluded (covers gpt-4, gpt-3.5, o1, gpt-5, and any future chat models)
            if not should_exclude:
                # Get the original case model ID
                chat_models.append(model.id)

        return sorted(chat_models)
    except Exception as e:
        raise ValueError(f"Error fetching OpenAI models: {e}")


def _get_anthropic_models(api_key: Optional[str] = None) -> List[str]:
    """Fetch available chat/completion models from Anthropic API.

    Returns all currently available Claude models (all are chat/completion models).
    Note: Anthropic may not provide a models list endpoint - if so, this will return an empty list.
    """
    if anthropic is None:
        return []

    if api_key is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")

    if not api_key:
        return []

    try:
        client = anthropic.Anthropic(api_key=api_key)

        # Try to use the models list endpoint if available
        if hasattr(client, "models") and hasattr(client.models, "list"):
            try:
                models_response = client.models.list()
                # Extract model IDs from the response
                if hasattr(models_response, "data"):
                    return sorted([model.id for model in models_response.data])
                elif isinstance(models_response, list):
                    return sorted(
                        [
                            model.id if hasattr(model, "id") else str(model)
                            for model in models_response
                        ]
                    )
            except AttributeError:
                # Fall through - models.list() may not be implemented
                pass
            except Exception as e:
                # If models.list() exists but fails, raise the error
                raise ValueError(f"Error calling Anthropic models.list(): {e}")

        # Anthropic doesn't provide a public models list endpoint
        # Return empty list since we can't dynamically discover models
        return []

    except Exception as e:
        raise ValueError(f"Error fetching Anthropic models: {e}")


def _get_gemini_models(api_key: Optional[str] = None) -> List[str]:
    """Fetch available chat/completion models from Google Gemini API.

    Filters for models that support chat/completion (generateContent).
    Excludes embeddings and other non-chat models.
    """
    if genai is None:
        return []

    if api_key is None:
        api_key = os.getenv("GEMINI_API_KEY")

    if not api_key:
        return []

    try:
        genai.configure(api_key=api_key)

        # List available models
        models = genai.list_models()

        # Filter for chat/completion models (those that support generateContent)
        chat_models = []
        for model in models:
            # Only include models that support generateContent (chat/completion)
            if "generateContent" in model.supported_generation_methods:
                model_name = model.name.replace("models/", "")
                chat_models.append(model_name)

        return sorted(chat_models)
    except Exception as e:
        raise ValueError(f"Error fetching Gemini models: {e}")


@dg.asset(
    kinds={"duckdb"},
    group_name="agents",
    description="Fetches and stores current available chat/completion AI models from OpenAI, Anthropic, and Gemini APIs suitable for economic analysis",
)
def fetch_available_ai_models(
    context: dg.AssetExecutionContext,
    md: MotherDuckResource,
) -> dg.MaterializeResult:
    """
    Fetch available chat/completion models from OpenAI, Anthropic, and Gemini APIs and store results.

    This asset queries each provider's API to get the current list of available chat/completion models
    suitable for economic analysis. Filters out embeddings, fine-tuning models, and other non-chat models.
    Stores them in the database for reference and configuration.
    """
    # Log environment info for debugging
    env = os.getenv("ENVIRONMENT", "not set")
    context.log.info(f"Environment: {env}")
    context.log.info("Starting to fetch available AI models from all providers...")

    results: Dict[str, Any] = {
        "openai": [],
        "anthropic": [],
        "gemini": [],
    }

    errors: Dict[str, str] = {}

    # Fetch OpenAI models
    context.log.info("Fetching OpenAI models...")
    try:
        openai_models = _get_openai_models()
        results["openai"] = openai_models
        context.log.info(f"Found {len(openai_models)} OpenAI models")
    except Exception as e:
        error_msg = str(e)
        errors["openai"] = error_msg
        context.log.warning(f"Failed to fetch OpenAI models: {error_msg}")

    # Fetch Anthropic models
    context.log.info("Fetching Anthropic models...")
    try:
        anthropic_models = _get_anthropic_models()
        results["anthropic"] = anthropic_models
        if len(anthropic_models) == 0:
            context.log.warning(
                "No Anthropic models found. Anthropic may not provide a models list endpoint. "
                "Check Anthropic documentation for available models."
            )
        else:
            context.log.info(f"Found {len(anthropic_models)} Anthropic models")
    except Exception as e:
        error_msg = str(e)
        errors["anthropic"] = error_msg
        context.log.warning(f"Failed to fetch Anthropic models: {error_msg}")

    # Fetch Gemini models
    context.log.info("Fetching Gemini models...")
    try:
        gemini_models = _get_gemini_models()
        results["gemini"] = gemini_models
        context.log.info(f"Found {len(gemini_models)} Gemini models")
    except Exception as e:
        error_msg = str(e)
        errors["gemini"] = error_msg
        context.log.warning(f"Failed to fetch Gemini models: {error_msg}")

    # Prepare results - one row per model
    fetch_timestamp = datetime.now()
    rows = []

    # Add one row for each OpenAI model
    for model_name in results["openai"]:
        rows.append(
            {
                "fetch_timestamp": fetch_timestamp.isoformat(),
                "fetch_date": fetch_timestamp.strftime("%Y-%m-%d"),
                "fetch_time": fetch_timestamp.strftime("%H:%M:%S"),
                "model_provider": "openai",
                "model_name": model_name,
                "dagster_run_id": context.run_id,
                "dagster_asset_key": str(context.asset_key),
            }
        )

    # Add one row for each Anthropic model
    for model_name in results["anthropic"]:
        rows.append(
            {
                "fetch_timestamp": fetch_timestamp.isoformat(),
                "fetch_date": fetch_timestamp.strftime("%Y-%m-%d"),
                "fetch_time": fetch_timestamp.strftime("%H:%M:%S"),
                "model_provider": "anthropic",
                "model_name": model_name,
                "dagster_run_id": context.run_id,
                "dagster_asset_key": str(context.asset_key),
            }
        )

    # Add one row for each Gemini model
    for model_name in results["gemini"]:
        rows.append(
            {
                "fetch_timestamp": fetch_timestamp.isoformat(),
                "fetch_date": fetch_timestamp.strftime("%Y-%m-%d"),
                "fetch_time": fetch_timestamp.strftime("%H:%M:%S"),
                "model_provider": "gemini",
                "model_name": model_name,
                "dagster_run_id": context.run_id,
                "dagster_asset_key": str(context.asset_key),
            }
        )

    # Write to database (drop and recreate table)
    context.log.info(
        f"Writing {len(rows)} model records to database (dropping and recreating table)..."
    )
    try:
        # Convert rows to Polars DataFrame
        if rows:
            df = pl.DataFrame(rows)
        else:
            # Create empty DataFrame with correct schema if no models found
            df = pl.DataFrame(
                {
                    "fetch_timestamp": [],
                    "fetch_date": [],
                    "fetch_time": [],
                    "model_provider": [],
                    "model_name": [],
                    "dagster_run_id": [],
                    "dagster_asset_key": [],
                }
            )

        # Drop and recreate table with new data
        md.drop_create_duck_db_table(
            table_name="available_ai_models",
            df=df,
        )
        context.log.info(f"Successfully wrote {len(rows)} model records to database")
    except Exception as e:
        error_msg = f"Failed to write to database: {e}"
        context.log.error(error_msg)
        # Add database error to errors dict
        errors["database"] = error_msg
        # Still return results in metadata even if database write fails
        context.log.warning(
            "Continuing despite database write failure - results available in metadata"
        )

    # Prepare metadata
    metadata = {
        "openai_models_count": len(results["openai"]),
        "anthropic_models_count": len(results["anthropic"]),
        "gemini_models_count": len(results["gemini"]),
        "fetch_timestamp": fetch_timestamp.isoformat(),
    }

    # Add model lists to metadata (truncated if too long)
    if results["openai"]:
        metadata["openai_models"] = results["openai"][:10]  # First 10 models
        if len(results["openai"]) > 10:
            metadata["openai_models_note"] = (
                f"Showing first 10 of {len(results['openai'])} models"
            )

    if results["anthropic"]:
        metadata["anthropic_models"] = results["anthropic"]

    if results["gemini"]:
        metadata["gemini_models"] = results["gemini"][:10]  # First 10 models
        if len(results["gemini"]) > 10:
            metadata["gemini_models_note"] = (
                f"Showing first 10 of {len(results['gemini'])} models"
            )

    if errors:
        metadata["errors"] = errors

    context.log.info(
        f"Successfully fetched models: OpenAI={len(results['openai'])}, "
        f"Anthropic={len(results['anthropic'])}, Gemini={len(results['gemini'])}"
    )

    return dg.MaterializeResult(metadata=metadata)
