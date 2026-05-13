from typing import TYPE_CHECKING, Any, cast

import dagster as dg

if TYPE_CHECKING:
    from macro_agents.defs.analysis.economy_state.resource import (
        EconomicAnalysisResource,
    )


def _get_token_usage(
    economic_analysis: "EconomicAnalysisResource",
    initial_history_length: int,
    context: dg.AssetExecutionContext,
) -> dict[str, Any]:
    """
    Extract token usage and cost from DSPy LM history.

    Returns dictionary with prompt_tokens, completion_tokens, total_tokens,
    and cost information (if available from API, otherwise calculated).
    """
    token_usage: dict[str, Any] = {
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "total_tokens": 0,
    }

    if not hasattr(economic_analysis, "_lm"):
        context.log.warning("LM object not found, cannot track token usage")
        return token_usage

    try:
        lm = economic_analysis._lm

        if hasattr(lm, "usage") and lm.usage:
            usage = lm.usage
            if isinstance(usage, dict):
                usage_dict = cast(dict[str, Any], usage)
                token_usage["prompt_tokens"] = usage_dict.get("prompt_tokens", 0)
                token_usage["completion_tokens"] = usage_dict.get(
                    "completion_tokens", 0
                )
                token_usage["total_tokens"] = usage_dict.get("total_tokens", 0)
                if (
                    "cost" in usage_dict
                    or "total_cost" in usage_dict
                    or "cost_usd" in usage_dict
                ):
                    token_usage["total_cost_usd"] = usage_dict.get(
                        "cost",
                        usage_dict.get("total_cost", usage_dict.get("cost_usd", 0)),
                    )
                if "prompt_cost" in usage_dict or "prompt_cost_usd" in usage_dict:
                    token_usage["prompt_cost_usd"] = usage_dict.get(
                        "prompt_cost", usage_dict.get("prompt_cost_usd", 0)
                    )
                if (
                    "completion_cost" in usage_dict
                    or "completion_cost_usd" in usage_dict
                ):
                    token_usage["completion_cost_usd"] = usage_dict.get(
                        "completion_cost", usage_dict.get("completion_cost_usd", 0)
                    )
            elif hasattr(usage, "prompt_tokens"):
                token_usage["prompt_tokens"] = getattr(usage, "prompt_tokens", 0)
                token_usage["completion_tokens"] = getattr(
                    usage, "completion_tokens", 0
                )
                token_usage["total_tokens"] = getattr(usage, "total_tokens", 0)
                if (
                    hasattr(usage, "cost")
                    or hasattr(usage, "total_cost")
                    or hasattr(usage, "cost_usd")
                ):
                    token_usage["total_cost_usd"] = getattr(
                        usage,
                        "cost",
                        getattr(usage, "total_cost", getattr(usage, "cost_usd", 0)),
                    )
                if hasattr(usage, "prompt_cost") or hasattr(usage, "prompt_cost_usd"):
                    token_usage["prompt_cost_usd"] = getattr(
                        usage, "prompt_cost", getattr(usage, "prompt_cost_usd", 0)
                    )
                if hasattr(usage, "completion_cost") or hasattr(
                    usage, "completion_cost_usd"
                ):
                    token_usage["completion_cost_usd"] = getattr(
                        usage,
                        "completion_cost",
                        getattr(usage, "completion_cost_usd", 0),
                    )

            if token_usage["total_tokens"] > 0:
                context.log.info(f"Token usage from lm.usage: {token_usage}")
                return token_usage

        get_token_count = getattr(lm, "get_token_count", None)
        if callable(get_token_count):
            try:
                actual_counts = get_token_count()
                if actual_counts:
                    if isinstance(actual_counts, dict):
                        token_usage.update(cast(dict[str, Any], actual_counts))
                    else:
                        token_usage["total_tokens"] = actual_counts
                    if token_usage["total_tokens"] > 0:
                        context.log.info(
                            f"Token usage from get_token_count: {token_usage}"
                        )
                        return token_usage
            except Exception as e:
                context.log.debug(f"get_token_count failed: {e}")

        if hasattr(lm, "history"):
            history = lm.history
            new_entries = (
                history[initial_history_length:]
                if len(history) > initial_history_length
                else []
            )

            for entry in new_entries:
                if isinstance(entry, dict):
                    entry_dict = cast(dict[str, Any], entry)
                    if "usage" in entry_dict:
                        usage = entry_dict["usage"]
                        if isinstance(usage, dict):
                            usage_dict = cast(dict[str, Any], usage)
                            token_usage["prompt_tokens"] += usage_dict.get(
                                "prompt_tokens", 0
                            )
                            token_usage["completion_tokens"] += usage_dict.get(
                                "completion_tokens", 0
                            )
                            token_usage["total_tokens"] += usage_dict.get(
                                "total_tokens", 0
                            )
                            if (
                                "cost" in usage_dict
                                or "total_cost" in usage_dict
                                or "cost_usd" in usage_dict
                            ):
                                token_usage["total_cost_usd"] = token_usage.get(
                                    "total_cost_usd", 0
                                ) + usage_dict.get(
                                    "cost",
                                    usage_dict.get(
                                        "total_cost", usage_dict.get("cost_usd", 0)
                                    ),
                                )
                            if (
                                "prompt_cost" in usage_dict
                                or "prompt_cost_usd" in usage_dict
                            ):
                                token_usage["prompt_cost_usd"] = token_usage.get(
                                    "prompt_cost_usd", 0
                                ) + usage_dict.get(
                                    "prompt_cost",
                                    usage_dict.get("prompt_cost_usd", 0),
                                )
                            if (
                                "completion_cost" in usage_dict
                                or "completion_cost_usd" in usage_dict
                            ):
                                token_usage["completion_cost_usd"] = token_usage.get(
                                    "completion_cost_usd", 0
                                ) + usage_dict.get(
                                    "completion_cost",
                                    usage_dict.get("completion_cost_usd", 0),
                                )
                    elif "prompt_tokens" in entry_dict:
                        token_usage["prompt_tokens"] += entry_dict.get(
                            "prompt_tokens", 0
                        )
                    elif "completion_tokens" in entry_dict:
                        token_usage["completion_tokens"] += entry_dict.get(
                            "completion_tokens", 0
                        )
                elif hasattr(entry, "prompt_tokens"):
                    token_usage["prompt_tokens"] += getattr(entry, "prompt_tokens", 0)
                if hasattr(entry, "completion_tokens"):
                    token_usage["completion_tokens"] += getattr(
                        entry, "completion_tokens", 0
                    )
                elif isinstance(entry, dict) and "response" in entry:
                    entry_dict = cast(dict[str, Any], entry)
                    try:
                        prompt_text = str(
                            entry_dict.get("messages", entry_dict.get("prompt", ""))
                        )
                        response_text = str(entry_dict.get("response", ""))
                        token_usage["prompt_tokens"] += len(prompt_text) // 4
                        token_usage["completion_tokens"] += len(response_text) // 4
                    except Exception:
                        pass

            token_usage["total_tokens"] = (
                token_usage["prompt_tokens"] + token_usage["completion_tokens"]
            )

            if token_usage["total_tokens"] > 0:
                context.log.info(f"Token usage from history: {token_usage}")
                return token_usage

        if hasattr(lm, "client") and hasattr(lm.client, "usage"):
            try:
                client_usage = lm.client.usage
                if client_usage:
                    if isinstance(client_usage, dict):
                        token_usage.update(cast(dict[str, Any], client_usage))
                    elif hasattr(client_usage, "prompt_tokens"):
                        token_usage["prompt_tokens"] = getattr(
                            client_usage, "prompt_tokens", 0
                        )
                        token_usage["completion_tokens"] = getattr(
                            client_usage, "completion_tokens", 0
                        )
                        token_usage["total_tokens"] = getattr(
                            client_usage, "total_tokens", 0
                        )
                    if token_usage["total_tokens"] > 0:
                        context.log.info(
                            f"Token usage from client.usage: {token_usage}"
                        )
                        return token_usage
            except Exception as e:
                context.log.debug(f"client.usage access failed: {e}")

        if token_usage["total_tokens"] == 0:
            context.log.warning(
                "Could not extract token usage. "
                f"History length: {len(history) if hasattr(lm, 'history') else 'N/A'}, "
                f"Initial length: {initial_history_length}, LM type: {type(lm)}"
            )

    except Exception as e:
        context.log.warning(f"Could not extract token usage: {e}", exc_info=True)

    return token_usage


def _calculate_cost(
    provider: str,
    model_name: str,
    prompt_tokens: int,
    completion_tokens: int,
) -> dict[str, float]:
    """
    Calculate cost based on provider, model, and token usage.

    Pricing per 1M tokens (as of 2024-2025):
    - OpenAI: https://openai.com/api/pricing/
    - Anthropic: https://www.anthropic.com/pricing
    - Gemini: https://ai.google.dev/pricing

    Returns dictionary with prompt_cost, completion_cost, and total_cost in USD.
    """
    pricing = {
        "openai": {
            "gpt-4-turbo-preview": {"prompt": 10.0, "completion": 30.0},
            "gpt-4-turbo": {"prompt": 10.0, "completion": 30.0},
            "gpt-4": {"prompt": 30.0, "completion": 60.0},
            "gpt-4o": {"prompt": 5.0, "completion": 15.0},
            "gpt-4o-mini": {"prompt": 0.15, "completion": 0.6},
            "gpt-3.5-turbo": {"prompt": 0.5, "completion": 1.5},
            "default": {"prompt": 10.0, "completion": 30.0},
        },
        "anthropic": {
            "claude-3-5-opus-20241022": {"prompt": 15.0, "completion": 75.0},
            "claude-3-5-sonnet-20241022": {"prompt": 3.0, "completion": 15.0},
            "claude-3-5-haiku-20241022": {"prompt": 1.0, "completion": 5.0},
            "claude-3-opus-20240229": {"prompt": 15.0, "completion": 75.0},
            "claude-3-sonnet-20240229": {"prompt": 3.0, "completion": 15.0},
            "claude-3-haiku-20240307": {"prompt": 0.25, "completion": 1.25},
            "default": {"prompt": 3.0, "completion": 15.0},
        },
        "gemini": {
            "gemini-2.0-flash-exp": {"prompt": 0.075, "completion": 0.3},
            "gemini-1.5-pro": {"prompt": 1.25, "completion": 5.0},
            "gemini-1.5-flash": {"prompt": 0.075, "completion": 0.3},
            "gemini-3-pro-preview": {"prompt": 1.25, "completion": 5.0},
            "default": {"prompt": 0.075, "completion": 0.3},
        },
    }

    provider_pricing = pricing.get(provider.lower(), {})
    model_pricing = provider_pricing.get(
        model_name.lower(),
        provider_pricing.get("default", {"prompt": 1.0, "completion": 2.0}),
    )

    prompt_cost_per_million = model_pricing["prompt"]
    completion_cost_per_million = model_pricing["completion"]

    prompt_cost = (prompt_tokens / 1_000_000) * prompt_cost_per_million
    completion_cost = (completion_tokens / 1_000_000) * completion_cost_per_million
    total_cost = prompt_cost + completion_cost

    return {
        "prompt_cost_usd": round(prompt_cost, 6),
        "completion_cost_usd": round(completion_cost, 6),
        "total_cost_usd": round(total_cost, 6),
    }
