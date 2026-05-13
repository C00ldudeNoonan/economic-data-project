import time
from collections import defaultdict

import dagster as dg

ANTHROPIC_RATE_LIMITS = {
    "claude-3-5-sonnet-20241022": {
        "input_tokens_per_minute": 200000,
        "output_tokens_per_minute": 200000,
        "requests_per_minute": 5000,
    },
    "claude-3-5-haiku-20241022": {
        "input_tokens_per_minute": 50000,
        "output_tokens_per_minute": 50000,
        "requests_per_minute": 5000,
    },
    "claude-3-opus-20240229": {
        "input_tokens_per_minute": 50000,
        "output_tokens_per_minute": 50000,
        "requests_per_minute": 5000,
    },
    "claude-3-sonnet-20240229": {
        "input_tokens_per_minute": 200000,
        "output_tokens_per_minute": 200000,
        "requests_per_minute": 5000,
    },
    "claude-3-haiku-20240307": {
        "input_tokens_per_minute": 50000,
        "output_tokens_per_minute": 50000,
        "requests_per_minute": 5000,
    },
}

_rate_limit_tracking = defaultdict(lambda: {"tokens": [], "requests": []})


def _estimate_tokens(text: str) -> int:
    """Rough token estimation: ~4 characters per token."""
    return len(text) // 4


def _check_rate_limit(
    provider: str,
    model_name: str,
    estimated_input_tokens: int,
    context: dg.AssetExecutionContext | None = None,
) -> None:
    """Check if request would exceed rate limits and wait if necessary."""
    if provider != "anthropic":
        return

    current_time = time.time()
    limits = ANTHROPIC_RATE_LIMITS.get(model_name)
    if not limits:
        if context:
            context.log.warning(
                f"Unknown Anthropic model {model_name}, using default limits"
            )
        limits = {
            "input_tokens_per_minute": 50000,
            "output_tokens_per_minute": 50000,
            "requests_per_minute": 5000,
        }

    key = f"{provider}:{model_name}"
    tracking = _rate_limit_tracking[key]

    one_minute_ago = current_time - 60

    tracking["tokens"] = [t for t in tracking["tokens"] if t["time"] > one_minute_ago]
    tracking["requests"] = [r for r in tracking["requests"] if r > one_minute_ago]

    total_tokens_last_minute = sum(t["tokens"] for t in tracking["tokens"])

    if (
        total_tokens_last_minute + estimated_input_tokens
        > limits["input_tokens_per_minute"]
    ):
        oldest_token_time = min(
            (t["time"] for t in tracking["tokens"]), default=current_time
        )
        wait_time = 60 - (current_time - oldest_token_time) + 1
        if wait_time > 0:
            if context:
                context.log.warning(
                    f"Rate limit approaching for {model_name}. "
                    f"Used {total_tokens_last_minute}/{limits['input_tokens_per_minute']} tokens/min. "
                    f"Waiting {wait_time:.1f}s before proceeding."
                )
            time.sleep(wait_time)
            tracking["tokens"] = []
            tracking["requests"] = []

    tracking["tokens"].append({"time": current_time, "tokens": estimated_input_tokens})
    tracking["requests"].append(current_time)
