"""Pure Financial Conditions Index scoring math."""

import polars as pl


def calculate_weighted_score(values: list[float], weights: list[float]) -> float:
    """
    Calculate weighted score using the provided weights.
    """
    if len(values) != len(weights):
        raise ValueError("Values and weights must have the same length")

    return sum(val * weight for val, weight in zip(values, weights))


def calculate_fci_scores(
    merged_df: pl.DataFrame, weights: dict[str, list[float]]
) -> pl.DataFrame:
    """
    Calculate FCI scores using rolling windows and weights.
    """

    # Create weights DataFrame
    weights_df = pl.DataFrame(weights)

    # Calculate rolling scores for each component
    result_df = merged_df.clone()

    # Define the rolling window size
    window_size = 12

    # Calculate scores for each component
    components = ["equity", "tripleb", "mortgage", "housing", "dollar", "10yr", "FFR"]

    for component in components:
        if component in merged_df.columns:
            # Get weights for this component (reversed order)
            component_weights = weights_df[component].to_list()[::-1]

            # Calculate rolling weighted score
            scores = []
            for i in range(len(merged_df)):
                if i < window_size - 1:
                    scores.append(None)
                else:
                    window_values = merged_df[component][
                        i - window_size + 1 : i + 1
                    ].to_list()

                    # Filter out None values and get corresponding weights
                    valid_indices = [
                        j for j, val in enumerate(window_values) if val is not None
                    ]
                    valid_values = [window_values[j] for j in valid_indices]
                    valid_weights = [component_weights[j] for j in valid_indices]

                    # Only calculate score if we have valid values
                    if len(valid_values) > 0 and len(valid_values) == len(
                        valid_weights
                    ):
                        score = calculate_weighted_score(valid_values, valid_weights)
                        scores.append(score)
                    else:
                        scores.append(None)

            result_df = result_df.with_columns(
                [pl.Series(f"{component}_score", scores)]
            )

    # Calculate FCI as sum of all component scores
    score_columns = [
        f"{comp}_score" for comp in components if f"{comp}_score" in result_df.columns
    ]

    if score_columns:
        result_df = result_df.with_columns(
            [pl.sum_horizontal(score_columns).alias("FCI")]
        )

    return result_df
