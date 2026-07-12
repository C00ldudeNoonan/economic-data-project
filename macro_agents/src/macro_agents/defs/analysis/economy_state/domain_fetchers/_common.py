"""Shared helpers for the economy-state domain fetchers."""

import io

import polars as pl


def df_to_csv(df: pl.DataFrame) -> str:
    """Serialize a Polars DataFrame to a UTF-8 CSV string."""
    buffer = io.BytesIO()
    df.write_csv(buffer)
    return buffer.getvalue().decode("utf-8")
