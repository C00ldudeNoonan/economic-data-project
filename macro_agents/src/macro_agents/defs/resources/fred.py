import dagster as dg
import polars as pl
from pydantic import Field

from macro_agents.defs.resources._url_secrets import (
    get_safe,
    raise_for_status_safe,
)


class FredResource(dg.ConfigurableResource):
    api_key: str = Field(description="FRED API key")

    def get_fred_data(self, series_code: str) -> pl.DataFrame:
        """Fetch and process data from FRED API for a given series.


        Args:
            series_code: FRED series identifier
        """
        response = get_safe(
            "https://api.stlouisfed.org/fred/series/observations",
            params={
                "series_id": series_code,
                "api_key": self.api_key,
                "file_type": "json",
            },
        )
        raise_for_status_safe(response)
        data = response.json()

        df = pl.DataFrame(data["observations"])
        df = df.with_columns(series_code=pl.lit(series_code))

        return (
            df.drop(["realtime_start", "realtime_end"])
            .with_columns(
                pl.col("date").str.strptime(pl.Date),
                pl.when(pl.col("value") == ".")
                .then(None)
                .otherwise(pl.col("value"))
                .cast(pl.Float64),
            )
            .drop_nulls("value")
        )


fred_resource = FredResource(api_key=dg.EnvVar("FRED_API_KEY"))
