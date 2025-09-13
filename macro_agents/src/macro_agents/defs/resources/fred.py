import requests
import polars as pl
import dagster as dg


class FredResource(dg.ConfigurableResource):
    api_key: str

    def get_fred_data(self, series_code: str) -> pl.DataFrame:
        """Fetch and process data from FRED API for a given series.


        Args:
            series_code: FRED series identifier
        """
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_code}&api_key={self.api_key}&file_type=json&"

        response = requests.get(url)
        response.raise_for_status()
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
