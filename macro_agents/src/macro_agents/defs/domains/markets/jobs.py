import dagster as dg

sp500_companies_list_job = dg.define_asset_job(
    name="sp500_companies_list_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("sp500_companies_raw"),
    description="S&P 500 company list scraping job - runs monthly on 1st at 2 AM EST",
)

nasdaq_companies_list_job = dg.define_asset_job(
    name="nasdaq_companies_list_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("nasdaq_companies_raw"),
    description="NASDAQ company list scraping job - runs monthly on 1st at 3 AM EST",
)

us_sector_etfs_ingestion_job = dg.define_asset_job(
    name="us_sector_etfs_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("us_sector_etfs_raw"),
    description="US Sector ETFs ingestion job - runs current month partition weekly on Fridays",
)

currency_etfs_ingestion_job = dg.define_asset_job(
    name="currency_etfs_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("currency_etfs_raw"),
    description="Currency ETFs ingestion job - runs current month partition weekly on Fridays",
)

commodity_etfs_ingestion_job = dg.define_asset_job(
    name="commodity_etfs_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("commodity_etfs_raw"),
    description="Commodity ETFs ingestion job - runs current month partition weekly on Fridays",
)

major_indices_ingestion_job = dg.define_asset_job(
    name="major_indices_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("major_indices_raw"),
    description="Major Indices ingestion job - runs current month partition weekly on Fridays",
)

fixed_income_etfs_ingestion_job = dg.define_asset_job(
    name="fixed_income_etfs_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("fixed_income_etfs_raw"),
    description="Fixed Income ETFs ingestion job - runs current month partition weekly on Fridays",
)

global_markets_ingestion_job = dg.define_asset_job(
    name="global_markets_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("global_markets_raw"),
    description="Global Markets ingestion job - runs current month partition weekly on Fridays",
)

sp500_companies_prices_ingestion_job = dg.define_asset_job(
    name="sp500_companies_prices_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("sp500_companies_prices_raw"),
    description="S&P 500 company prices ingestion job - runs current month partition weekly on Fridays",
)

nasdaq_companies_prices_ingestion_job = dg.define_asset_job(
    name="nasdaq_companies_prices_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("nasdaq_companies_prices_raw"),
    description="NASDAQ company prices ingestion job - runs current month partition weekly on Fridays",
)

energy_commodities_ingestion_job = dg.define_asset_job(
    name="energy_commodities_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("energy_commodities_raw"),
    description="Energy Commodities ingestion job - runs current month partition weekly on Fridays",
)

input_commodities_ingestion_job = dg.define_asset_job(
    name="input_commodities_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("input_commodities_raw"),
    description="Input Commodities ingestion job - runs current month partition weekly on Fridays",
)

agriculture_commodities_ingestion_job = dg.define_asset_job(
    name="agriculture_commodities_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 1800},
    selection=dg.AssetSelection.assets("agriculture_commodities_raw"),
    description="Agriculture Commodities ingestion job - runs current month partition weekly on Fridays",
)

sp500_splits_ingestion_job = dg.define_asset_job(
    name="sp500_splits_ingestion_job",
    tags={"dagster/priority": "10", "dagster/max_runtime": 7200},
    selection=dg.AssetSelection.assets("sp500_splits_raw"),
    description="S&P 500 stock splits ingestion job - runs monthly on 1st at 4 AM EST",
)
