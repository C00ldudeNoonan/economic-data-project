import dagster as dg
from macro_agents.defs.resources.motherduck import MotherDuckResource
from macro_agents.defs.resources.fred import FredResource


fred_series_partition = dg.StaticPartitionsDefinition(
  [
    'BAMLH0A0HYM2',
    'DJIA',
    'DFF',
    'MORTGAGE30US',
    'USAUCSFRCONDOSMSAMID',
    'DTWEXBGS',
    'DGS10',
    'USCONS',
    'LFWA64TTUSM647S',
    'EXHOSLUSM495S',
    'MDSP',
    'MSPUS',
    'CDSP',
    'MEDDAYONMARUS',
    'MEDLISPRIPERSQUFEEUS',
    'WPUIP2311102',
    'TTLHH',
    'TTLFHH',
    'TTLHHM156N',
    'T4232MM157NCEN',
    'MEHOINUSA672N',
    'GDP',
    'GDPC1',
    'A939RX0Q048SBEA',
    'A191RL1Q225SBEA',
    'PAYEMS',
    'MANEMP',
    'CE16OV',
    'INDPRO',
    'IPMAN',
    'TCU',
    'OPHNFB',
    'PRS85006091',
    'PRS84006091',
    'OPHMFG',
    'PCEC96',
    'PCECC96',
    'GPDI',
    'RSXFS',
    'RRSFS',
    'BOPGSTB',
    'TTLCONS',
    'CPIAUCSL',
    'CPIAUCNS',
    'CPILFESL',
    'CPILFENS',
    'PCEPI',
    'PCEPILFE',
    'MEDCPIM158SFRBCLE',
    'PCETRIM12M159SFRBDAL',
    'STICKCPIM159SFRBATL',
    'CORESTICKM159SFRBATL',
    'PPIFIS',
    'PPIFID',
    'PPIACO',
    'WPU01',
    'PCUOMFGOMFG',
    'CUSR0000SA0L2',
    'CUSR0000SAH1',
    'CUSR0000SACL1E',
    'CUSR0000SASLE',
    'STICKCPIXSHLTRM159SFRBATL',
    'ECIWAG',
    'ECIMANWAG',
    'ECICONWAG'
]
)



@dg.asset(
    group_name="ingestion",
    kinds={"polars", "duckdb"},
    partitions_def=fred_series_partition,
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * 1"),
    description="Raw data from FRED API",
)
def fred_raw(
    context: dg.AssetExecutionContext, fred: FredResource, md: MotherDuckResource
) -> dg.MaterializeResult:
    series_code = context.partition_key

    data = fred.get_fred_data(series_code)
    md.upsert_data("fred_raw", data, ["date", "series_code"])

    return dg.MaterializeResult(
        metadata={
            "series_code": series_code,
            "num_records": len(data),
            "max_date": str(data["date"].max()),
            "min_date": str(data["date"].min()),
        }
    )
