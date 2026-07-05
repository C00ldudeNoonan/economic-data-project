# Major US market indices and broad market ETFs
# Note: VIX removed - MarketStack returns 406 for VIX.INDX; use FRED VIXCLS instead
MAJOR_INDICES_TICKERS = [
    "SPY",
    "QQQ",
    "DIA",
    "IWM",
    "RSP",
    "IYT",
    "SOXX",
    "IWD",  # iShares Russell 1000 Value
    "IWF",  # iShares Russell 1000 Growth
    "VLUE",  # iShares Edge MSCI USA Value Factor
    "MTUM",  # iShares Edge MSCI USA Momentum Factor
    "QUAL",  # iShares Edge MSCI USA Quality Factor
    "USMV",  # iShares Edge MSCI USA Min Vol Factor
    "SIZE",  # iShares Edge MSCI USA Min Vol Factor
]

# US sector ETFs
US_SECTOR_ETFS = [
    "XLK",  # Technology
    "XLC",  # Communication Services
    "XLY",  # Consumer Discretionary
    "XLF",  # Financial
    "XLI",  # Industrial
    "XLU",  # Utilities
    "XLP",  # Consumer Staples
    "XLRE",  # Real Estate
    "XLB",  # Materials
    "XLE",  # Energy
    "XLV",  # Health Care
]

# Fixed income ETFs
FIXED_INCOME_ETFS = [
    "CWB",  # SPDR Bloomberg Convertible Securities ETF
    "HYG",  # iShares iBoxx $ High Yield Corporate Bond ETF
    "LQD",  # iShares iBoxx $ Investment Grade Corporate Bond ETF
    "TIP",  # iShares TIPS Bond ETF
    "GOVT",  # iShares U.S. Treasury Bond ETF
    "MUB",  # iShares National Muni Bond ETF
]

# Currency and crypto ETFs
CURRENCY_ETFS = [
    "FXE",  # CurrencyShares Euro Trust
    "FXY",  # CurrencyShares Japanese Yen Trust
    "FXB",  # CurrencyShares British Pound Sterling Trust
    "FXC",  # CurrencyShares Canadian Dollar Trust
    "FXA",  # CurrencyShares Australian Dollar Trust
    "CEW",  # WisdomTree Emerging Currency Fund
    "ETHE",  # Grayscale Ethereum Trust
    # IBIT.INDX removed - MarketStack returns 422 (ticker dot notation unsupported)
]

# Commodity ETFs spanning precious metals, energy, agriculture, and broad baskets.
COMMODITY_ETFS = [
    "GLD",  # SPDR Gold Shares
    "SLV",  # iShares Silver Trust
    "USO",  # United States Oil Fund
    "UNG",  # United States Natural Gas Fund
    "DBA",  # Invesco DB Agriculture Fund
    "DBC",  # Invesco DB Commodity Index Tracking Fund
    "CORN",  # Teucrium Corn Fund
    "WEAT",  # Teucrium Wheat Fund
    "SOYB",  # Teucrium Soybean Fund
    "CPER",  # United States Copper Index Fund
    "PPLT",  # abrdn Physical Platinum Shares ETF
    "PALL",  # abrdn Physical Palladium Shares ETF
]

# Energy commodities (requires premium plan)
ENERGY_COMMODITIES = [
    "brent",
    "crude_oil",
    "gasoline",
    "coal",
    "heating_oil",
    "natural_gas",
]

# Industrial input commodities (requires premium plan)
INPUT_COMMODITIES = [
    "silver",
    "gold",
    "copper",
    "steel",
    "aluminum",
    "lead",
    "lithium",
    "lumber",
    "titanium",
    "zinc",
    "iron_ore",
    "gallium",
    "germanium",
    "manganese",
    "nickel",
    "bitumen",
    "rubber",
    "polyethylene",
    "polypropylene",
    "polyvinyl",
]

# Agricultural commodities (requires premium plan)
AGRICULTURE_COMMODITIES = [
    "corn",
    "wheat",
    "soybeans",
    "feeder_cattle",
    "live_cattle",
    "sugar",
    "cotton",
    "poultry",
    "eggs_us",
]

# Global market ETFs
GLOBAL_MARKETS = [
    "EEM",  # iShares MSCI Emerging Markets ETF
    "ACWI",  # iShares MSCI ACWI ETF
    "EWG",  # iShares MSCI Germany ETF
    "EWJ",  # iShares MSCI Japan ETF
    "EWU",  # iShares MSCI United Kingdom ETF
    "EWQ",  # iShares MSCI France ETF
    "EWA",  # iShares MSCI Australia ETF
    "FXI",  # iShares China Large-Cap ETF
    "EZA",  # iShares MSCI South Africa ETF
    "EWY",  # iShares MSCI South Korea ETF
    "EWW",  # iShares MSCI Mexico ETF
    "EWZ",  # iShares MSCI Brazil ETF
    "EPI",  # WisdomTree India Earnings Fund
]

# Combined lists for convenience
ALL_TICKER_GROUPS = [
    MAJOR_INDICES_TICKERS,
    US_SECTOR_ETFS,
    FIXED_INCOME_ETFS,
    COMMODITY_ETFS,
]

ALL_COMMODITIES = ENERGY_COMMODITIES + INPUT_COMMODITIES + AGRICULTURE_COMMODITIES

SP500_COMPANY_TICKERS_PARTITION_NAME = "sp500_company_tickers"
NASDAQ_COMPANY_TICKERS_PARTITION_NAME = "nasdaq_company_tickers"
