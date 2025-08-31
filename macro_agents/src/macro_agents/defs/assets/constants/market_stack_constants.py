# Major US market indices
MAJOR_INDICES_TICKERS = [
    "SPY",
    "QQQ", 
    "DIA",
    "IWM",
    "VIX.INDX"
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
    "XLRE", # Real Estate
    "XLB",  # Materials
    "XLE",  # Energy
    "XLV"   # Health Care
]

# Fixed income ETFs
FIXED_INCOME_ETFS = [
    "CWB",   # SPDR Bloomberg Convertible Securities ETF
    "HYG",   # iShares iBoxx $ High Yield Corporate Bond ETF
    "LQD",   # iShares iBoxx $ Investment Grade Corporate Bond ETF
    "TIP",   # iShares TIPS Bond ETF
    "GOVT",  # iShares U.S. Treasury Bond ETF
    "MUB"    # iShares National Muni Bond ETF
]

# Currency and crypto ETFs
CURRENCY_ETFS = [
    "FXE",        # CurrencyShares Euro Trust
    "FXY",        # CurrencyShares Japanese Yen Trust
    "FXB",        # CurrencyShares British Pound Sterling Trust
    "FXC",        # CurrencyShares Canadian Dollar Trust
    "FXA",        # CurrencyShares Australian Dollar Trust
    "CEW",        # WisdomTree Emerging Currency Fund
    "ETHE",       # Grayscale Ethereum Trust
    "IBIT.INDX"   # iShares Bitcoin Trust
]

# Energy commodities (requires premium plan)
ENERGY_COMMODITIES = [
    "brent",
    "crude oil",
    "gasoline",
    "coal",
    "heating oil",
    "natural gas"
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
    "iron ore",
    "gallium",
    "germanium",
    "manganese",
    "nickel",
    "bitumen",
    "rubber",
    "polyethylene",
    "polypropylene",
    "polyvinyl"
]

# Agricultural commodities (requires premium plan)
AGRICULTURE_COMMODITIES = [
    "corn",
    "wheat", 
    "soybeans",
    "feeder cattle",
    "live cattle",
    "sugar",
    "cotton",
    "poultry",
    "eggs us"
]

# Global market ETFs
GLOBAL_MARKETS = [
    "EEM",  # iShares MSCI Emerging Markets ETF
    "ACWI", # iShares MSCI ACWI ETF
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
    "EPI"   # WisdomTree India Earnings Fund
]

# Combined lists for convenience
ALL_TICKER_GROUPS = [
    MAJOR_INDICES_TICKERS,
    US_SECTOR_ETFS, 
    FIXED_INCOME_ETFS,
]

ALL_COMMODITIES = (
    ENERGY_COMMODITIES + 
    INPUT_COMMODITIES + 
    AGRICULTURE_COMMODITIES
)
