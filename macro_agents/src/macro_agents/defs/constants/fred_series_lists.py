# Labor Market & Employment
labor_market_series = [
    ("UNRATE", "Unemployment Rate"),
    ("CIVPART", "Labor Force Participation Rate"),
    ("JTSJOL", "Job Openings: Total Nonfarm"),
    ("ICSA", "Initial Claims"),
    ("PAYEMS", "All Employees, Total Nonfarm"),
    (
        "AHETPI",
        "Average Hourly Earnings of Production and Nonsupervisory Employees, Total Private",
    ),
    ("JTSQUR", "Job Openings and Labor Turnover Survey: Quits Rate"),
    (
        "U6RATE",
        "Total Unemployed, Plus All Marginally Attached Workers Plus Total Employed Part Time",
    ),
    ("EMRATIO", "Employment-Population Ratio"),
    ("UNEMPLOY", "Unemployed"),
]

# Inflation & Price Dynamics
inflation_series = [
    ("CPIAUCSL", "Consumer Price Index for All Urban Consumers: All Items"),
    (
        "CPILFESL",
        "Consumer Price Index for All Urban Consumers: All Items Less Food and Energy",
    ),
    ("PCEPI", "Personal Consumption Expenditures: Chain-type Price Index"),
    (
        "PCEPILFE",
        "Personal Consumption Expenditures Excluding Food and Energy (Core PCE Price Index)",
    ),
    ("PPIACO", "Producer Price Index by Commodity: All Commodities"),
    ("T5YIE", "5-Year Breakeven Inflation Rate"),
    ("T10YIE", "10-Year Breakeven Inflation Rate"),
    ("T5YIFR", "5-Year, 5-Year Forward Inflation Expectation Rate"),
    ("CPIENGSL", "Consumer Price Index for All Urban Consumers: Energy"),
    ("CPIFABSL", "Consumer Price Index for All Urban Consumers: Food and Beverages"),
]

# ============================================================================
# MONETARY POLICY & FINANCIAL CONDITIONS
# ============================================================================

# Interest Rates & Yield Curve
interest_rates_series = [
    ("FEDFUNDS", "Effective Federal Funds Rate"),
    ("DFF", "Federal Funds Effective Rate"),
]

# Money Supply & Credit
money_credit_series = [
    ("M1SL", "M1 Money Stock"),
    ("M2SL", "M2 Money Stock"),
    ("BUSLOANS", "Commercial and Industrial Loans, All Commercial Banks"),
    ("TOTALSL", "Total Consumer Loans at All Commercial Banks"),
    ("MORTGAGE30US", "30-Year Fixed Rate Mortgage Average in the United States"),
    ("MORTGAGE15US", "15-Year Fixed Rate Mortgage Average in the United States"),
    ("BAMLH0A0HYM2", "ICE BofA US High Yield Index Option-Adjusted Spread"),
    ("BAMLC0A0CM", "ICE BofA US Corporate Index Option-Adjusted Spread"),
    (
        "AAA10Y",
        "Moody's Seasoned Aaa Corporate Bond Yield Relative to Yield on 10-Year Treasury Constant Maturity",
    ),
    (
        "BAA10Y",
        "Moody's Seasoned Baa Corporate Bond Yield Relative to Yield on 10-Year Treasury Constant Maturity",
    ),
]

# ============================================================================
# ECONOMIC OUTPUT & ACTIVITY
# ============================================================================

# GDP & Production
gdp_production_series = [
    ("GDPC1", "Real Gross Domestic Product"),
    ("GDPPOT", "Real Potential Gross Domestic Product"),
    ("INDPRO", "Industrial Production Index"),
    ("TCU", "Capacity Utilization: Total Industry"),
    ("CAPUTLG2211S", "Capacity Utilization: Manufacturing"),
    ("A939RX0Q048SBEA", "Real Gross Domestic Product Per Capita"),
    ("NYGDPMKTPCDWLD", "GDP per capita, PPP (current international $)"),
    ("GDPC96", "Real Gross Domestic Product (Seasonally Adjusted Annual Rate)"),
    ("GDPDEF", "Gross Domestic Product: Implicit Price Deflator"),
    ("NFCI", "Chicago Fed National Financial Conditions Index"),
]

# Consumer Behavior & Spending
consumer_series = [
    ("PI", "Personal Income"),
    ("PCE", "Personal Consumption Expenditures"),
    ("PCEC96", "Real Personal Consumption Expenditures"),
    ("RSXFS", "Advance Retail Sales: Retail and Food Services, Total"),
    ("PSAVERT", "Personal Saving Rate"),
    (
        "CSCICP03USM665S",
        "Consumer Opinion Surveys: Confidence Indicators: Composite Indicators: OECD Indicator for the United States",
    ),
    ("UMCSENT", "University of Michigan: Consumer Sentiment"),
    ("DSPIC96", "Real Disposable Personal Income"),
    ("PCEDG", "Personal Consumption Expenditures: Durable Goods"),
    ("PCEND", "Personal Consumption Expenditures: Nondurable Goods"),
]

# ============================================================================
# SECTORAL & FORWARD-LOOKING INDICATORS
# ============================================================================

# Housing Market
housing_series = [
    ("HOUST", "Housing Starts: Total: New Privately Owned Housing Units Started"),
    ("PERMIT", "New Private Housing Units Authorized by Building Permits"),
    ("EXHOSLUSM495S", "Existing Home Sales"),
    ("CSUSHPISA", "S&P/Case-Shiller U.S. National Home Price Index"),
    ("RHORUSQ156N", "Homeownership Rate in the United States"),
    ("HOUST1F", "New Privately-Owned Housing Units Started: Single-Family Units"),
    ("MSACSR", "Monthly Supply of New Houses in the United States"),
    ("NHSDPTS", "New Home Sales in the United States"),
    ("MORTGAGE30US", "30-Year Fixed Rate Mortgage Average in the United States"),
    ("COMPUTSA", "New Private Housing Units Under Construction"),
    ("USAUCSFRCONDOSMSAMID", "Zillow Housing Index"),  # ADDED: Zillow Housing Index
]

# International Trade & Global Linkages
trade_series = [
    ("BOPGSTB", "Trade Balance: Goods and Services, Balance of Payments Basis"),
    ("EXPGS", "Exports of Goods and Services"),
    ("IMPGS", "Imports of Goods and Services"),
    ("DTWEXBGS", "Nominal Broad U.S. Dollar Index"),
    ("DTWEXAFEGS", "Nominal Advanced Foreign Economies U.S. Dollar Index"),
    ("DTWEXEMEGS", "Nominal Emerging Markets U.S. Dollar Index"),
    ("EXUSEU", "U.S. / Euro Foreign Exchange Rate"),
    ("EXJPUS", "Japan / U.S. Foreign Exchange Rate"),
    ("EXCHUS", "China / U.S. Foreign Exchange Rate"),
    ("EXUSUK", "U.S. / U.K. Foreign Exchange Rate"),
]

# ============================================================================
# FINANCIAL MARKET STRESS & RISK
# ============================================================================

# Financial Conditions
financial_conditions_series = [
    ("VIXCLS", "CBOE Volatility Index: VIX"),
    ("NFCI", "Chicago Fed National Financial Conditions Index"),
    (
        "NFCINONFINLEVERAGE",
        "Chicago Fed National Financial Conditions Non-Financial Leverage Subindex",
    ),
    ("NFCICREDIT", "Chicago Fed National Financial Conditions Credit Subindex"),
    ("TEDRATE", "TED Spread"),
    ("BAMLH0A0HYM2", "ICE BofA US High Yield Index Option-Adjusted Spread"),
    ("BAMLC0A0CM", "ICE BofA US Corporate Index Option-Adjusted Spread"),
    (
        "DRTSCILM",
        "Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Large and Medium Firms",
    ),
    (
        "DRTSCIS",
        "Net Percentage of Domestic Banks Tightening Standards for Commercial and Industrial Loans to Small Firms",
    ),
    ("SP500", "S&P 500"),
    ("DJIA", "Dow Jones Industrial Average"),  # ADDED: Dow Jones Industrial Average
]

# ============================================================================
# LEADING & COMPOSITE INDICATORS
# ============================================================================

# Forward-Looking Measures
leading_indicators_series = [
    ("USSLIND", "Leading Index for the United States"),
    ("CFNAI", "Chicago Fed National Activity Index"),
    ("CFNAIDIFF", "Chicago Fed National Activity Index: Diffusion Index"),
    ("CFNAIMA3", "Chicago Fed National Activity Index: Three-Month Moving Average"),
    (
        "AHETPI",
        "Average Hourly Earnings of Production and Nonsupervisory Employees, Total Private",
    ),
    ("PAYEMS", "All Employees, Total Nonfarm"),
    ("HOUST", "Housing Starts: Total: New Privately Owned Housing Units Started"),
    ("PERMIT", "New Private Housing Units Authorized by Building Permits"),
    (
        "T10Y3M",
        "10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity",
    ),
]

# Regional Economic Activity
regional_indicators_series = [
    ("CFNAI", "Chicago Fed National Activity Index"),
    (
        "GACDISA066MSFRBNY",
        "Empire State Manufacturing Survey: General Business Conditions",
    ),
    ("USPHCI", "Recession Probabilities"),
]
