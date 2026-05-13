"""Generate CSV of affected partitions that need rerun after non-USD cleanup."""

import csv
from collections import Counter

rows = []


def add(table, symbol, months):
    for m in months.split(","):
        rows.append([table, symbol, m.strip()])


t = "sp500_companies_prices_raw"
full8 = "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01,2026-02"

for s in [
    "A",
    "ABBV",
    "ABNB",
    "ABT",
    "ACGL",
    "ACN",
    "ADBE",
    "ADI",
    "ADM",
    "ADP",
    "ADSK",
    "AEE",
    "AEP",
    "AES",
    "AFL",
    "AIG",
    "ALB",
    "ALL",
    "AMAT",
    "AMD",
    "AME",
    "AMGN",
    "AMP",
]:
    add(t, s, full8)
add(t, "AMZN", "2025-07,2025-08")
for s in ["AON", "APA"]:
    add(t, s, full8)
add(t, "APH", "2025-07,2025-08,2025-12,2026-01")
add(t, "APO", "2025-07,2025-08,2026-01")
for s in [
    "ARE",
    "ATO",
    "AWK",
    "AXP",
    "AZO",
    "BA",
    "BAC",
    "BAX",
    "BBY",
    "BDX",
    "BEN",
    "BIIB",
    "BK",
]:
    add(t, s, full8)
add(t, "BLDR", "2025-07,2025-08,2025-09,2025-10,2026-01")
for s in ["BLK", "BMY", "BR", "BRO", "BSX"]:
    add(t, s, full8)
add(t, "BXP", "2025-07,2025-08,2025-10,2025-11,2025-12,2026-01,2026-02")
for s in ["C", "CAG", "CAT", "CBRE", "CCI", "CCL", "CDW"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["CEG", "CF"]:
    add(t, s, full8)
for s in ["CFG", "CI", "CINF"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "CL", full8)
add(t, "CLX", "2025-04,2025-05,2025-07,2025-08,2026-01")
for s in ["CMCSA", "CMG", "CMI"]:
    add(t, s, full8)
for s in ["CMS", "CNC", "CNP"]:
    add(t, s, "2025-04,2025-05,2025-07,2025-08,2026-01")
for s in ["COF", "COO", "COP"]:
    add(t, s, full8)
for s in ["COST", "CPT"]:
    add(t, s, "2025-07,2025-08,2025-12,2026-01")
add(t, "CRH", full8)
add(t, "CRL", "2025-07,2025-08,2025-12,2026-01")
add(t, "CRM", full8)
add(t, "CRWD", "2025-04,2025-05,2025-07,2025-08,2026-01")
add(t, "CSCO", full8)
add(t, "CSX", "2025-04,2025-05,2025-07,2025-08,2026-01")
add(t, "CTSH", full8)
add(t, "CVNA", "2025-04,2025-05,2025-07,2025-08,2026-01")
add(t, "CVS", full8)
add(t, "CVX", "2025-07,2025-08,2026-01")
add(t, "D", "2025-07,2025-08,2025-11,2025-12,2026-01,2026-02")
for s in ["DAL", "DAY", "DD"]:
    add(t, s, full8)
add(t, "DE", "2025-04,2025-05,2025-07,2025-08,2026-01")
add(t, "DG", "2025-03,2025-04,2025-05")
add(t, "DHR", full8)
add(t, "DIS", "2025-07,2025-08")
add(t, "DLR", "2025-07,2025-08,2026-01")
add(t, "DLTR", full8)
for s in ["DOC", "DOV", "DOW", "DRI"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "DTE", full8)
add(t, "DVN", "2025-07,2025-08,2026-01")
add(t, "EBAY", "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01")
add(t, "ECL", "2025-07,2025-08,2025-10,2025-12,2026-01")
for s in ["EFX", "EIX"]:
    add(t, s, "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01")
add(t, "EL", "2025-03,2025-04,2025-05")
add(t, "EME", "2025-07,2025-08,2025-09,2025-10,2026-01")
add(t, "EMR", "2025-07,2025-08,2026-01")
for s in ["EOG", "EQR"]:
    add(t, s, full8)
add(t, "EQT", "2025-07,2025-08,2025-12,2026-01")
add(t, "ETN", "2025-07,2025-08,2026-01")
add(t, "EW", full8)
for s in ["EXC", "EXR"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "F", full8)
add(t, "FAST", "2025-07,2025-08,2026-01")
for s in ["FCX", "FDX"]:
    add(t, s, full8)
add(t, "FE", "2025-07,2025-08,2025-11,2025-12,2026-01,2026-02")
add(t, "FFIV", full8)
add(t, "FIS", "2025-07,2025-08,2025-10,2025-11,2025-12,2026-01,2026-02")
add(t, "FOX", "2025-07,2025-08,2026-01")
add(t, "FRT", "2025-07,2025-08,2025-09,2026-01")
add(t, "FSLR", full8)
for s in ["FTV", "GD"]:
    add(t, s, full8)
add(t, "GE", "2025-07,2025-08,2026-01")
for s in ["GEN", "GEV", "GILD", "GIS", "GL", "GLW"]:
    add(t, s, full8)
add(t, "GM", "2025-07,2025-08,2026-01")
for s in ["GOOG", "GOOGL", "GS"]:
    add(t, s, full8)
for s in ["HAL", "HAS", "HCA"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "HD", full8)
add(t, "HIG", "2025-07,2025-08,2025-09,2025-10,2026-01")
for s in ["HII", "HLT"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["HON", "HPE", "HPQ"]:
    add(t, s, full8)
for s in ["HRL", "HSY", "HUM"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["HWM", "IBM", "ICE"]:
    add(t, s, full8)
for s in ["IEX", "IFF", "INTC"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "INTU", full8)
add(t, "IP", "2025-07,2025-08,2026-01")
for s in ["IT", "ITW", "J"]:
    add(t, s, full8)
for s in ["JBL", "JNJ"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["JPM", "KEY"]:
    add(t, s, full8)
add(t, "KEYS", "2025-07,2025-08,2026-01")
add(t, "KHC", full8)
for s in ["KMB", "KO"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["LEN", "LH"]:
    add(t, s, full8)
add(t, "LIN", "2025-03,2025-04,2025-05")
add(t, "LLY", "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01")
for s in ["LMT", "LUV", "LVS"]:
    add(t, s, "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01")
for s in ["LYV", "MA"]:
    add(t, s, full8)
add(t, "MAA", "2025-03,2025-04,2025-05")
for s in ["MAR", "MAS", "MCD", "MCK", "MCO"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "MDLZ", full8)
add(t, "MET", "2025-07,2025-08,2026-01")
add(
    t,
    "META",
    "2012-05,2012-06,2012-07,2012-08,2012-09,2012-10,2012-11,2012-12,2013-01,2013-02,2013-03,2013-04,2013-05,2013-06,2013-07,2013-08,2013-09,2013-10,2013-11,2013-12,2014-01,2014-02,2014-03,2014-04,2014-05,2014-06,2014-07,2014-08,2014-09,2014-10,2014-11,2014-12,2015-01,2015-02,2015-03,2015-04,2021-06,2021-07,2021-08,2021-09,2021-10,2021-11,2021-12,2022-01,2022-02,2022-03,2022-04,2022-05,2022-06,2022-07,2022-08,2022-09,2022-10,2022-11,2022-12,2023-01,2023-02,2023-03,2023-04,2023-05,2023-06,2023-07,2023-08,2023-09,2023-10,2023-11,2023-12,2024-01,2024-02,2024-03,2024-04,2024-05,2024-06,2024-07,2024-08,2024-09,2024-10,2024-11,2024-12,2025-01,2025-02,2025-03,2025-04,2025-05,2025-06,2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01,2026-02",
)
for s in ["MGM", "MKC", "MLM", "MMC", "MMM"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["MNST", "MO"]:
    add(t, s, full8)
for s in ["MOS", "MPC", "MRK"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "MS", full8)
add(t, "MSFT", "2025-07,2025-08,2026-01")
add(t, "MSI", "2025-07,2025-08,2025-12,2026-01")
add(t, "MTB", "2025-07,2025-08,2025-10,2025-11,2025-12,2026-01,2026-02")
add(t, "MTD", "2025-03,2025-04,2025-05")
add(t, "MU", full8)
for s in ["NEE", "NEM", "NFLX"]:
    add(t, s, full8)
add(t, "NKE", "2025-07,2025-08")
add(t, "NOC", "2025-07,2025-08,2026-01")
add(t, "NRG", "2025-03,2025-04,2025-05")
for s in ["NSC", "NUE", "NVDA"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["NWS", "ORCL"]:
    add(t, s, full8)
add(t, "OXY", "2025-07,2025-08,2026-01")
for s in ["PANW", "PCAR"]:
    add(t, s, full8)
add(t, "PCG", "2025-07,2025-08,2026-01")
for s in ["PEG", "PEP"]:
    add(t, s, "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01")
add(
    t,
    "PFE",
    "2013-01,2013-02,2013-03,2013-04,2013-05,2013-06,2013-07,2013-08,2013-09,2013-10,2013-11,2013-12,2014-01,2014-02,2014-03,2014-04,2014-05,2014-06,2014-07,2014-08,2014-09,2014-10,2014-11,2014-12,2015-01,2015-02,2015-03,2015-04,2021-06,2021-07,2021-08,2021-09,2021-10,2021-11,2021-12,2022-01,2022-02,2022-03,2022-04,2022-05,2022-06,2022-07,2022-08,2022-09,2022-10,2022-11,2022-12,2024-01,2024-02,2024-03,2024-04,2024-05,2024-06,2024-07,2024-08,2024-09,2024-10,2024-11,2024-12,2025-07,2025-08,2026-01",
)
add(t, "PFG", "2025-07,2025-08,2025-11,2025-12,2026-01")
for s in ["PG", "PGR"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "PHM", "2025-07,2025-08,2025-09,2025-10,2025-11,2025-12,2026-01")
add(t, "PM", "2025-07,2025-08,2025-11,2025-12,2026-01,2026-02")
add(t, "PNC", "2025-07,2025-08,2025-11,2025-12,2026-01,2026-02")
add(t, "PNW", "2025-07,2025-08,2026-01")
add(t, "POOL", full8)
for s in ["PPG", "PPL"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "PRU", full8)
for s in ["PSA", "PSX", "PTC"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "QCOM", full8)
for s in ["RCL", "REG"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "RF", "2025-03,2025-04,2025-05")
add(t, "RJF", full8)
add(t, "RMD", "2025-07,2025-08,2026-01")
add(t, "ROL", "2025-07,2025-08,2025-12,2026-01")
add(t, "ROP", "2025-07,2025-08,2026-01")
add(t, "ROST", full8)
add(t, "RSG", "2025-07,2025-08,2025-11,2025-12,2026-01,2026-02")
for s in ["SBUX", "SCHW"]:
    add(t, s, full8)
for s in ["SNPS", "SO"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["SPG", "SRE", "STLD"]:
    add(t, s, full8)
for s in ["STT", "STX"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "STZ", "2025-03,2025-04,2025-05")
add(t, "SWK", "2025-07,2025-08,2025-10,2025-11,2025-12,2026-01,2026-02")
add(t, "SYK", full8)
add(t, "SYY", "2025-07,2025-08,2026-01")
add(t, "T", full8)
add(t, "TDG", "2025-07,2025-08,2025-12,2026-01,2026-02")
add(t, "TECH", full8)
add(t, "TEL", "2025-07,2025-08,2026-01")
add(t, "TFC", "2025-07,2025-08,2025-09,2025-10,2025-11,2026-01,2026-02")
add(t, "TGT", full8)
add(t, "TJX", "2025-07,2025-08,2026-01")
add(t, "TMO", "2025-07,2025-08,2025-12,2026-01,2026-02")
add(t, "TPL", full8)
add(t, "TRV", "2025-07,2025-08,2026-01")
add(t, "TSLA", "2025-07,2025-08,2026-01")
add(t, "TSN", "2025-07,2025-08,2025-12,2026-01,2026-02")
add(t, "TTD", full8)
for s in ["TXN", "TXT"]:
    add(t, s, "2025-07,2025-08,2026-01")
add(t, "UAL", full8)
add(t, "UHS", "2025-07,2025-08,2026-01")
for s in ["UNH", "UNP", "UPS", "V", "VICI", "VLO"]:
    add(t, s, full8)
add(t, "VMC", "2025-07,2025-08,2026-01")
for s in ["VRTX", "VST"]:
    add(t, s, full8)
add(t, "VTR", "2025-03,2025-04,2025-05")
add(t, "VZ", full8)
for s in ["WAB", "WAT"]:
    add(t, s, "2025-07,2025-08,2026-01")
for s in ["WBD", "WDC"]:
    add(t, s, full8)
add(t, "WELL", "2025-03,2025-04,2025-05")
add(t, "WFC", "2025-07,2025-08,2026-01")
for s in ["WM", "WMB"]:
    add(t, s, full8)
add(t, "WMT", "2025-07,2025-08,2026-01")
for s in ["WYNN", "XOM", "YUM", "ZBRA"]:
    add(t, s, full8)

# us_sector_etfs_raw
for s in ["XLE", "XLI", "XLP"]:
    add("us_sector_etfs_raw", s, full8)

# fixed_income_etfs_raw
for s in ["CWB", "TIP"]:
    add("fixed_income_etfs_raw", s, full8)

# major_indices_raw
add("major_indices_raw", "IYT", "2026-01,2026-02")

# Write CSV
with open("scripts/affected_partitions.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["source_table", "symbol", "partition_month"])
    w.writerows(rows)

# Summary
table_counts = Counter(r[0] for r in rows)
print(f"Total partition rows: {len(rows)}")
for tbl, cnt in sorted(table_counts.items()):
    print(f"  {tbl}: {cnt}")
print("CSV written to scripts/affected_partitions.csv")
