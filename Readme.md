# Economic Data Application


## Introduction

This repository serves as a public reference on how to build a end to end data application using open-source tools. It covers the entire workflow from data collection and processing to visualization and deployment. This project will use python and the following open source tools/libraries: 
* duckdb
    * [duckdb Docs](https://duckdb.org/docs) 
* evidence
    * [evidence Docs](https://evidence.dev/docs)
* polars
    * [polars Docs](https://docs.pola.rs/api/python/stable/reference/index.html)

## Data Sources

All data for this project is from publicly available sources. The data will be stored in a duckdb database embedded within the Evidence Directory.

* Census Bureau
    * [Census Bureau API](https://www.census.gov/content/dam/Census/data/developers/api-user-guide/api-guide.pdf)
* St. Louis Federal Reserve
    * [FRED API](https://fred.stlouisfed.org/docs/api/fred/)
* Realtor.com
    * [Realtor.com API](https://www.realtor.com/research/data/)

