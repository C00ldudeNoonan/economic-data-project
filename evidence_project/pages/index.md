---
queries:
  - state_data: maps/state_map_data.sql
  - inventory: govt_economic_stats/housing_inventory_latest_aggregates.sql
  - fred: govt_economic_stats/fred_series_latest_aggregates.sql
title: US Real Estate Market
---


## Overview



## State Level Data

```sql state_data_columns
SELECT
    column_name
FROM information_schema.columns 
WHERE table_name = 'state_data'
    AND column_name not in ('year_month', 'state')

```


### Metrics by State Level
<Dropdown 
    data={state_data_columns} 
    name=column_selector 
    value=column_name 
    title="Select a series" 
    defaultValue="total_listing_count"
/>
{inputs.column_selector.value}

```sql state
SELECT
    year_month,
    state,
    ${inputs.column_selector.value} as metric
FROM state_data
```

### {inputs.column_selector.value} By State 
<AreaMap 
    data={state} 
    areaCol=state
    geoJsonUrl=https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json
    geoId=name
    value=metric
    startingLat=40
    startingLong=-95
    startingZoom=4
    height=600
    legend=true
/> 

## Supply and Demand

```sql economic_data
With supply_and_demand as (
    SELECT
        *,
    CASE 
        WHEN series_code in ('WPUIP2311102', 'USCONS', 'T4232MM157NCEN', 'TOTAL', 'MORTGAGE30US') THEN 'Supply'
        WHEN series_code in ('TTLHHM156N', 'MEDDAYONMARUS', 'LFWA64TTUSM647S', 'MEDLISPRIPERSQUFEEUS', 'MDSP') THEN 'Demand'
        ELSE 'other' END As series_type
    FROM ${inventory}
    UNION
    SELECT
        *,
    CASE 
        WHEN series_code in ('WPUIP2311102', 'USCONS', 'T4232MM157NCEN', 'TOTAL', 'MORTGAGE30US') THEN 'Supply'
        WHEN series_code in ('TTLHHM156N', 'MEDDAYONMARUS', 'LFWA64TTUSM647S', 'MEDLISPRIPERSQUFEEUS', 'MDSP') THEN 'Demand'
        ELSE 'other' END As series_type
    FROM ${fred}
)

SELECT
    *
FROM supply_and_demand
WHERE series_type <> 'other' 
```

<DataTable data={economic_data}>
	<Column id=series_name wrap=true/>
	<Column id=series_type align=center/>
	<Column id=current_value title="Latest Value"/>
  	<Column id=pct_change_3m title="3 Month Percent Change" contentType=bar barColor=#aecfaf/>
  	<Column id=pct_change_6m title="6 Month Percent Change" contentType=bar barColor=#ffe08a/>
    <Column id=pct_change_1y title="1 Year Percent Change" contentType=bar barColor=#aecfaf/>
</DataTable>



eventually do a heat map for these two

Supply:
Total Housing units
Construction
Manufacturing
Raw Materials
Housing Occupancy

Demand:
Population growth
Household formation
zillow housing index
interest rates
financial conditions
