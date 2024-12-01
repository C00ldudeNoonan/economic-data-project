---
queries:
  - state_data: sources/econ_md/raw/state_data.sql

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

```sql state
SELECT
    *
FROM state_data
```


### Filter query first and then pass into map
<Dropdown 
    data={state_data_columns} 
    name=column_selector 
    value=column_name 
    title="Select a series" 
    defaultValue="total_listing_count"
/>
{inputs.column_selector.value}
<AreaMap 
    data={state} 
    areaCol=state
    geoJsonUrl=https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json
    geoId=name
    value=total_listing_count
    startingLat=40
    startingLong=-95
    startingZoom=4
    height=600
/> 

## supply and demand pivot table heatmap by values

 series value ,last month change,  3month delta, 6 month, 12 month, 

