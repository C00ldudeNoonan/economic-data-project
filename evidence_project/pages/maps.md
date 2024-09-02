# Map Data




```sql state_data
  select
  state_name,
  state_id,
  avg(median_days_on_market)  as median_days_on_market
  from econ_db.state_map_data
  GROUP BY
  state_name,
  state_id
```

<AreaMap 
    data={state_data} 
    areaCol=state_name
    geoJsonUrl=https://raw.githubusercontent.com/PublicaMundi/MappingAPI/master/data/geojson/us-states.json
    geoId=name
    value=median_days_on_market
    valueFmt=usd
/>