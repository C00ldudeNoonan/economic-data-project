---
queries:
  - fred_data: sources/econ_md/raw/fred_data.sql
  - housing_invetory: sources/econ_md/raw/housing_inventory.sql
  - housing_pulse: sources/econ_md/raw/housing_pulse.sql
title: Raw Data Export
---

# Main Schema

 - fred_data (St. Louis Fed)
 - housing_inventory (Census Bureau)
 - housing_pulse (Census Bureau)

## Select a table to download
<Dropdown name=tables> 
    <DropdownOption valueLabel="fred_data" value="fred_data" />
    <DropdownOption valueLabel="housing_inventory" value="housing_inventory" />
    <DropdownOption valueLabel="housing_pulse" value="housing_pulse" />
</Dropdown>


```sql export_table
select * from ${inputs.tables.value}
```

<DataTable data={export_table} />




