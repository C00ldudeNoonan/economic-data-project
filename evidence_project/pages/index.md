---
title: United States Housing Inventory View
---

<Details title='Housing Data Series'>
  This page displays data from the United States Housing Inventory dataset. The dataset contains information on the number of housing units, vacancies and ownership rates in the United States. The data is broken down by series name, year, and Quarter going back to 2000. The source is from the US Census Bureau. Details on the data can be found [here](https://data.commerce.gov/time-series-economic-indicators-time-series-housing-vacancies-and-homeownership).
</Details>



## Housing Inventory and Population

```sql population
  select
    series_name,
    time_date,
    year,
    series_value,
    number_of_households,
    series_value/number_of_households as number_of_households_ratio
  from econ_db.housing_inventory_and_population
```

<Dropdown
    name=TimeSeries
    data={population}
    value=series_name
    multiple=true
    selectAllByDefault=true
/>

```sql population_plot
  select
    *
  from ${population}
  WHERE series_name in ${inputs.TimeSeries.value}
```



<LineChart 
    data={population_plot}
    x=time_date
    y=number_of_households_ratio
    series=series_name
/>



```sql full_data
  select
      series_name as series_name,
      plot_groupings,
      time_date,
      series_value
  from econ_db.housing_inventory
```

<DateRange
    name=date_range
    presetRanges={['Last Year', 'Year to Date', 'All Time', 'Last 12 Months']}
    data={full_data}
    dates=time_date
    defaultValue={'All Time'} 
/>

## High Level Inventory View


```sql occupancy_inventory
  select
        series_name,
        time_date,
        series_value
    from ${full_data}
    where series_name in ('Owner Occupied Units', 'Total Vacant Housing Units', 'Renter Occupied Units')
    AND time_date between '${inputs.date_range.start}' and '${inputs.date_range.end}'
```

```sql total_inventory
  select
        series_name,
        time_date,
        series_value
    from ${full_data}
    where series_name = 'Total Housing Units'
    AND time_date between '${inputs.date_range.start}' and '${inputs.date_range.end}'

```

<AreaChart
  data={occupancy_inventory}
    x=time_date
    y=series_value
    series=series_name
    title='Total Housing Inventory'
/>

```sql rate_series 
  select 
        series_name,
    from ${full_data}
    where plot_groupings = 'Rates'
    AND time_date between '${inputs.date_range.start}' and '${inputs.date_range.end}'
    group by
        series_name
```
## Ownership and Vacancy Rates

<Dropdown
    name=TimeSeries
    data={rate_series}
    value=series_name
    multiple=true
    selectAllByDefault=true
/>

```sql rates 
  select
        series_name,
        time_date,
        series_value
    from ${full_data}
    where plot_groupings = 'Rates'
    AND time_date between '${inputs.date_range.start}' and '${inputs.date_range.end}'
    AND series_name in ${inputs.TimeSeries.value}
```

<LineChart 
    data={rates}
    x=time_date
    y=series_value
    series=series_name
/>

## Housing Vacancies

<ButtonGroup 
    name=hardcoded_options
    title='Select Chart Type'
    >
    <ButtonGroupItem valueLabel="Stacked Values" value="1" />
    <ButtonGroupItem valueLabel="100% Stacked" value="2" />

</ButtonGroup>

```sql vacant_inventory
  select
        series_name,
        time_date,
        series_value,
        plot_groupings
    from ${full_data}
    where plot_groupings = 'Vacant Inventory'
    AND time_date between '${inputs.date_range.start}' and '${inputs.date_range.end}'
```

{#if inputs.hardcoded_options === '2'}

<AreaChart
    data={vacant_inventory}
    x=time_date
    y=series_value
    series=series_name
    type=stacked100
/>

{:else }

<AreaChart
    data={vacant_inventory}
    x=time_date
    y=series_value
    series=series_name
/>


{/if}