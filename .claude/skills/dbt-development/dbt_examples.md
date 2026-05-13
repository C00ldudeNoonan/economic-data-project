# dbt Code Examples & Patterns

This document provides comprehensive code examples for common dbt patterns and use cases.

## Table of Contents
1. [Staging Models](#staging-models)
2. [Intermediate Models](#intermediate-models)
3. [Mart Models](#mart-models)
4. [Incremental Models](#incremental-models)
5. [Snapshots](#snapshots)
6. [Macros](#macros)
7. [Tests](#tests)
8. [Advanced Patterns](#advanced-patterns)

---

## Staging Models

### Basic Staging Model with Type Casting

```sql
-- models/staging/salesforce/stg_salesforce__accounts.sql
with source as (
    select * from {{ source('salesforce', 'accounts') }}
),

renamed as (
    select
        -- Primary Key
        id as account_id,
        
        -- Foreign Keys
        owner_id,
        parent_account_id,
        
        -- Timestamps - standardize to UTC with consistent naming
        cast(created_date as timestamp) as account_created_at,
        cast(last_modified_date as timestamp) as account_updated_at,
        
        -- Strings - clean and standardize
        lower(trim(name)) as account_name,
        lower(trim(type)) as account_type,
        lower(trim(industry)) as industry,
        
        -- Numerics - proper casting
        cast(annual_revenue as decimal(15,2)) as annual_revenue,
        cast(number_of_employees as integer) as employee_count,
        
        -- Booleans - standardize
        case 
            when is_deleted = 'true' then true
            when is_deleted = 'false' then false
            else null
        end as is_deleted,
        
        -- JSON flattening (if needed)
        json_extract_scalar(custom_fields, '$.region') as region,
        json_extract_scalar(custom_fields, '$.segment') as segment

    from source
    where not coalesce(is_deleted, false) -- Remove deleted records
)

select * from renamed
```

### Staging with Union (Multiple Sources)

```sql
-- models/staging/ecommerce/stg_ecommerce__orders.sql
with shopify_orders as (
    select
        id as order_id,
        'shopify' as source_system,
        customer_id,
        created_at as order_created_at,
        total_price as order_amount,
        status as order_status
    from {{ source('shopify', 'orders') }}
),

amazon_orders as (
    select
        order_id,
        'amazon' as source_system,
        customer_id,
        order_date as order_created_at,
        total as order_amount,
        order_status
    from {{ source('amazon', 'orders') }}
),

combined as (
    select * from shopify_orders
    union all
    select * from amazon_orders
)

select * from combined
```

### Deduplication in Staging

```sql
-- models/staging/mixpanel/stg_mixpanel__events.sql
with source as (
    select * from {{ source('mixpanel', 'events') }}
),

deduplicated as (
    select
        event_id,
        user_id,
        event_name,
        event_timestamp,
        event_properties,
        row_number() over (
            partition by event_id 
            order by _loaded_at desc
        ) as row_num
    from source
),

final as (
    select
        event_id,
        user_id,
        event_name,
        event_timestamp,
        event_properties
    from deduplicated
    where row_num = 1
)

select * from final
```

---

## Intermediate Models

### Grain Change with Aggregation

```sql
-- models/intermediate/product/int_order_items_daily.sql
with order_items as (
    select * from {{ ref('stg_shopify__order_items') }}
),

daily_aggregates as (
    select
        date(order_created_at) as order_date,
        product_id,
        product_category,
        
        -- Aggregations
        count(distinct order_id) as order_count,
        count(*) as item_count,
        sum(quantity) as total_quantity,
        sum(item_revenue) as total_revenue,
        
        -- Statistical measures
        avg(item_revenue) as avg_item_revenue,
        min(item_revenue) as min_item_revenue,
        max(item_revenue) as max_item_revenue,
        stddev(item_revenue) as stddev_item_revenue
        
    from order_items
    group by 1, 2, 3
)

select * from daily_aggregates
```

### Complex Business Logic

```sql
-- models/intermediate/finance/int_customer_segments.sql
with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

customer_metrics as (
    select
        customers.customer_id,
        customers.customer_created_at,
        customers.customer_email,
        
        -- Order metrics
        count(orders.order_id) as lifetime_orders,
        sum(orders.order_amount) as lifetime_revenue,
        avg(orders.order_amount) as avg_order_value,
        max(orders.order_created_at) as last_order_date,
        min(orders.order_created_at) as first_order_date,
        
        -- Recency calculation
        datediff('day', max(orders.order_created_at), current_date()) as days_since_last_order
        
    from customers
    left join orders on customers.customer_id = orders.customer_id
    group by 1, 2, 3
),

segmented as (
    select
        *,
        
        -- RFM Segmentation
        case
            when lifetime_orders >= 10 and lifetime_revenue >= 1000 then 'VIP'
            when lifetime_orders >= 5 and lifetime_revenue >= 500 then 'High Value'
            when lifetime_orders >= 2 and lifetime_revenue >= 100 then 'Regular'
            when lifetime_orders = 1 then 'One-Time'
            else 'Prospect'
        end as customer_segment,
        
        -- Churn risk
        case
            when days_since_last_order > 180 then 'High Risk'
            when days_since_last_order > 90 then 'Medium Risk'
            when days_since_last_order > 30 then 'Low Risk'
            else 'Active'
        end as churn_risk_category
        
    from customer_metrics
)

select * from segmented
```

### Fan-out then Fan-in Pattern

```sql
-- models/intermediate/finance/int_order_items_with_costs.sql
with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

-- Fan out to item level, join product costs
items_with_costs as (
    select
        order_items.order_id,
        order_items.item_id,
        order_items.product_id,
        order_items.quantity,
        order_items.item_price,
        
        -- Cost information from products
        products.unit_cost,
        
        -- Calculated fields
        order_items.quantity * order_items.item_price as item_revenue,
        order_items.quantity * products.unit_cost as item_cost,
        (order_items.quantity * order_items.item_price) - 
        (order_items.quantity * products.unit_cost) as item_profit
        
    from order_items
    left join products on order_items.product_id = products.product_id
)

select * from items_with_costs
```

---

## Mart Models

### Dimension Table (SCD Type 2)

```sql
-- models/marts/core/dim_customers.sql
{{ config(
    materialized='table',
    tags=['core', 'daily']
) }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_segments as (
    select * from {{ ref('int_customer_segments') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customers.customer_id']) }} as customer_key,
        
        -- Natural key
        customers.customer_id,
        
        -- Attributes
        customers.customer_name,
        customers.customer_email,
        customers.customer_created_at,
        
        -- Enriched attributes from intermediate model
        customer_segments.customer_segment,
        customer_segments.lifetime_orders,
        customer_segments.lifetime_revenue,
        customer_segments.avg_order_value,
        customer_segments.churn_risk_category,
        
        -- SCD Type 2 fields
        customers.customer_created_at as valid_from,
        null as valid_to,
        true as is_current
        
    from customers
    left join customer_segments 
        on customers.customer_id = customer_segments.customer_id
)

select * from final
```

### Fact Table with Multiple Grain Levels

```sql
-- models/marts/core/fct_order_line_items.sql
{{ config(
    materialized='table',
    tags=['core', 'daily']
) }}

with order_items as (
    select * from {{ ref('int_order_items_with_costs') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        -- Grain: One row per order line item
        
        -- Fact table key
        {{ dbt_utils.generate_surrogate_key([
            'order_items.order_id', 
            'order_items.item_id'
        ]) }} as order_line_item_key,
        
        -- Foreign keys to dimensions
        customers.customer_key,
        products.product_key,
        
        -- Degenerate dimensions
        order_items.order_id,
        order_items.item_id,
        orders.order_status,
        
        -- Time dimensions
        date(orders.order_created_at) as order_date,
        orders.order_created_at as order_timestamp,
        
        -- Measures
        order_items.quantity,
        order_items.item_price,
        order_items.item_revenue,
        order_items.item_cost,
        order_items.item_profit,
        
        -- Derived measures
        order_items.item_revenue / nullif(order_items.item_cost, 0) as profit_margin
        
    from order_items
    inner join orders on order_items.order_id = orders.order_id
    left join customers on orders.customer_id = customers.customer_id
    left join products on order_items.product_id = products.product_id
)

select * from final
```

---

## Incremental Models

### Basic Incremental Pattern

```sql
-- models/marts/core/fct_events.sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='append_new_columns'
) }}

with events as (
    select * from {{ ref('stg_mixpanel__events') }}
    
    {% if is_incremental() %}
    -- Only process new events since the last run
    where event_timestamp > (select max(event_timestamp) from {{ this }})
    {% endif %}
),

enriched as (
    select
        event_id,
        user_id,
        event_name,
        event_timestamp,
        event_properties,
        
        -- Add processing metadata
        current_timestamp() as dbt_loaded_at
        
    from events
)

select * from enriched
```

### Incremental with Delete/Update Pattern

```sql
-- models/marts/core/fct_orders_incremental.sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

with new_and_updated_orders as (
    select * from {{ ref('stg_orders') }}
    
    {% if is_incremental() %}
    -- Get orders created or updated since last run
    where order_updated_at > (select max(order_updated_at) from {{ this }})
    {% endif %}
),

final as (
    select
        order_id,
        customer_id,
        order_created_at,
        order_updated_at,
        order_status,
        order_amount,
        
        current_timestamp() as dbt_updated_at
        
    from new_and_updated_orders
)

select * from final
```

### Incremental with Late-Arriving Facts

```sql
-- models/marts/core/fct_transactions_incremental.sql
{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    on_schema_change='append_new_columns'
) }}

with source_transactions as (
    select * from {{ ref('stg_transactions') }}
    
    {% if is_incremental() %}
    -- Look back 7 days to catch late-arriving data
    where transaction_date >= (
        select dateadd('day', -7, max(transaction_date)) 
        from {{ this }}
    )
    {% endif %}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by transaction_id 
            order by _loaded_at desc
        ) as rn
    from source_transactions
),

final as (
    select
        transaction_id,
        customer_id,
        transaction_date,
        transaction_amount,
        transaction_type
    from deduplicated
    where rn = 1
)

select * from final
```

---

## Snapshots

### Basic Snapshot Configuration

```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    customer_name,
    customer_email,
    customer_status,
    customer_segment,
    updated_at
from {{ source('crm', 'customers') }}

{% endsnapshot %}
```

### Check Strategy Snapshot

```sql
-- snapshots/products_snapshot.sql
{% snapshot products_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['price', 'category', 'is_active']
    )
}}

select
    product_id,
    product_name,
    price,
    category,
    is_active,
    updated_at
from {{ source('ecommerce', 'products') }}

{% endsnapshot %}
```

---

## Macros

### Reusable Field Cleaning Macro

```sql
-- macros/clean_string_field.sql
{% macro clean_string_field(column_name) %}
    lower(trim(regexp_replace({{ column_name }}, '[^a-zA-Z0-9\\s]', '')))
{% endmacro %}
```

Usage:
```sql
select
    {{ clean_string_field('customer_name') }} as customer_name_clean
from {{ ref('stg_customers') }}
```

### Date Spine Macro

```sql
-- macros/create_date_spine.sql
{% macro create_date_spine(start_date, end_date, datepart='day') %}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart=datepart,
        start_date="to_date('" ~ start_date ~ "', 'YYYY-MM-DD')",
        end_date="to_date('" ~ end_date ~ "', 'YYYY-MM-DD')"
    ) }}
)

select
    date_{{ datepart }} as date_value,
    extract(year from date_{{ datepart }}) as year,
    extract(month from date_{{ datepart }}) as month,
    extract(day from date_{{ datepart }}) as day,
    extract(dayofweek from date_{{ datepart }}) as day_of_week
from date_spine

{% endmacro %}
```

### Grant Permissions Macro

```sql
-- macros/grant_select.sql
{% macro grant_select_on_schemas(schemas, role) %}
  {% for schema in schemas %}
    grant usage on schema {{ schema }} to role {{ role }};
    grant select on all tables in schema {{ schema }} to role {{ role }};
    grant select on all views in schema {{ schema }} to role {{ role }};
  {% endfor %}
{% endmacro %}
```

---

## Tests

### Custom Generic Test

```sql
-- tests/generic/test_valid_email.sql
{% test valid_email(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is not null
    and not regexp_like(
        {{ column_name }}, 
        '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'
    )

{% endtest %}
```

### Singular Test

```sql
-- tests/assert_positive_order_amounts.sql
-- This test will fail if any orders have negative amounts

select
    order_id,
    order_amount
from {{ ref('fct_orders') }}
where order_amount < 0
```

### Relationship Test with Custom Message

```yaml
# models/marts/core/_core__models.yml
version: 2

models:
  - name: fct_orders
    tests:
      - dbt_utils.expression_is_true:
          expression: "order_amount >= 0"
          config:
            severity: error
            error_if: ">0"
            warn_if: ">10"
```

---

## Advanced Patterns

### Dynamic Pivot with Jinja

```sql
-- models/intermediate/product/int_product_metrics_pivoted.sql
{%- set metrics = ['revenue', 'units_sold', 'profit'] -%}
{%- set time_periods = ['7d', '30d', '90d'] -%}

with daily_metrics as (
    select * from {{ ref('int_product_metrics_daily') }}
),

pivoted as (
    select
        product_id,
        product_name,
        
        {% for period in time_periods %}
        {% for metric in metrics %}
        sum(
            case 
                when datediff('day', metric_date, current_date()) <= {{ period[:-1] }}
                then {{ metric }}
                else 0
            end
        ) as {{ metric }}_{{ period }}
        {%- if not loop.last or not loop.parent.last %},{% endif %}
        {% endfor %}
        {% endfor %}
        
    from daily_metrics
    group by 1, 2
)

select * from pivoted
```

### Slowly Changing Dimension Type 2

```sql
-- models/marts/core/dim_products_scd2.sql
{{ config(
    materialized='incremental',
    unique_key='product_scd_key'
) }}

with source_data as (
    select
        product_id,
        product_name,
        category,
        price,
        is_active,
        updated_at
    from {{ ref('stg_products') }}
),

{% if is_incremental() %}

-- Find changed records
changes as (
    select
        source_data.*,
        existing.product_scd_key,
        existing.valid_from,
        existing.valid_to,
        existing.is_current
    from source_data
    inner join {{ this }} as existing
        on source_data.product_id = existing.product_id
        and existing.is_current = true
    where (
        source_data.product_name != existing.product_name
        or source_data.category != existing.category
        or source_data.price != existing.price
        or source_data.is_active != existing.is_active
    )
),

-- Close out old records
updated_records as (
    select
        product_scd_key,
        product_id,
        product_name,
        category,
        price,
        is_active,
        valid_from,
        current_timestamp() as valid_to,
        false as is_current
    from changes
),

-- Create new records
new_records as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'updated_at']) }} as product_scd_key,
        product_id,
        product_name,
        category,
        price,
        is_active,
        updated_at as valid_from,
        null as valid_to,
        true as is_current
    from changes
),

{% else %}

-- Initial load
new_records as (
    select
        {{ dbt_utils.generate_surrogate_key(['product_id', 'updated_at']) }} as product_scd_key,
        product_id,
        product_name,
        category,
        price,
        is_active,
        updated_at as valid_from,
        null as valid_to,
        true as is_current
    from source_data
),

updated_records as (
    select * from new_records limit 0
),

{% endif %}

final as (
    select * from new_records
    union all
    select * from updated_records
)

select * from final
```

---

## Project Configuration Examples

### dbt_project.yml Best Practices

```yaml
name: 'analytics'
version: '1.0.0'
config-version: 2

profile: 'analytics'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  analytics:
    # Staging layer - views by default
    staging:
      +materialized: view
      +schema: staging
      +tags:
        - "staging"
      
      # Source-specific configs
      salesforce:
        +tags:
          - "staging"
          - "salesforce"
      
      stripe:
        +tags:
          - "staging"
          - "stripe"
    
    # Intermediate layer
    intermediate:
      +materialized: view
      +schema: intermediate
      +tags:
        - "intermediate"
    
    # Marts layer - tables by default
    marts:
      +materialized: table
      +tags:
        - "marts"
      
      core:
        +schema: core
        +tags:
          - "marts"
          - "core"
          - "daily"
      
      finance:
        +schema: finance
        +tags:
          - "marts"
          - "finance"
          - "daily"
      
      marketing:
        +schema: marketing
        +tags:
          - "marts"
          - "marketing"
          - "hourly"

seeds:
  analytics:
    +schema: seeds
    +tags:
      - "seeds"

snapshots:
  analytics:
    +target_schema: snapshots
    +tags:
      - "snapshots"
```

This comprehensive reference should serve as your go-to resource for dbt development patterns!