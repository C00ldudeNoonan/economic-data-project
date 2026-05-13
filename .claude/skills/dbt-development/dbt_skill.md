---
name: dbt-development
description: Expert guidance for developing dbt projects including project structure, model patterns, testing, and best practices. Use when working with dbt models, building data transformations, or setting up dbt projects.
---

# dbt Development Expert

## Quick Reference

### When to Use This Skill
- Building or refactoring dbt models
- Setting up dbt project structure
- Writing SQL transformations in dbt
- Implementing tests and documentation
- Optimizing dbt performance
- Following dbt best practices

## Core Principles

### 1. Project Structure
Organize dbt projects in three primary layers moving from source-conformed to business-conformed:

```
models/
├── staging/          # Source-conformed, atomic building blocks
│   ├── source_a/     # One subdirectory per source system
│   └── source_b/
├── intermediate/     # Purpose-built transformations
│   ├── finance/      # Business domain groupings
│   └── marketing/
└── marts/            # Business-conformed, end-user ready
    ├── core/
    ├── finance/
    └── marketing/
```

### 2. Model Naming Conventions

**Staging Models:**
- Format: `stg_[source]__[entity]s.sql`
- Example: `stg_stripe__payments.sql`
- Use double underscore between source and entity
- Always plural (unless truly singular)

**Intermediate Models:**
- Format: `int_[entity]s_[verb]s.sql`
- Example: `int_payments_pivoted_to_orders.sql`

**Mart Models:**
- Format: `fct_[entity]s.sql` or `dim_[entity]s.sql`
- Example: `fct_orders.sql`, `dim_customers.sql`

## Staging Layer Best Practices

### Standard Staging Pattern
Every staging model follows this exact pattern:

```sql
-- models/staging/stripe/stg_stripe__payments.sql

with source as (
    select * from {{ source('stripe', 'payments') }}
),

renamed as (
    select
        -- IDs
        id as payment_id,
        customer_id,
        order_id,
        
        -- Timestamps (convert to UTC, standardize naming)
        created_at as payment_created_at,
        updated_at as payment_updated_at,
        
        -- Strings (clean, standardize)
        lower(trim(payment_method)) as payment_method,
        lower(trim(status)) as payment_status,
        
        -- Numerics (recast to correct types)
        cast(amount as decimal(10,2)) as payment_amount,
        cast(amount / 100.0 as decimal(10,2)) as payment_amount_usd,
        
        -- Booleans
        case 
            when is_successful = 'true' then true
            when is_successful = 'false' then false
            else null
        end as is_successful

    from source
)

select * from renamed
```

### Staging Layer Guidelines
- **One source, one staging model** - 1:1 mapping
- **Rename fields** to fit project conventions (e.g., `<event>_at` for timestamps)
- **Recast datatypes** (timestamps to UTC, prices to decimals)
- **Light cleansing** only (remove unwanted characters, NULL for empty strings)
- **No joins or aggregations** - preserve source grain
- **Materialize as views** (default) unless handling large volumes
- **Always test primary keys** for uniqueness and not null

### Incremental Staging (Large Tables)

```sql
-- models/staging/shopify/stg_shopify__orders.sql
{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

with source as (
    select * from {{ source('shopify', 'orders') }}
    
    {% if is_incremental() %}
    where created_at > (select max(order_created_at) from {{ this }})
    {% endif %}
),

renamed as (
    select
        id as order_id,
        customer_id,
        created_at as order_created_at,
        status as order_status,
        total_amount
    from source
)

select * from renamed
```

## Intermediate Layer Best Practices

### Purpose
- Join staging models together
- Apply business logic
- Create reusable building blocks
- Reduce complexity in mart models

### Example Intermediate Model

```sql
-- models/intermediate/finance/int_payments_pivoted_to_orders.sql

{%- set payment_methods = ['credit_card', 'bank_transfer', 'paypal'] -%}

with payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

pivoted as (
    select
        order_id,
        {% for method in payment_methods %}
        sum(case when payment_method = '{{ method }}' then payment_amount else 0 end) 
            as {{ method }}_amount
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    from payments
    where payment_status = 'success'
    group by 1
)

select * from pivoted
```

### Intermediate Guidelines
- **Subdirectories by business domain** (finance, marketing, etc.)
- **Single purpose per model** - do one thing well
- **Multiple inputs, single output** - join/combine here
- **Materialize as views** in dev, consider tables in prod
- **Use custom schemas** to separate from production

## Mart Layer Best Practices

### Purpose
- Analytics-ready tables for BI tools
- Business-friendly naming
- Denormalized for query performance
- Well-documented and tested

### Example Fact Table

```sql
-- models/marts/core/fct_orders.sql
{{ config(
    materialized='table',
    tags=['daily']
) }}

with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

payments as (
    select * from {{ ref('int_payments_pivoted_to_orders') }}
),

final as (
    select
        -- Primary Key
        orders.order_id,
        
        -- Foreign Keys
        orders.customer_id,
        
        -- Dimensions
        customers.customer_name,
        customers.customer_email,
        orders.order_status,
        orders.order_created_at,
        
        -- Metrics
        payments.credit_card_amount,
        payments.bank_transfer_amount,
        payments.paypal_amount,
        coalesce(
            payments.credit_card_amount, 0
        ) + coalesce(
            payments.bank_transfer_amount, 0
        ) + coalesce(
            payments.paypal_amount, 0
        ) as total_amount

    from orders
    left join customers on orders.customer_id = customers.customer_id
    left join payments on orders.order_id = payments.order_id
)

select * from final
```

### Mart Guidelines
- **Organize by business domain**
- **Materialize as tables** for performance
- **Include comprehensive documentation**
- **Test all metrics and joins**
- **Use prefixes** (fct_, dim_) for clarity

## Testing Best Practices

### Standard Tests in schema.yml

```yaml
# models/staging/stripe/_stg_stripe__models.yml
version: 2

models:
  - name: stg_stripe__payments
    description: "Cleaned and standardized payment data from Stripe"
    columns:
      - name: payment_id
        description: "Primary key for payments"
        tests:
          - unique
          - not_null
      
      - name: payment_amount
        description: "Payment amount in cents"
        tests:
          - not_null
          
      - name: payment_status
        description: "Payment status (success, failed, pending)"
        tests:
          - accepted_values:
              values: ['success', 'failed', 'pending']
```

### Custom Generic Tests

```sql
-- tests/generic/test_positive_values.sql
{% test positive_values(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
```

Usage:
```yaml
columns:
  - name: payment_amount
    tests:
      - positive_values
```

### Testing Checklist
- [ ] Every model has a primary key tested for unique + not_null
- [ ] Accepted values for categorical fields
- [ ] Relationships between foreign keys
- [ ] Custom business logic tests
- [ ] Row count checks for incremental models

## Jinja & Macros

### DRY Principle with Macros

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name, decimal_places=2) %}
    round({{ column_name }} / 100.0, {{ decimal_places }})
{% endmacro %}
```

Usage:
```sql
select
    payment_id,
    {{ cents_to_dollars('amount_cents') }} as amount_dollars
from {{ ref('stg_payments') }}
```

### Common Jinja Patterns

**Looping for DRY code:**
```sql
{%- set payment_methods = ['credit', 'debit', 'paypal'] -%}

select
    order_id,
    {% for method in payment_methods %}
    sum(case when payment_method = '{{ method }}' then amount else 0 end) as {{ method }}_amount
    {%- if not loop.last %},{% endif %}
    {% endfor %}
from payments
group by 1
```

## Performance Optimization

### 1. Materialization Strategy
- **Views**: Staging models, low-cost queries
- **Tables**: Mart models, frequently queried
- **Incremental**: Large fact tables, append-only data
- **Ephemeral**: Intermediate steps, no materialization needed

### 2. Incremental Models Pattern

```sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
) }}

select
    order_id,
    order_date,
    total_amount
from {{ source('raw', 'orders') }}

{% if is_incremental() %}
    where order_date > (select max(order_date) from {{ this }})
{% endif %}
```

### 3. Execution Optimization
- Use `dbt run --select +model_name` for focused builds
- Use `dbt build` for production (runs + tests in DAG order)
- Use `state:modified` for CI/CD efficiency
- Apply tags for selective runs: `dbt run --select tag:daily`

## Documentation Best Practices

```yaml
# models/marts/core/_core__models.yml
version: 2

models:
  - name: fct_orders
    description: |
      Order fact table containing one row per order with associated 
      customer information and payment totals.
      
      This model is refreshed daily at 6 AM UTC.
    
    columns:
      - name: order_id
        description: "Unique identifier for each order"
        tests:
          - unique
          - not_null
      
      - name: total_amount
        description: |
          Total order amount in USD, calculated as the sum of all 
          successful payment methods (credit card + bank transfer + paypal).
```

## Git Workflow

### Branch Strategy
```bash
# Create feature branch
git checkout -b feature/new-customer-metrics

# Make changes
git add models/marts/core/fct_customers.sql
git commit -m "Add customer lifetime value metric to fct_customers"

# Push and create PR
git push origin feature/new-customer-metrics
```

### Development Flow
1. **Development**: Use `dev` target, work in feature branch
2. **Testing**: Run `dbt build --select state:modified+`
3. **Code Review**: Submit PR with clear description
4. **Deployment**: Merge to main, runs against `prod` target

## Common Patterns & Anti-Patterns

### ✅ DO
- Use `ref()` for model dependencies
- Use `source()` for raw tables
- Keep staging models 1:1 with sources
- Test every primary key
- Document business logic
- Use consistent naming
- Materialize staging as views
- Group models by business domain

### ❌ DON'T
- Reference raw tables in marts (use staging)
- Join or aggregate in staging
- Hardcode table names (use ref/source)
- Leave models without tests
- Use `SELECT *` in final models
- Skip documentation
- Over-optimize prematurely
- Nest CTEs more than 3 levels deep

## Quick Command Reference

```bash
# Development
dbt run --select model_name              # Run single model
dbt run --select +model_name             # Run model + upstream
dbt run --select model_name+             # Run model + downstream
dbt run --select tag:daily               # Run tagged models

# Testing
dbt test --select model_name             # Test single model
dbt build --select model_name            # Run + test single model

# Documentation
dbt docs generate                         # Generate docs
dbt docs serve                            # Serve docs locally

# State-based selection (CI/CD)
dbt run --select state:modified+         # Modified models + downstream
dbt test --select state:modified         # Test only modified models
```

## References

For deeper dives into specific topics:
- [Official dbt Best Practices](https://docs.getdbt.com/best-practices)
- [How We Structure Projects](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview)
- [dbt Style Guide](https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md)
- [dbt Utils Package](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/)

---

## Validation Checklist

Before completing any dbt work, verify:

- [ ] Models follow naming conventions (stg_, int_, fct_, dim_)
- [ ] Models are in correct layer/directory
- [ ] Primary keys tested (unique + not_null)
- [ ] All models use ref() or source()
- [ ] Business logic is documented in schema.yml
- [ ] Materialization strategy is appropriate
- [ ] No hardcoded values or table names
- [ ] CTEs are well-named and logical
- [ ] Model compiles: `dbt compile --select model_name`
- [ ] Tests pass: `dbt test --select model_name`