---
title: Asset Selection Syntax
triggers:
  - "filtering assets by tag, group, kind, upstream, or downstream"
---

Reference for the asset selection syntax used by `dg list defs --assets` and `dg launch --assets`.

## Attributes

- `key:<name>` or just `<name>` — select by asset key (e.g. `customers`)
- `tag:<key>=<value>` or `tag:<key>` — select by tag (e.g. `tag:priority=high`)
- `owner:<value>` — select by owner (e.g. `owner:team@company.com`)
- `group:<value>` — select by group (e.g. `group:sales_analytics`)
- `kind:<value>` — select by kind (e.g. `kind:dbt`)

**Wildcards:** `key:customer*`, `key:*_raw`, `*` (all assets)

## Operators

- `and` / `AND` — e.g. `tag:priority=high and kind:dbt`
- `or` / `OR` — e.g. `group:sales or group:marketing`
- `not` / `NOT` — e.g. `not kind:dbt`
- `(expr)` — grouping, e.g. `tag:priority=high and (kind:dbt or kind:python)`

## Functions

- `sinks(expr)` — assets with no downstream dependents (e.g. `sinks(group:analytics)`)
- `roots(expr)` — assets with no upstream dependencies (e.g. `roots(kind:dbt)`)

## Traversals

- `+expr` — all upstream dependencies (e.g. `+customers`)
- `expr+` — all downstream dependents (e.g. `customers+`)
- `N+expr` — N levels upstream (e.g. `2+kind:dbt`)
- `expr+N` — N levels downstream (e.g. `group:sales+1`)
- `N+expr+M` — N up, M down (e.g. `1+key:customers+2`)

## Examples

```bash
# Select by name
dg launch --assets customers
dg launch --assets "customer*"

# Select by metadata
dg launch --assets "tag:priority=high"
dg launch --assets "group:sales_analytics"
dg launch --assets "kind:dbt"
dg launch --assets "owner:team@company.com"

# Combine with operators
dg launch --assets "tag:priority=high and kind:dbt"
dg launch --assets "group:sales or group:marketing"
dg launch --assets "not kind:dbt"

# With traversals
dg launch --assets "+kind:dbt"            # all upstream of dbt assets
dg launch --assets "group:sales+"         # group:sales + all downstream
dg launch --assets "2+key:customers"      # customers + 2 levels upstream
dg launch --assets "kind:python+1"        # kind:python + 1 level downstream

# With functions
dg launch --assets "sinks(group:analytics)"  # terminal assets in group
dg launch --assets "roots(kind:dbt)"         # source dbt assets
```
