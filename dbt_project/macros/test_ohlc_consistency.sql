{% test ohlc_consistency(model) %}
-- Fails if any OHLC row violates logical consistency rules.
-- high must be >= low, open, close; low must be <= open, close; all prices > 0.
select
    symbol,
    cast(date as date) as date,
    open, high, low, close, adj_close
from {{ model }}
where
    high < low
    or high < open
    or high < close
    or low > open
    or low > close
    or close <= 0
    or open <= 0
    or high <= 0
    or low <= 0
    or adj_close <= 0
    or adj_close > close * 5
    or adj_close < close * 0.01
{% endtest %}
