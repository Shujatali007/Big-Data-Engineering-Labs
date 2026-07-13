-- Fact table: daily revenue aggregated by date, category, and country.
-- Used by the analytics team for trend analysis and dashboard reporting.

with transactions as (
    select * from {{ ref('stg_transactions') }}
)

select
    txn_date                as sale_date,
    category,
    country_code,
    count(transaction_id)   as order_count,
    round(sum(amount), 2)   as gross_revenue
from transactions
group by 1, 2, 3
order by 1, 2, 3
