-- Dimension table: one row per active customer, with lifetime order statistics.
-- Customers with no completed transactions have number_of_orders = 0
-- and lifetime_value = 0.0 via COALESCE.

with customers as (
    select * from {{ ref('stg_customers') }}
),

transactions as (
    select * from {{ ref('stg_transactions') }}
),

customer_orders as (
    select
        customer_id,
        min(txn_date)          as first_order_date,
        max(txn_date)          as most_recent_order_date,
        count(transaction_id)  as number_of_orders,
        round(sum(amount), 2)  as lifetime_value
    from transactions
    group by 1
)

select
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.signup_date,
    c.country_code,
    coalesce(co.first_order_date, null)       as first_order_date,
    coalesce(co.most_recent_order_date, null) as most_recent_order_date,
    coalesce(co.number_of_orders, 0)          as number_of_orders,
    coalesce(co.lifetime_value, 0.0)          as lifetime_value
from customers c
left join customer_orders co using (customer_id)
