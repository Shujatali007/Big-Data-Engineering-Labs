-- Staging model for DataShop transactions.
-- Filters to completed transactions with a positive amount.
-- Normalises currency and country_code to uppercase.
-- Casts the raw 'timestamp' column to a DATE for use in mart aggregations.

with source as (
    select * from {{ source('raw', 'raw_transactions') }}
),

cleaned as (
    select
        transaction_id,
        customer_id,
        product_id,
        category,
        amount,
        upper(trim(currency))     as currency,
        upper(trim(country_code)) as country_code,
        status,
        timestamp::date           as txn_date
    from source
    where status = 'completed'
      and amount > 0
)

select * from cleaned
