-- Staging model for DataShop customers.
-- Excludes soft-deleted customers (is_deleted = true).
-- Normalises email to lowercase and country_code to uppercase.

with source as (
    select * from {{ source('raw', 'raw_customers') }}
),

cleaned as (
    select
        customer_id,
        lower(trim(email))        as email,
        first_name,
        last_name,
        signup_date::date         as signup_date,
        upper(trim(country_code)) as country_code
    from source
    where trim(is_deleted::varchar) = 'false'
)

select * from cleaned
