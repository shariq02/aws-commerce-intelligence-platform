with source as (
    select * from {{ source('gold', 'dim_customer') }}
),

staged as (
    select
        customer_key,
        customer_id,
        customer_unique_id,
        customer_segment,
        region,
        state_region,
        customer_state,
        effective_date,
        expiry_date,
        is_current
    from source
    where customer_key is not null
    and is_current = true
)

select * from staged
