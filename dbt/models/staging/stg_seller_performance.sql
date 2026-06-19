with source as (
    select * from {{ source('gold', 'fact_seller_performance') }}
),

staged as (
    select
        performance_key,
        event_id,
        event_type,
        correlation_id,
        seller_key,
        date_key,
        seller_tier,
        seller_state,
        seller_region,
        product_id,
        category,
        category_group,
        price,
        freight_value,
        dispatch_time_mins,
        dispatch_time_days,
        sla_threshold_mins,
        is_sla_breached,
        dispatch_speed_bucket,
        old_price,
        new_price,
        change_pct,
        occurred_at
    from source
    where performance_key is not null
)

select * from staged
