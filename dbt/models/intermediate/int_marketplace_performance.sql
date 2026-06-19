with performance as (
    select * from {{ ref('stg_seller_performance') }}
),

dim_seller as (
    select
        seller_key,
        seller_id,
        seller_tier,
        seller_city,
        seller_state,
        seller_region
    from {{ source('gold', 'dim_seller') }}
    where is_current = true
),

dim_date as (
    select
        date_key,
        full_date,
        year,
        month,
        quarter,
        day_of_week,
        is_weekend
    from {{ source('gold', 'dim_date') }}
),

joined as (
    select
        p.performance_key,
        p.event_id,
        p.event_type,
        p.correlation_id,
        p.seller_tier,
        p.seller_state,
        p.seller_region,
        p.product_id,
        p.category,
        p.category_group,
        p.price,
        p.freight_value,
        p.dispatch_time_mins,
        p.dispatch_time_days,
        p.sla_threshold_mins,
        p.is_sla_breached,
        p.dispatch_speed_bucket,
        p.old_price,
        p.new_price,
        p.change_pct,
        p.occurred_at,
        s.seller_id,
        s.seller_city,
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.day_of_week,
        d.is_weekend
    from performance p
    left join dim_seller s on p.seller_key = s.seller_key
    left join dim_date d on p.date_key = d.date_key
)

select * from joined
