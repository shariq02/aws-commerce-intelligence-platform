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

-- Parse occurred_at as fallback when date_key is null
-- occurred_at is ISO 8601 format for marketplace events
parsed as (
    select
        *,
        try_cast(occurred_at as timestamp) as occurred_ts
    from performance
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
        -- Use dim_date if date_key exists, fall back to occurred_at parsing
        coalesce(d.full_date, cast(p.occurred_ts as date)) as full_date,
        coalesce(d.year, year(p.occurred_ts)) as year,
        coalesce(d.month, month(p.occurred_ts)) as month,
        coalesce(d.quarter, quarter(p.occurred_ts)) as quarter,
        coalesce(d.day_of_week, dayofweek(p.occurred_ts)) as day_of_week,
        coalesce(d.is_weekend,
            case when dayofweek(p.occurred_ts) in (1, 7) then true else false end
        ) as is_weekend
    from parsed p
    left join dim_seller s on p.seller_key = s.seller_key
    left join dim_date d on p.date_key = d.date_key
    where coalesce(d.year, year(p.occurred_ts)) is not null
)

select * from joined
