with base as (
    select * from {{ ref('int_marketplace_performance') }}
),

final as (
    select
        performance_key,
        event_id,
        event_type,
        correlation_id,
        seller_id,
        seller_tier,
        seller_state,
        seller_region,
        seller_city,
        product_id,
        category,
        category_group,
        dispatch_speed_bucket,
        full_date,
        year,
        month,
        quarter,
        day_of_week,
        is_weekend,
        -- domain is always marketplace for this mart
        'marketplace' as domain,
        price,
        freight_value,
        dispatch_time_mins,
        dispatch_time_days,
        sla_threshold_mins,
        -- is_sla_breached only meaningful for dispatch events
        -- listing.created and price.updated events do not have SLA fields
        case
            when event_type = 'seller.order.dispatched' then is_sla_breached
            else null
        end as is_sla_breached,
        old_price,
        new_price,
        change_pct,
        case
            when is_sla_breached = true
                then dispatch_time_days - (sla_threshold_mins / 1440.0)
            else 0
        end as sla_overrun_days,
        case
            when freight_value / nullif(price, 0) >= 0.3 then 'high'
            when freight_value / nullif(price, 0) >= 0.15 then 'medium'
            when price is not null then 'low'
            else null
        end as freight_burden,
        -- gross_revenue and net_revenue only for events with price
        -- listing.created has price, price.updated has old/new_price, dispatch has price
        -- null for event types where price is not in payload
        price as gross_revenue,
        case
            when price is not null and freight_value is not null
                then price - freight_value
            when price is not null
                then price
            else null
        end as net_revenue,
        occurred_at
    from base
)

select * from final
