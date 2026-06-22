-- mart_seller_leaderboard
-- Seller performance leaderboard with rankings across revenue, SLA compliance, volume
-- Grain: one row per seller (current sellers only)
-- Sources: fact_seller_performance, dim_seller

with dispatch_events as (
    select
        seller_key,
        seller_tier,
        price,
        freight_value,
        dispatch_time_days,
        dispatch_speed_bucket,
        is_sla_breached,
        sla_threshold_mins
    from {{ source('gold', 'fact_seller_performance') }}
    where event_type = 'seller.order.dispatched'
      and seller_key is not null
      and price is not null
),

-- Aggregate per seller
seller_stats as (
    select
        seller_key,
        max(seller_tier)                                                         as seller_tier,
        count(*)                                                                 as total_orders,
        sum(price)                                                               as total_revenue,
        avg(price)                                                               as avg_order_value,
        sum(freight_value)                                                       as total_freight,
        avg(freight_value)                                                       as avg_freight_value,
        avg(dispatch_time_days)                                                  as avg_dispatch_days,
        min(dispatch_time_days)                                                  as min_dispatch_days,
        max(dispatch_time_days)                                                  as max_dispatch_days,
        sum(case when is_sla_breached = true  then 1 else 0 end)                as sla_breach_count,
        sum(case when is_sla_breached = false then 1 else 0 end)                as sla_compliant_count,
        avg(sla_threshold_mins)                                                  as avg_sla_threshold_mins,
        avg(case dispatch_speed_bucket
            when 'express'  then 4
            when 'fast'     then 3
            when 'standard' then 2
            when 'slow'     then 1
            else 0
        end)                                                                     as avg_dispatch_speed_score,
        sum(case when dispatch_speed_bucket = 'express'  then 1 else 0 end)     as express_count,
        sum(case when dispatch_speed_bucket = 'fast'     then 1 else 0 end)     as fast_count,
        sum(case when dispatch_speed_bucket = 'standard' then 1 else 0 end)     as standard_count,
        sum(case when dispatch_speed_bucket = 'slow'     then 1 else 0 end)     as slow_count
    from dispatch_events
    group by seller_key
),

-- Join seller dimension
sellers as (
    select
        seller_key,
        seller_id,
        seller_tier,
        seller_city,
        seller_state,
        seller_region
    from (
        select
            seller_key,
            seller_id,
            seller_tier,
            seller_city,
            seller_state,
            seller_region,
            row_number() over (
                partition by seller_id
                order by seller_key asc
            ) as rn
        from {{ source('gold', 'dim_seller') }}
        where is_current = true
    )
    where rn = 1
),

-- Compute rankings
ranked as (
    select
        s.seller_key,
        s.seller_id,
        coalesce(st.seller_tier, s.seller_tier)             as seller_tier,
        s.seller_city,
        s.seller_state,
        s.seller_region,

        -- Transaction metrics
        coalesce(st.total_orders, 0)                        as total_orders,
        round(cast(coalesce(st.total_revenue, 0) as decimal(18,2)), 2) as total_revenue,
        round(cast(coalesce(st.avg_order_value, 0) as decimal(18,2)), 2) as avg_order_value,
        round(cast(coalesce(st.total_freight, 0) as decimal(18,2)), 2) as total_freight,
        round(cast(coalesce(st.avg_freight_value, 0) as decimal(18,2)), 2) as avg_freight_value,

        -- Dispatch metrics
        round(cast(coalesce(st.avg_dispatch_days, 0) as decimal(18,2)), 2) as avg_dispatch_days,
        round(cast(coalesce(st.min_dispatch_days, 0) as decimal(18,2)), 2) as min_dispatch_days,
        round(cast(coalesce(st.max_dispatch_days, 0) as decimal(18,2)), 2) as max_dispatch_days,
        round(cast(coalesce(st.avg_dispatch_speed_score, 0) as decimal(18,2)), 2) as avg_dispatch_speed_score,

        -- Speed bucket counts
        coalesce(st.express_count, 0)                       as express_count,
        coalesce(st.fast_count, 0)                          as fast_count,
        coalesce(st.standard_count, 0)                      as standard_count,
        coalesce(st.slow_count, 0)                          as slow_count,

        -- SLA metrics using macro
        coalesce(st.sla_breach_count, 0)                    as sla_breach_count,
        coalesce(st.sla_compliant_count, 0)                 as sla_compliant_count,
        {{ sla_breach_rate('coalesce(st.sla_breach_count, 0)', 'coalesce(st.total_orders, 0)') }} as sla_breach_rate,
        round(
            1 - {{ sla_breach_rate('coalesce(st.sla_breach_count, 0)', 'coalesce(st.total_orders, 0)') }},
            4
        )                                                   as sla_compliance_rate,
        round(cast(coalesce(st.avg_sla_threshold_mins, 0) as decimal(18,2)), 2) as avg_sla_threshold_mins,

        -- Freight burden rate
        round(
            cast(coalesce(st.avg_freight_value, 0) as double) /
            nullif(cast(coalesce(st.avg_order_value, 0) as double), 0),
            4
        )                                                   as avg_freight_burden_rate,

        -- Rankings (lower rank number = better performance)
        rank() over (order by coalesce(st.total_revenue, 0) desc)       as rank_by_revenue,
        rank() over (order by
            round(
                1 - {{ sla_breach_rate('coalesce(st.sla_breach_count, 0)', 'coalesce(st.total_orders, 0)') }},
                4
            ) desc nulls last
        )                                                               as rank_by_sla_compliance,
        rank() over (order by coalesce(st.total_orders, 0) desc)        as rank_by_volume,
        rank() over (order by coalesce(st.avg_dispatch_speed_score, 0) desc) as rank_by_speed

    from sellers s
    left join seller_stats st on s.seller_key = st.seller_key
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(['seller_id']) }}          as leaderboard_key,
        *,
        -- Overall rank: average of revenue, compliance, volume ranks
        round(
            cast(rank_by_revenue + rank_by_sla_compliance + rank_by_volume as double) / 3.0,
            1
        )                                                   as overall_score,
        rank() over (
            order by
                rank_by_revenue + rank_by_sla_compliance + rank_by_volume asc
        )                                                   as overall_rank
    from ranked
)

select * from final
order by overall_rank asc
