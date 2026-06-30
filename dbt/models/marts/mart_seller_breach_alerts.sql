-- mart_seller_breach_alerts
-- Daily SLA breach rate per seller, flagging sellers above 20% breach threshold
-- Grain: one row per seller per day (dispatch events only)
-- Sources: fact_seller_performance (via stg_seller_performance)
-- Serves use case: AD-03 (batch reframe of real-time session-window breach alert)

with dispatches as (
    select
        seller_key,
        seller_tier,
        seller_state,
        seller_region,
        is_sla_breached,
        cast(occurred_at as date) as dispatch_date
    from {{ ref('stg_seller_performance') }}
    where seller_key is not null
      and occurred_at is not null
      -- filter to dispatch events only -- is_sla_breached is structurally
      -- null on listing.created and price.updated events
      and is_sla_breached is not null
),

daily_seller_stats as (
    select
        seller_key,
        max(seller_tier)                                            as seller_tier,
        max(seller_state)                                           as seller_state,
        max(seller_region)                                          as seller_region,
        dispatch_date,
        count(*)                                                    as total_dispatches,
        sum(case when is_sla_breached = true then 1 else 0 end)     as breach_count
    from dispatches
    group by seller_key, dispatch_date
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(["seller_key", "cast(dispatch_date as string)"]) }}
                                                                     as breach_alert_key,

        seller_key,
        coalesce(seller_tier, 'unknown')                            as seller_tier,
        seller_state,
        coalesce(seller_region, 'unknown')                          as seller_region,
        dispatch_date,
        total_dispatches,
        breach_count,

        -- breach rate using existing macro
        {{ sla_breach_rate('breach_count', 'total_dispatches') }}   as breach_rate,

        -- flag sellers above 20% daily breach rate
        case
            when {{ sla_breach_rate('breach_count', 'total_dispatches') }} > 0.20
                then true
            else false
        end                                                          as is_flagged,

        case
            when {{ sla_breach_rate('breach_count', 'total_dispatches') }} > 0.50
                then 'severe'
            when {{ sla_breach_rate('breach_count', 'total_dispatches') }} > 0.20
                then 'warning'
            else 'normal'
        end                                                          as breach_severity

    from daily_seller_stats
)

select * from final
order by dispatch_date desc, breach_rate desc
