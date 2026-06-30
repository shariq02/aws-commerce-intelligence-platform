-- mart_hourly_transaction_volume
-- Transaction count per hour of day per domain
-- Grain: one row per domain per hour (0-23)
-- Sources: stg_transactions (ecommerce), stg_inventory_snapshots (pharmacy),
--          stg_seller_performance (marketplace)
-- Serves use case: CD-04 (batch reframe of real-time hourly volume)

with ecommerce_hourly as (
    select
        'ecommerce' as domain,
        hour(try_to_timestamp(occurred_at)) as hour_of_day,
        count(*) as event_count
    from {{ ref('stg_transactions') }}
    where occurred_at is not null
      and try_to_timestamp(occurred_at) is not null
    group by hour(try_to_timestamp(occurred_at))
),

pharmacy_hourly as (
    select
        'pharmacy' as domain,
        hour_of_day,
        count(*) as event_count
    from {{ ref('stg_inventory_snapshots') }}
    where hour_of_day is not null
    group by hour_of_day
),

marketplace_hourly as (
    select
        'marketplace' as domain,
        hour(try_to_timestamp(occurred_at)) as hour_of_day,
        count(*) as event_count
    from {{ ref('stg_seller_performance') }}
    where occurred_at is not null
      and try_to_timestamp(occurred_at) is not null
    group by hour(try_to_timestamp(occurred_at))
),

unioned as (
    select * from ecommerce_hourly
    union all
    select * from pharmacy_hourly
    union all
    select * from marketplace_hourly
),

-- Total events per domain, used to compute each hour's share of domain total
domain_totals as (
    select
        domain,
        sum(event_count) as domain_total
    from unioned
    group by domain
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(["u.domain", "cast(u.hour_of_day as string)"]) }}
                                                                     as hourly_volume_key,

        u.domain,
        u.hour_of_day,
        u.event_count,
        t.domain_total,

        round(
            cast(u.event_count as double) /
            nullif(cast(t.domain_total as double), 0) * 100,
            2
        )                                                            as pct_of_domain_total,

        -- platform load classification relative to other hours in same domain
        case
            when u.hour_of_day between 6 and 11 then 'morning'
            when u.hour_of_day between 12 and 17 then 'afternoon'
            when u.hour_of_day between 18 and 22 then 'evening'
            else 'night'
        end                                                          as time_of_day_bucket

    from unioned u
    left join domain_totals t
        on u.domain = t.domain
)

select * from final
order by domain asc, hour_of_day asc
