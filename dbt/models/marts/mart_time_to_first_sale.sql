-- mart_time_to_first_sale
-- Time elapsed between listing.created and first seller.order.dispatched per listing
-- Grain: one row per listing that has both a listing.created and a dispatch event
-- Sources: fact_seller_performance (via stg_seller_performance)

with events as (
    select
        correlation_id,
        event_type,
        seller_key,
        seller_tier,
        category,
        category_group,
        occurred_at
    from {{ ref('stg_seller_performance') }}
    where correlation_id is not null
      and event_type in ('listing.created', 'seller.order.dispatched')
),

listings as (
    select
        correlation_id,
        seller_key,
        seller_tier,
        category,
        category_group,
        -- occurred_at is stored as a string with microseconds and a
        -- timezone offset (e.g. 2026-06-29T20:48:34.751209+00:00) which
        -- unix_timestamp() cannot parse. try_to_timestamp handles it and
        -- returns null instead of failing on malformed rows.
        try_to_timestamp(occurred_at) as listed_at
    from events
    where event_type = 'listing.created'
),

-- First dispatch per listing (a listing can in theory dispatch more than once
-- across reruns of the synthetic data -- take the earliest)
first_dispatch as (
    select
        correlation_id,
        min(try_to_timestamp(occurred_at)) as first_dispatched_at
    from events
    where event_type = 'seller.order.dispatched'
    group by correlation_id
),

joined as (
    select
        l.correlation_id,
        l.seller_key,
        l.seller_tier,
        l.category,
        l.category_group,
        l.listed_at,
        d.first_dispatched_at,
        -- minutes and days between listing and first dispatch
        (unix_timestamp(d.first_dispatched_at) - unix_timestamp(l.listed_at)) / 60.0
            as time_to_first_sale_mins,
        (unix_timestamp(d.first_dispatched_at) - unix_timestamp(l.listed_at)) / 86400.0
            as time_to_first_sale_days
    from listings l
    inner join first_dispatch d
        on l.correlation_id = d.correlation_id
    where l.listed_at is not null
      and d.first_dispatched_at is not null
    -- guard against any out-of-order synthetic data where dispatch precedes listing
      and d.first_dispatched_at >= l.listed_at
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(['correlation_id']) }}           as time_to_sale_key,

        correlation_id                                              as listing_id,
        seller_key,
        coalesce(seller_tier, 'unknown')                           as seller_tier,
        coalesce(category, 'unknown')                              as category,
        coalesce(category_group, 'unknown')                        as category_group,

        listed_at,
        first_dispatched_at,

        round(cast(time_to_first_sale_mins as decimal(18,2)), 2)   as time_to_first_sale_mins,
        round(cast(time_to_first_sale_days as decimal(18,2)), 2)   as time_to_first_sale_days,

        -- speed bucket consistent with dispatch_speed_bucket conventions elsewhere
        case
            when time_to_first_sale_days <= 1 then 'express'
            when time_to_first_sale_days <= 3 then 'fast'
            when time_to_first_sale_days <= 7 then 'standard'
            else 'slow'
        end                                                         as first_sale_speed_bucket

    from joined
)

select * from final
order by time_to_first_sale_days asc
