-- mart_pharmacy_reorder_alerts
-- Latest inventory snapshot per product with reorder urgency analysis
-- Grain: one row per product (latest snapshot only)
-- Sources: fact_inventory_snapshots, dim_product

with snapshots as (
    select
        snapshot_key,
        product_key,
        event_type,
        stock_level,
        reorder_threshold,
        days_of_supply,
        stock_alert_level,
        fill_time_mins,
        is_prescription,
        time_of_day,
        hour_of_day,
        occurred_at
    from {{ source('gold', 'fact_inventory_snapshots') }}
    where product_key is not null
      and stock_level is not null
      and reorder_threshold is not null
),

-- Get latest snapshot per product
latest_snapshots as (
    select
        snapshot_key,
        product_key,
        event_type,
        stock_level,
        reorder_threshold,
        days_of_supply,
        stock_alert_level,
        fill_time_mins,
        is_prescription,
        time_of_day,
        hour_of_day,
        occurred_at,
        row_number() over (
            partition by product_key
            order by occurred_at desc
        ) as rn
    from snapshots
),

-- Historical stats per product for trend analysis
product_history as (
    select
        product_key,
        count(*)                                        as total_snapshots,
        avg(stock_level)                                as avg_stock_level,
        min(stock_level)                                as min_stock_level,
        max(stock_level)                                as max_stock_level,
        avg(days_of_supply)                             as avg_days_of_supply,
        avg(fill_time_mins)                             as avg_fill_time_mins,
        sum(case when stock_alert_level = 'critical' then 1 else 0 end) as critical_count,
        sum(case when stock_alert_level = 'high'     then 1 else 0 end) as high_count,
        sum(case when stock_alert_level = 'medium'   then 1 else 0 end) as medium_count,
        sum(case when stock_alert_level = 'normal'   then 1 else 0 end) as normal_count
    from snapshots
    group by product_key
),

-- Join product dimension
products as (
    select
        product_key,
        product_id,
        category,
        category_group,
        atc_code,
        drug_class,
        is_prescription,
        domain
    from {{ source('gold', 'dim_product') }}
    where domain = 'pharmacy'
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(['p.product_id']) }}          as alert_key,

        -- Product identifiers
        p.product_id,
        p.category,
        p.category_group,
        p.atc_code,
        p.drug_class,
        coalesce(l.is_prescription, p.is_prescription)          as is_prescription,

        -- Current stock state (from latest snapshot)
        l.stock_level                                            as current_stock_level,
        l.reorder_threshold,
        l.stock_level - l.reorder_threshold                     as stock_buffer,
        l.days_of_supply                                        as current_days_of_supply,
        l.stock_alert_level,
        l.fill_time_mins                                        as last_fill_time_mins,
        l.occurred_at                                           as last_snapshot_at,
        l.event_type                                            as last_event_type,

        -- Urgency score using macro (4=critical, 3=high, 2=medium, 1=normal)
        {{ stock_urgency('l.stock_alert_level') }}              as urgency_score,

        -- Recommended action using macro
        {{ stock_recommended_action('l.stock_alert_level') }}   as recommended_action,

        -- Boolean flags
        case
            when l.stock_level <= l.reorder_threshold then true
            else false
        end                                                      as is_below_reorder,
        case
            when l.stock_level <= l.reorder_threshold * 0.5 then true
            else false
        end                                                      as is_critical,

        -- Historical context
        h.total_snapshots,
        round(cast(h.avg_stock_level as decimal(18,2)), 1)      as avg_stock_level,
        h.min_stock_level,
        h.max_stock_level,
        round(cast(h.avg_days_of_supply as decimal(18,2)), 1)   as avg_days_of_supply,
        round(cast(h.avg_fill_time_mins as decimal(18,2)), 1)   as avg_fill_time_mins,
        h.critical_count,
        h.high_count,
        h.medium_count,
        h.normal_count,

        -- Critical frequency rate: how often this product hits critical stock
        round(
            cast(h.critical_count as double) /
            nullif(cast(h.total_snapshots as double), 0),
            4
        )                                                        as critical_frequency_rate,

        -- Stock trend: current vs average (positive = improving, negative = declining)
        round(
            cast(l.stock_level - h.avg_stock_level as decimal(18,2)),
            1
        )                                                        as stock_vs_avg_trend,

        -- Rank within alert level by days of supply (lowest = most urgent)
        rank() over (
            partition by l.stock_alert_level
            order by coalesce(l.days_of_supply, 0) asc
        )                                                        as urgency_rank_within_level,

        -- Overall urgency rank across all products
        rank() over (
            order by
                {{ stock_urgency('l.stock_alert_level') }} desc,
                coalesce(l.days_of_supply, 0) asc
        )                                                        as overall_urgency_rank

    from latest_snapshots l
    join products p on l.product_key = p.product_key
    left join product_history h on l.product_key = h.product_key
    where l.rn = 1
)

select * from final
order by overall_urgency_rank asc
