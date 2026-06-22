with inventory as (
    select * from {{ ref('stg_inventory_snapshots') }}
),

dim_product as (
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
),

-- occurred_at has two formats:
-- batch:     M/D/YYYY H:MMT08  e.g. 1/5/2014 8:00T08
-- streaming: ISO 8601           e.g. 2026-01-15T10:30:00
-- Parse both using TO_TIMESTAMP with fallback

parsed as (
    select
        *,
        coalesce(
            -- Try ISO 8601 first (streaming events)
            try_cast(occurred_at as timestamp),
            -- Try M/D/YYYY H:MM format (batch events - strip trailing T08 suffix)
            to_timestamp(
                regexp_replace(occurred_at, 'T[0-9]+$', ''),
                'M/d/yyyy H:mm'
            )
        ) as occurred_ts
    from inventory
),

joined as (
    select
        p.snapshot_key,
        p.event_id,
        p.event_type,
        p.correlation_id,
        p.quantity,
        p.stock_level,
        p.reorder_threshold,
        p.days_of_supply,
        p.stock_alert_level,
        p.fill_time_mins,
        p.is_prescription,
        p.time_of_day,
        p.is_peak_hour,
        p.is_weekend,
        p.hour_of_day,
        p.occurred_at,
        pr.product_id,
        pr.category,
        pr.category_group,
        pr.atc_code,
        pr.drug_class,
        pr.domain,
        cast(p.occurred_ts as date) as full_date,
        year(p.occurred_ts) as year,
        month(p.occurred_ts) as month,
        quarter(p.occurred_ts) as quarter,
        dayofweek(p.occurred_ts) as day_of_week
    from parsed p
    left join dim_product pr on p.product_key = pr.product_key
    where p.occurred_ts is not null
)

select * from joined
