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
        i.snapshot_key,
        i.event_id,
        i.event_type,
        i.correlation_id,
        i.quantity,
        i.stock_level,
        i.reorder_threshold,
        i.days_of_supply,
        i.stock_alert_level,
        i.fill_time_mins,
        i.is_prescription,
        i.time_of_day,
        i.is_peak_hour,
        i.is_weekend,
        i.hour_of_day,
        i.occurred_at,
        p.product_id,
        p.category,
        p.category_group,
        p.atc_code,
        p.drug_class,
        p.domain,
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.day_of_week
    from inventory i
    left join dim_product p on i.product_key = p.product_key
    left join dim_date d on i.date_key = d.date_key
)

select * from joined
