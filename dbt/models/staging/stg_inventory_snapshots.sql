with source as (
    select * from {{ source('gold', 'fact_inventory_snapshots') }}
),

staged as (
    select
        snapshot_key,
        event_id,
        event_type,
        correlation_id,
        product_key,
        date_key,
        quantity,
        stock_level,
        reorder_threshold,
        days_of_supply,
        stock_alert_level,
        fill_time_mins,
        is_prescription,
        time_of_day,
        is_peak_hour,
        is_weekend,
        hour_of_day,
        occurred_at
    from source
    where snapshot_key is not null
)

select * from staged
