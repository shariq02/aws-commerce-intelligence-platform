with base as (
    select * from {{ ref('int_pharmacy_dispensing') }}
),

final as (
    select
        snapshot_key,
        event_id,
        event_type,
        correlation_id,
        product_id,
        category,
        category_group,
        atc_code,
        drug_class,
        is_prescription,
        domain,
        stock_alert_level,
        time_of_day,
        is_peak_hour,
        is_weekend,
        hour_of_day,
        full_date,
        year,
        month,
        quarter,
        day_of_week,
        stock_level,
        reorder_threshold,
        days_of_supply,
        quantity,
        fill_time_mins,
        case
            when stock_level <= reorder_threshold then true
            else false
        end as is_below_reorder,
        case
            when stock_alert_level in ('critical', 'high') then true
            else false
        end as requires_action,
        stock_level - reorder_threshold as stock_buffer,
        occurred_at
    from base
)

select * from final
