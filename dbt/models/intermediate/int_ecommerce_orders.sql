with transactions as (
    select * from {{ ref('stg_transactions') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

dim_geo as (
    select
        geo_key,
        city,
        state,
        state_region,
        country,
        region
    from {{ source('gold', 'dim_geography') }}
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
        t.transaction_key,
        t.event_id,
        t.event_type,
        t.order_id,
        t.total_amount,
        t.payment_method,
        t.is_installment,
        t.max_installments,
        t.order_status,
        t.item_count,
        t.is_multi_item,
        t.is_multi_seller,
        t.fulfilment_time_mins,
        t.fulfilment_time_days,
        t.fulfilment_bucket,
        t.delivery_on_time,
        t.avg_review_score,
        t.review_sentiment,
        t.has_negative_review,
        t.return_reason,
        t.occurred_at,
        c.customer_id,
        c.customer_unique_id,
        c.customer_segment,
        c.region as customer_region,
        c.state_region as customer_state_region,
        c.customer_state,
        g.city,
        g.state,
        g.state_region,
        g.country,
        d.full_date,
        d.year,
        d.month,
        d.quarter,
        d.day_of_week,
        d.is_weekend
    from transactions t
    left join customers c on t.customer_key = c.customer_key
    left join dim_geo g on t.geo_key = g.geo_key
    left join dim_date d on t.date_key = d.date_key
)

select * from joined
