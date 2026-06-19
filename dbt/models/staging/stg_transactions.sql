with source as (
    select * from {{ source('gold', 'fact_transactions') }}
),

staged as (
    select
        transaction_key,
        event_id,
        event_type,
        order_id,
        customer_key,
        date_key,
        geo_key,
        total_amount,
        payment_method,
        is_installment,
        max_installments,
        order_status,
        item_count,
        is_multi_item,
        is_multi_seller,
        fulfilment_time_mins,
        fulfilment_time_days,
        fulfilment_bucket,
        delivery_on_time,
        avg_review_score,
        review_sentiment,
        has_negative_review,
        return_reason,
        occurred_at
    from source
    where transaction_key is not null
)

select * from staged
