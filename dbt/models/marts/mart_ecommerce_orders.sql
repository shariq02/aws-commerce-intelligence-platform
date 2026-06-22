with base as (
    select * from {{ ref('int_ecommerce_orders') }}
),

final as (
    select
        transaction_key,
        event_id,
        event_type,
        order_id,
        order_status,
        payment_method,
        is_installment,
        max_installments,
        item_count,
        is_multi_item,
        is_multi_seller,
        fulfilment_bucket,
        fulfilment_time_mins,
        fulfilment_time_days,
        delivery_on_time,
        avg_review_score,
        review_sentiment,
        has_negative_review,
        return_reason,
        customer_id,
        customer_unique_id,
        customer_segment,
        customer_region,
        customer_state_region,
        customer_state,
        city,
        state,
        state_region,
        country,
        full_date,
        year,
        month,
        quarter,
        day_of_week,
        is_weekend,
        -- domain is always ecommerce for this mart
        'ecommerce' as domain,
        total_amount,
        case
            when return_reason is not null then 0
            when total_amount is null then 0
            else total_amount
        end as net_revenue,
        case
            when total_amount is null then 0
            when is_installment = true then total_amount / nullif(max_installments, 0)
            else total_amount
        end as effective_payment_amount,
        occurred_at
    from base
)

select * from final
