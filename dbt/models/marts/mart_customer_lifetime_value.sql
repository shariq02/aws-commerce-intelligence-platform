-- mart_customer_lifetime_value
-- Customer lifetime value mart combining transaction history with customer dimension
-- Grain: one row per customer (current record only)
-- Sources: fact_transactions, dim_customer, dim_date

with transactions as (
    select
        t.customer_key,
        t.total_amount,
        t.return_reason,
        t.order_id,
        t.event_type,
        d.full_date,
        d.year,
        d.month
    from {{ source('gold', 'fact_transactions') }} t
    left join {{ source('gold', 'dim_date') }} d
        on t.date_key = d.date_key
    where t.event_type = 'order.placed'
      and t.customer_key is not null
),

-- Aggregate transaction history per customer
customer_orders as (
    select
        customer_key,
        count(order_id)                                              as total_orders,
        sum(total_amount)                                            as total_spend,
        avg(total_amount)                                            as avg_order_value,
        min(full_date)                                               as first_order_date,
        max(full_date)                                               as last_order_date,
        sum(case when return_reason is not null then 1 else 0 end)   as total_returns,
        count(distinct year || '-' || lpad(cast(month as string), 2, '0')) as active_months
    from transactions
    where total_amount is not null
    group by customer_key
),

-- Compute spend percentiles for CLV segmentation
spend_percentiles as (
    select
        percentile_cont(0.80) within group (order by total_spend) as p80_spend,
        percentile_cont(0.50) within group (order by total_spend) as p50_spend
    from customer_orders
),

-- Join customer dimension
customers as (
    select
        customer_key,
        customer_id,
        customer_unique_id,
        customer_segment,
        region,
        state_region,
        customer_state
    from {{ source('gold', 'dim_customer') }}
    where is_current = true
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(['c.customer_id']) }}              as clv_key,

        c.customer_id,
        c.customer_unique_id,
        c.customer_segment,
        c.region,
        c.state_region,
        c.customer_state,

        -- Transaction metrics
        coalesce(o.total_orders, 0)                                  as total_orders,
        coalesce(o.total_spend, 0)                                   as total_spend,
        round(cast(coalesce(o.avg_order_value, 0) as decimal(18,2)), 2) as avg_order_value,
        coalesce(o.total_returns, 0)                                 as total_returns,

        -- Return rate
        round(
            cast(coalesce(o.total_returns, 0) as double) /
            nullif(cast(coalesce(o.total_orders, 0) as double), 0),
            4
        )                                                            as return_rate,

        -- Date metrics
        o.first_order_date,
        o.last_order_date,
        datediff(
            coalesce(o.last_order_date, current_date()),
            coalesce(o.first_order_date, current_date())
        )                                                            as customer_tenure_days,

        -- Order frequency
        coalesce(o.active_months, 0)                                 as active_months,
        round(
            cast(coalesce(o.total_orders, 0) as double) /
            nullif(cast(coalesce(o.active_months, 0) as double), 0),
            2
        )                                                            as orders_per_active_month,

        -- Days since last order
        datediff(current_date(), o.last_order_date)                  as days_since_last_order,

        -- Churn flag: no orders in last 180 days
        case
            when o.last_order_date is null then true
            when datediff(current_date(), o.last_order_date) > 180 then true
            else false
        end                                                          as is_churned,

        -- CLV segment using macro with pre-computed percentiles
        {{ clv_segment('o.total_spend', 'p.p80_spend', 'p.p50_spend') }} as clv_segment,

        -- Spend percentile thresholds (for reference)
        round(cast(p.p80_spend as decimal(18,2)), 2)                 as p80_spend_threshold,
        round(cast(p.p50_spend as decimal(18,2)), 2)                 as p50_spend_threshold

    from customers c
    left join customer_orders o
        on c.customer_key = o.customer_key
    cross join spend_percentiles p
)

select * from final
order by total_spend desc nulls last
