with ecommerce_summary as (
    select
        year,
        month,
        count(transaction_key) as total_transactions,
        sum(net_revenue) as total_net_revenue,
        avg(total_amount) as avg_order_value,
        sum(case when return_reason is not null then 1 else 0 end) as total_returns,
        sum(case when delivery_on_time = true then 1 else 0 end) as on_time_deliveries,
        avg(avg_review_score) as avg_review_score
    from {{ ref('mart_ecommerce_orders') }}
    group by year, month
),

pharmacy_summary as (
    select
        year,
        month,
        count(snapshot_key) as total_dispensing_events,
        avg(fill_time_mins) as avg_fill_time_mins,
        sum(case when requires_action = true then 1 else 0 end) as critical_stock_events,
        sum(case when is_prescription = true then 1 else 0 end) as rx_events,
        avg(days_of_supply) as avg_days_of_supply
    from {{ ref('mart_pharmacy_dispensing') }}
    group by year, month
),

marketplace_summary as (
    select
        year,
        month,
        count(performance_key) as total_dispatch_events,
        sum(gross_revenue) as total_gross_revenue,
        sum(net_revenue) as total_net_revenue,
        avg(dispatch_time_days) as avg_dispatch_days,
        sum(case when is_sla_breached = true then 1 else 0 end) as sla_breaches
    from {{ ref('mart_marketplace_performance') }}
    group by year, month
),

final as (
    select
        e.year,
        e.month,
        e.total_transactions as ecommerce_transactions,
        e.total_net_revenue as ecommerce_net_revenue,
        e.avg_order_value as ecommerce_avg_order_value,
        e.total_returns as ecommerce_returns,
        e.on_time_deliveries as ecommerce_on_time_deliveries,
        e.avg_review_score as ecommerce_avg_review_score,
        p.total_dispensing_events as pharmacy_dispensing_events,
        p.avg_fill_time_mins as pharmacy_avg_fill_time,
        p.critical_stock_events as pharmacy_critical_stock_events,
        p.rx_events as pharmacy_rx_events,
        p.avg_days_of_supply as pharmacy_avg_days_of_supply,
        m.total_dispatch_events as marketplace_dispatch_events,
        m.total_gross_revenue as marketplace_gross_revenue,
        m.total_net_revenue as marketplace_net_revenue,
        m.avg_dispatch_days as marketplace_avg_dispatch_days,
        m.sla_breaches as marketplace_sla_breaches
    from ecommerce_summary e
    left join pharmacy_summary p on e.year = p.year and e.month = p.month
    left join marketplace_summary m on e.year = m.year and e.month = m.month
)

select * from final
