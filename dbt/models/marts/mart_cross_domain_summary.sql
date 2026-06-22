with ecommerce_summary as (
    select
        year,
        month,
        count(transaction_key)                                           as total_transactions,
        sum(net_revenue)                                                 as total_net_revenue,
        avg(total_amount)                                                as avg_order_value,
        sum(case when return_reason is not null then 1 else 0 end)       as total_returns,
        sum(case when delivery_on_time = true then 1 else 0 end)         as on_time_deliveries,
        avg(avg_review_score)                                            as avg_review_score
    from {{ ref('mart_ecommerce_orders') }}
    where year is not null
      and month is not null
    group by year, month
),

pharmacy_summary as (
    select
        year,
        month,
        count(snapshot_key)                                              as total_dispensing_events,
        avg(fill_time_mins)                                              as avg_fill_time_mins,
        sum(case when requires_action = true then 1 else 0 end)          as critical_stock_events,
        sum(case when is_prescription = true then 1 else 0 end)          as rx_events,
        avg(days_of_supply)                                              as avg_days_of_supply
    from {{ ref('mart_pharmacy_dispensing') }}
    where year is not null
      and month is not null
    group by year, month
),

marketplace_summary as (
    select
        year,
        month,
        count(case when event_type = 'seller.order.dispatched' then performance_key end) as total_dispatch_events,
        sum(gross_revenue)                                               as total_gross_revenue,
        sum(net_revenue)                                                 as total_net_revenue,
        avg(dispatch_time_days)                                          as avg_dispatch_days,
        sum(case when is_sla_breached = true then 1 else 0 end)          as sla_breaches
    from {{ ref('mart_marketplace_performance') }}
    where year is not null
      and month is not null
    group by year, month
),

-- Union all year/month combinations across all 3 domains
all_periods as (
    select year, month from ecommerce_summary
    union
    select year, month from pharmacy_summary
    union
    select year, month from marketplace_summary
),

final as (
    select
        ap.year,
        ap.month,
        -- COALESCE to 0 for periods where a domain has no data
        -- (left join produces nulls for months only in one domain)
        coalesce(e.total_transactions, 0)       as ecommerce_transactions,
        coalesce(e.total_net_revenue, 0)        as ecommerce_net_revenue,
        e.avg_order_value                       as ecommerce_avg_order_value,
        coalesce(e.total_returns, 0)            as ecommerce_returns,
        coalesce(e.on_time_deliveries, 0)       as ecommerce_on_time_deliveries,
        e.avg_review_score                      as ecommerce_avg_review_score,
        coalesce(p.total_dispensing_events, 0)  as pharmacy_dispensing_events,
        p.avg_fill_time_mins                    as pharmacy_avg_fill_time,
        coalesce(p.critical_stock_events, 0)    as pharmacy_critical_stock_events,
        coalesce(p.rx_events, 0)                as pharmacy_rx_events,
        p.avg_days_of_supply                    as pharmacy_avg_days_of_supply,
        coalesce(m.total_dispatch_events, 0)    as marketplace_dispatch_events,
        coalesce(m.total_gross_revenue, 0)      as marketplace_gross_revenue,
        coalesce(m.total_net_revenue, 0)        as marketplace_net_revenue,
        m.avg_dispatch_days                     as marketplace_avg_dispatch_days,
        coalesce(m.sla_breaches, 0)             as marketplace_sla_breaches
    from all_periods ap
    left join ecommerce_summary e
        on ap.year = e.year and ap.month = e.month
    left join pharmacy_summary p
        on ap.year = p.year and ap.month = p.month
    left join marketplace_summary m
        on ap.year = m.year and ap.month = m.month
)

select * from final
order by year desc, month desc
