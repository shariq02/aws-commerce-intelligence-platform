-- mart_domain_anomaly_rates
-- Daily event count per domain with statistical spike detection
-- Grain: one row per domain per day
-- Sources: agg_daily_domain_metrics
-- Serves use cases: CD-02 (cross-domain anomaly comparison),
--                    AD-01 (order volume spike detection, batch reframe)

with daily_raw as (
    select
        domain,
        metric_date,
        event_count,
        total_value
    from {{ source('gold', 'agg_daily_domain_metrics') }}
    where domain is not null
      and metric_date is not null
),

-- agg_daily_domain_metrics is grain (metric_date, domain, event_type) --
-- e.g. ecommerce has order.placed, order.fulfilled, order.returned as
-- separate rows for the same date. Roll up to (domain, metric_date) here
-- since this mart needs one row per domain per day for anomaly detection.
daily as (
    select
        domain,
        metric_date,
        sum(event_count) as event_count,
        sum(total_value) as total_value
    from daily_raw
    group by domain, metric_date
),

-- Rolling 30-day mean and stddev per domain, computed per row using a
-- trailing window (excludes the current day itself from its own baseline)
stats as (
    select
        domain,
        metric_date,
        event_count,
        total_value,
        avg(event_count) over (
            partition by domain
            order by metric_date
            rows between 30 preceding and 1 preceding
        )                                                          as rolling_mean,
        stddev(event_count) over (
            partition by domain
            order by metric_date
            rows between 30 preceding and 1 preceding
        )                                                          as rolling_std,
        count(*) over (
            partition by domain
            order by metric_date
            rows between 30 preceding and 1 preceding
        )                                                          as trailing_days_available
    from daily
),

final as (
    select
        -- Surrogate key using macro
        {{ generate_surrogate_key(["domain", "cast(metric_date as string)"]) }}
                                                                     as anomaly_key,

        domain,
        metric_date,
        event_count,
        total_value,

        round(cast(rolling_mean as decimal(18,2)), 2)              as rolling_mean,
        round(cast(rolling_std as decimal(18,2)), 2)                as rolling_std,
        trailing_days_available,

        -- spike magnitude: how many std deviations above the mean
        round(
            cast(
                (event_count - rolling_mean) /
                nullif(rolling_std, 0)
            as decimal(18,2)),
            2
        )                                                            as spike_magnitude,

        -- spike flag: only evaluated once at least 7 days of trailing history exist
        -- avoids false positives on the first week of data with no baseline
        case
            when trailing_days_available < 7 then false
            when rolling_std is null or rolling_std = 0 then false
            when event_count > rolling_mean + (2 * rolling_std) then true
            else false
        end                                                          as is_spike,

        case
            when trailing_days_available < 7 then false
            when rolling_std is null or rolling_std = 0 then false
            when event_count < rolling_mean - (2 * rolling_std) then true
            else false
        end                                                          as is_drop

    from stats
)

select * from final
order by metric_date desc, domain asc
