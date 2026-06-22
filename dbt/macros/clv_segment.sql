{% macro clv_segment(spend_col, p80_col, p50_col) %}
    {#
        Derives a CLV (Customer Lifetime Value) segment based on spend
        relative to the 80th and 50th percentile thresholds passed in.

        Usage:
            {{ clv_segment('total_spend', 'p80_threshold', 'p50_threshold') }}

        Segments:
            high_value  -- top 20% spenders (spend >= p80)
            mid_value   -- middle 30% spenders (spend >= p50 and < p80)
            low_value   -- bottom 50% spenders (spend < p50)
            no_spend    -- customers with null or zero spend

        This macro takes pre-computed percentile columns rather than
        computing them inline so that the thresholds are calculated once
        in a CTE and reused, avoiding repeated window function evaluation.
    #}
    case
        when {{ spend_col }} is null or {{ spend_col }} = 0
            then 'no_spend'
        when {{ spend_col }} >= {{ p80_col }}
            then 'high_value'
        when {{ spend_col }} >= {{ p50_col }}
            then 'mid_value'
        else
            'low_value'
    end
{% endmacro %}
