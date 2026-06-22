{% macro sla_breach_rate(breached_col, total_col) %}
    {#
        Calculates SLA breach rate as a decimal between 0 and 1.
        Returns null if total is 0 to avoid division by zero.

        Usage:
            {{ sla_breach_rate('sla_breach_count', 'total_dispatches') }}

        Returns DOUBLE PRECISION between 0.0 and 1.0.
        Multiply by 100 at the call site for a percentage.

        Example output: 0.392 = 39.2% breach rate
    #}
    round(
        cast({{ breached_col }} as double) /
        nullif(cast({{ total_col }} as double), 0),
        4
    )
{% endmacro %}
