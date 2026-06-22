{% macro generate_surrogate_key(columns) %}
    {#
        Generates a surrogate key by hashing a list of columns using MD5.
        Concatenates column values with a separator to avoid collisions
        between ('a', 'bc') and ('ab', 'c').

        Usage:
            {{ generate_surrogate_key(['customer_id', 'order_id']) }}

        Returns a VARCHAR surrogate key as a hex MD5 hash.
        Null handling: nulls are replaced with the literal string '_null_'
        before hashing so that rows with nulls produce a deterministic key
        rather than a null surrogate key.

        Note: uses STRING not VARCHAR -- Databricks SQL requires VARCHAR(n)
        with explicit length. STRING is the correct unbounded type in Spark SQL.
    #}
    md5(
        concat_ws(
            '||',
            {% for col in columns %}
                coalesce(cast({{ col }} as string), '_null_')
                {%- if not loop.last %}, {% endif %}
            {% endfor %}
        )
    )
{% endmacro %}