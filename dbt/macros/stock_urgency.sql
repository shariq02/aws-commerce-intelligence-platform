{% macro stock_urgency(alert_level_col) %}
    {#
        Converts a stock_alert_level string to a numeric urgency score (1-4)
        and a recommended_action string.

        Usage:
            {{ stock_urgency('stock_alert_level') }}

        This macro returns the urgency score. Use the companion
        stock_recommended_action macro for the action string.

        Scores:
            4 = critical  (stock <= 50% of reorder threshold)
            3 = high      (stock <= reorder threshold)
            2 = medium    (stock <= 2x reorder threshold)
            1 = normal    (stock > 2x reorder threshold)
            0 = unknown   (null alert level)
    #}
    case {{ alert_level_col }}
        when 'critical' then 4
        when 'high'     then 3
        when 'medium'   then 2
        when 'normal'   then 1
        else 0
    end
{% endmacro %}


{% macro stock_recommended_action(alert_level_col) %}
    {#
        Returns a recommended action string based on stock alert level.

        Usage:
            {{ stock_recommended_action('stock_alert_level') }}

        Actions:
            order_immediately -- critical stock, risk of stockout
            order_soon        -- high stock alert, approaching threshold
            monitor           -- medium alert, worth watching
            sufficient        -- normal stock levels
            unknown           -- null or unexpected alert level
    #}
    case {{ alert_level_col }}
        when 'critical' then 'order_immediately'
        when 'high'     then 'order_soon'
        when 'medium'   then 'monitor'
        when 'normal'   then 'sufficient'
        else 'unknown'
    end
{% endmacro %}
