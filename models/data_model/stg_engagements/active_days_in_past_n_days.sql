with cte_id_stitched_tracks as 
(select distinct b.main_id as main_id, timestamp from {{ source('ecommerce', 'tracks') }} a left join 
ANALYTICS_DB.DATA_APPS_SIMULATED.{{var('id_stitcher_name')}} b 
on (a.user_id = b.other_id and b.other_id_type = 'user_id')),

cte_active_days_in_past_n_days as
({% for lookback_days in var('lookback_days') %}
select main_id,
count(distinct date(timestamp)) as active_days_in_past_n_days,
{{lookback_days}} as n_value
from cte_id_stitched_tracks
where datediff(day, date(timestamp), date({{get_end_timestamp()}})) <= {{lookback_days}}
and timestamp >= '{{ var('start_date') }}' and timestamp <= '{{ var('end_date') }}' and main_id is not null
group by main_id
{% if not loop.last %} union {% endif %}
{% endfor %})

{% for lookback_days in var('lookback_days') %}
select main_id, 
'active_days_in_past_{{lookback_days}}_days' as feature_name, active_days_in_past_n_days as feature_value from 
cte_active_days_in_past_n_days where n_value = {{lookback_days}}
{% if not loop.last %} union {% endif %}
{% endfor %}