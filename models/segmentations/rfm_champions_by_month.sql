{{ config( 
    materialized = 'table'
   ) 
}} 


{% set number_of_months = 12 %}

{% for _ in range(0, number_of_months) %}

    select
        user_id,
        dateadd('month', {{ ( loop.index - 1 ) * -1 }}, date_trunc('month', current_date ) ) as before_date
    from (
        select

            user_id,
            
            date_diff( 'months', first_order_date, current_date )                       as months_since_first_order,
            date_diff( 'months', last_order_date, current_date )                        as months_since_last_order,
            cbrt( date_diff( 'months', last_order_date, current_date ) )                as months_since_last_order_cbrt,
            greatest( sum(shipped_margin), 0)                                           as total_shipped_margin,
            total_shipped_margin / nullif(sum(shipped_no_of_units_sold), 0)             as price_point_index,
            count(distinct order_id)::float / months_since_first_order                  as monthly_orders,
            
            round( percent_rank() over (order by months_since_last_order_cbrt desc), 2)     as recency_percent_rank,
            round( percent_rank() over (order by total_shipped_margin), 2)                  as monetary_percent_rank,
            round( percent_rank() over (order by monthly_orders), 2)                        as frequency_percent_rank,
            round( percent_rank() over (order by price_point_index), 2)                     as price_point_index_percent_rank,
            
            6 - least( ceil( cbrt( date_diff( 'months', last_order_date, current_date ) + 1 ) ), 5)     as recency,
            round(monetary_percent_rank * 4, 0) + 1                                                 as monetary,
            round(frequency_percent_rank * 4, 0) + 1                                                as frequency,
            round(price_point_index_percent_rank * 4, 0) + 1                                        as price_point,
            
            greatest( sum(shipped_margin) / months_since_first_order, 0)            as monthly_shipped_margin,
            count(distinct order_id)                                                as total_orders,
            
            case
                when monetary = 5 and frequency = 5 then 'champion'
                when monetary + frequency >= 9
                    and recency not in (1,2)           then 'champion'
                                                    else 'other'
            end as segmentation
            
        from (
            select 
                *,
                coalesce(shipped_product_revenue_gbp, 0) + coalesce(shipped_shipping_revenue_gbp, 0) + coalesce(shipped_cogs_gbp, 0) as shipped_margin
            from dbt_production.master_orderitems
            where
                order_datetime < dateadd('month', {{ ( loop.index - 1 ) * -1 }}, date_trunc('month', current_date ) )
            and user_id is not null
            and is_digital = 0
        ) orders
        group by 1,2,3,4
    )
    where segmentation = 'champion'

    {% if not loop.last %}
        union
    {% endif %}

{% endfor %}
