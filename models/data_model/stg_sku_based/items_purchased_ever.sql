{% if target.type == 'redshift' %}
with numbers as ({{dbt_utils.generate_series(upper_bound=1000)}}),
cte_json as ( 

    select {{ var('main_id')}}, 
        {{ var('col_ecommerce_order_completed_properties_products')}}, 
        json_array_length({{ var('col_ecommerce_order_completed_properties_products')}})  as n_array
    from {{ ref('stg_order_completed') }}

), cte_product_data as (

    select {{ var('main_id')}},  
    json_extract_array_element_text({{ var('col_ecommerce_order_completed_properties_products')}}, generated_number::int, true) as product_array
    from cte_json a cross join (select generated_number - 1 as generated_number from numbers) b where b.generated_number <= (a.n_array-1)

), cte_user_product_id as (

    select {{ var('main_id')}}, 
        json_extract_path_text(product_array, '{{ var('product_ref_var') }}') as {{ var('product_ref_var') }} 
    from cte_product_data

)
{% endif %}
select {{ var('main_id')}}, 
    {{array_agg( var('product_ref_var') )}} as {{ var('product_ref_var') }}
{% if target.type == 'redshift' %}
from cte_user_product_id

{% else %}
from {{ ref('order_completed') }}, TABLE(FLATTEN(parse_json({{ var('col_ecommerce_order_completed_properties_products')}}))) t
where {{timebound( var('col_ecommerce_order_completed_timestamp'))}} and {{ var('main_id')}} is not null
{% endif %}
group by {{ var('main_id')}}