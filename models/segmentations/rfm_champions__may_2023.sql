{{ config( 
    materialized = 'table'
   ) 
}} 

with

may_champions as (

    select * from rudderstack_external.rfm_champions_by_month where before_date = '2023-05-01'
),

optin as (

    select email, optin from dbt_production.ometria_contact_records
),

final as (

select
    user_id,
    coalesce(optin.optin, false) as optin
    
from may_champions

left join optin
    on optin.email = may_champions.user_id
)

select * from final