{{ config( 
    materialized = 'table'
   ) 
}} 

with

first_optin as (

    select 
        email as user_id, 
        dt_optin,
        dt_optout,
        greatest(dt_optin,dt_optout) = dt_optin as optin_status,
        optin
    from dbt_production.ometria_contact_records
    where dt_optin < '2021-12-01' --or dt_optout < '2021-12-01'
      and optin_status = true
),

-- first_optin_ns as (

--     select sha1(lower(subscriber_email)) as user_id, true as optin_status, min(subscribed_at)::date as dt_optin from magento.newsletter_subscriber group by 1,2
--     having min(subscribed_at)::date < '2021-12-01'
-- ),

champions as (

    select c.user_id, c.before_date, optin from rudderstack_external.rfm_champions_by_month c
    left join rudderstack_external.subscribers_status_by_month__backup b
        on c.user_id = b.user_id and c.before_date = b.before_date
        
)

-- select count(optin_status), count(optin) from first_optin --test optin_status (999 in 1000 records pass)
, coalesced as (

    select
        champions.user_id,
        champions.before_date,
        -- champions.optin as in_month_optin,
        coalesce(champions.optin 
            ,first_optin.optin_status
            ,false
            -- ,first_optin_ns.optin_status
        ) as optin

    from champions
    left join first_optin on champions.user_id = first_optin.user_id
    -- left join first_optin_ns on champions.user_id = first_optin_ns.user_id
)

select * from coalesced