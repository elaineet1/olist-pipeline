with customers as (
    select * from {{ ref('stg_customers') }}
),

geolocation as (
    select
        geolocation_zip_code_prefix,
        avg(geolocation_lat) as geolocation_lat,
        avg(geolocation_lng) as geolocation_lng,
        max(geolocation_city) as geolocation_city,
        max(geolocation_state) as geolocation_state
    from {{ ref('stg_geolocation') }}
    group by geolocation_zip_code_prefix
)

select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.geolocation_lat,
    g.geolocation_lng
from customers c
left join geolocation g
    on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix