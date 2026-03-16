with geolocation as (
    select * from {{ ref('stg_geolocation') }}
)

select
    geolocation_zip_code_prefix,
    avg(geolocation_lat) as geolocation_lat,
    avg(geolocation_lng) as geolocation_lng,
    max(geolocation_city) as geolocation_city,
    max(geolocation_state) as geolocation_state
from geolocation
group by geolocation_zip_code_prefix