with orders as (
    select order_purchase_timestamp as order_date
    from {{ ref('stg_orders') }}
    where order_purchase_timestamp is not null
)

select distinct
    cast(date(order_date) as date) as date_id,
    extract(year from order_date) as year,
    extract(month from order_date) as month,
    extract(day from order_date) as day,
    extract(quarter from order_date) as quarter,
    extract(dayofweek from order_date) as day_of_week,
    format_date('%B', date(order_date)) as month_name,
    format_date('%A', date(order_date)) as day_name,
    case
        when extract(dayofweek from order_date) in (1, 7) then true
        else false
    end as is_weekend
from orders
order by date_id