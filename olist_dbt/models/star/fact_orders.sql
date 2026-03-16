with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select
        order_id,
        count(order_item_id) as order_item_count,
        sum(price) as total_price,
        sum(freight_value) as total_freight_value,
        sum(price + freight_value) as total_order_value
    from {{ ref('stg_order_items') }}
    group by order_id
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    cast(date(o.order_purchase_timestamp) as date) as order_date_id,
    coalesce(oi.order_item_count, 0) as order_item_count,
    coalesce(oi.total_price, 0) as total_price,
    coalesce(oi.total_freight_value, 0) as total_freight_value,
    coalesce(oi.total_order_value, 0) as total_order_value,
    date_diff(
        date(o.order_delivered_customer_date),
        date(o.order_purchase_timestamp),
        day
    ) as delivery_days
from orders o
left join order_items oi
    on o.order_id = oi.order_id