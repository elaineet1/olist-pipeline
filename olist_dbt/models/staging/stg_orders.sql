with source as (
    select * from {{ source('olist_raw', 'olist_orders') }}
)

select
    order_id,
    customer_id,
    order_status,
    timestamp(nullif(order_purchase_timestamp, '')) as order_purchase_timestamp,
    timestamp(nullif(order_approved_at, '')) as order_approved_at,
    timestamp(nullif(order_delivered_carrier_date, '')) as order_delivered_carrier_date,
    timestamp(nullif(order_delivered_customer_date, '')) as order_delivered_customer_date,
    timestamp(nullif(order_estimated_delivery_date, '')) as order_estimated_delivery_date
from source