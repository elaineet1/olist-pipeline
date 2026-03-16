with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select
        order_id,
        customer_id,
        cast(date(order_purchase_timestamp) as date) as order_date_id
    from {{ ref('stg_orders') }}
)

select
    p.order_id,
    o.customer_id,
    o.order_date_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments,
    p.payment_value
from payments p
left join orders o
    on p.order_id = o.order_id