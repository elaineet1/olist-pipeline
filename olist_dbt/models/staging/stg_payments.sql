with source as (
    select * from {{ source('olist_raw', 'olist_payments') }}
)

select
    order_id,
    cast(payment_sequential as numeric) as payment_sequential,
    payment_type,
    cast(payment_installments as numeric) as payment_installments,
    cast(payment_value as numeric) as payment_value
from source
