with payments as (
    select distinct payment_type
    from {{ ref('stg_payments') }}
    where payment_type is not null
)

select
    payment_type,
    case payment_type
        when 'credit_card' then 'Credit Card'
        when 'boleto' then 'Boleto'
        when 'voucher' then 'Voucher'
        when 'debit_card' then 'Debit Card'
        else 'Other'
    end as payment_type_description
from payments