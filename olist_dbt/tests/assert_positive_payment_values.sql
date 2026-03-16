-- Fails if any payment has a negative value.
-- Zero-value payments (e.g. fully discounted vouchers) are acceptable.

SELECT
    order_id,
    payment_type,
    payment_value
FROM {{ ref('fact_payments') }}
WHERE payment_value < 0