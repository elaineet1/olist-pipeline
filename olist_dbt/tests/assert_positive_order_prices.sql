-- Fails if any order has a negative total price.
-- Zero-value orders (e.g. cancelled orders with no items) are acceptable.

SELECT
    order_id,
    total_price
FROM {{ ref('fact_orders') }}
WHERE total_price < 0