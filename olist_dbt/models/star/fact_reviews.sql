with reviews as (
    select * from {{ ref('stg_reviews') }}
),

orders as (
    select
        order_id,
        customer_id,
        cast(date(order_purchase_timestamp) as date) as order_date_id
    from {{ ref('stg_orders') }}
)

select
    r.review_id,
    r.order_id,
    o.customer_id,
    o.order_date_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    date_diff(
        date(r.review_answer_timestamp),
        date(r.review_creation_date),
        day
    ) as review_response_days
from reviews r
left join orders o
    on r.order_id = o.order_id
qualify row_number() over (partition by r.review_id order by r.review_creation_date desc) = 1