with source as (
    select * from {{ source('olist_raw', 'olist_reviews') }}
)

select
    review_id,
    order_id,
    cast(review_score as numeric) as review_score,
    review_comment_title,
    review_comment_message,
    safe.parse_timestamp('%m/%d/%Y %H:%M', nullif(review_creation_date, '')) as review_creation_date,
    safe.parse_timestamp('%m/%d/%Y %H:%M', nullif(review_answer_timestamp, '')) as review_answer_timestamp
from source