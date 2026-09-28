with source as (
    select * from {{ source('raw', 'raw_payments') }}
)

select
    id as payment_id,
    order_id,
    payment_method,
    amount as amount_cents
from source
