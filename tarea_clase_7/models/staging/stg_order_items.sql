select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    p.product_name,
    oi.is_primary_item = 1 as is_primary_item,
    oi.price_usd,
    oi.cogs_usd,
    oi.price_usd - oi.cogs_usd as margin_usd
from {{ source('raw', 'raw_order_items') }} oi
left join {{ source('raw', 'raw_products') }} p on oi.product_id = p.product_id