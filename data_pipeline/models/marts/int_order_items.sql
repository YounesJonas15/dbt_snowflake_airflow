with order_cte as
(
    select 
    order_key,
    customer_key,
    order_date
    from {{ref('stg_tpch_orders')}}
    order by order_date
),
line_item_cte as 
(
    select 
    order_key,
    line_number,
    order_item_key,
    part_key
    from {{ref('stg_tpch_line_items')}}
),
final_cte as 
(
    select 
    order_cte.order_key,
    order_cte.customer_key,
    order_cte.order_date,
    line_item_cte.line_number,
    line_item_cte.order_item_key,
    line_item_cte.part_key
    from order_cte join line_item_cte using(order_key)
)

select * from final_cte
