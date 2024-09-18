with order_cte as
(
    select 
    order_key,
    customer_key,
    order_date
    from {{ref('stg_tpch_orders')}}
    
),
line_item_cte as 
(
    select 
    order_key,
    line_number,
    order_item_key,
    part_key,
    discount_percentage,
    extended_price
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
    line_item_cte.part_key,
    line_item_cte.extended_price,
    {{discounted_amount('line_item_cte.extended_price','line_item_cte.discount_percentage')}} as item_discounted_amount
    from order_cte join line_item_cte using(order_key)
    order by order_cte.order_date
)

select * from final_cte
