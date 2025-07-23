SELECT
    c.customer_state AS estado,
    p.product_category_name AS producto,
    COUNT(oi.order_item_id) AS cantidad_vendida,
    SUM(oi.price) AS precio_total_cantidad_vendidos
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = "delivered"
GROUP BY c.customer_state, p.product_category_name 
ORDER BY estado, cantidad_vendida DESC;

select * from orders

select * from orders o WHERE o.order_status = "delivered" order by o.order_purchase_timestamp desc

select * from products group by 


SELECT
    COUNT(DISTINCT strftime('%Y-%m', o.order_purchase_timestamp)) AS total_meses,
    COUNT(oi.order_item_id) AS cantidad_total_ventas
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status = "delivered"

SELECT
    COUNT(DISTINCT date(o.order_purchase_timestamp)) AS total_dias,
    COUNT(oi.order_item_id) AS cantidad_total_ventas
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status = "delivered";
