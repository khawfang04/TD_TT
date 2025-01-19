-- add database,schema for bigquery format

SELECT 
    pc.product_class_name,
    ROW_NUMBER() OVER (
        PARTITION BY pc.product_class_name 
        ORDER BY SUM(s.quantity * p.retail_price) DESC, SUM(s.quantity) ASC
    ) AS rank,
    p.product_name,
    SUM(s.quantity * p.retail_price) AS sales_value
FROM 
    Sales_Transaction s
JOIN 
    Product p ON s.product_id = p.product_id
JOIN 
    Product_Class pc ON p.product_class_id = pc.product_class_id
GROUP BY 
    pc.product_class_name, p.product_name
HAVING 
    ROW_NUMBER() OVER (
        PARTITION BY pc.product_class_name 
        ORDER BY SUM(s.quantity * p.retail_price) DESC, SUM(s.quantity) ASC
    ) <= 2
ORDER BY 
    1,2 ;
