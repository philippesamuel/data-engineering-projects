-- best performing products by total revenue
SELECT 
    stockcode
    , arbitrary(description) as description
    , round(avg(quantity),1) as mean_quantity
    , format_number(sum(revenue)) as total_revenue
FROM "retail_db"."curated" 
group by stockcode
order by sum(revenue) desc
LIMIT 10;