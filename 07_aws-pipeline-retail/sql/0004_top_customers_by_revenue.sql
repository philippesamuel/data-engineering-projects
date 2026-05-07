-- best most valuable customers by total revenue
SELECT 
    customerid
    , round(avg(quantity),1) as mean_quantity
    , format_number(sum(revenue)) as total_revenue
FROM "retail_db"."curated" 
group by customerid 
order by sum(revenue) desc
LIMIT 10;