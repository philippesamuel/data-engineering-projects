-- best performing months by total revenue
SELECT 
    year
    , month
    , round(avg(quantity),1) as mean_quantity
    , format_number(sum(revenue)) as total_revenue
FROM "retail_db"."curated" 
group by year, month
order by sum(revenue) desc
LIMIT 10;