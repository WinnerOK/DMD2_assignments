with
     inventory_category as (
         select i.inventory_id, fc.category_id
         from inventory i join film_category fc on i.film_id = fc.film_id
     ),
    recent_year as (
        select date_part('year', rental_date) as year
        from rental
        order by rental_date desc
        limit 1
    )
select c.customer_id, count(ic.category_id)
from rental r join inventory_category ic on r.inventory_id=ic.inventory_id
              join customer c on r.customer_id = c.customer_id
where date_part('year', r.rental_date) = (select year from recent_year)
-- (select year from recent_year)
-- should be replaced with
-- date_part('year', now())
group by c.customer_id
having count(ic.category_id) >= 2 ;