-- The only difference from neo4j query is order of rows with equal count
create or replace function customer_history(customerID int)
    returns table
            (
                film_id     int,
                title       varchar(255),
                category_id int,
                name        varchar(25),
                count       bigint
            )
as
$body$
with film_rent_count as (
    select i.film_id, count(*) as count
    from rental r
             join customer c on r.customer_id = c.customer_id
             join inventory i on r.inventory_id = i.inventory_id
    where c.customer_id = customerID
    group by i.film_id
),
     id_data as (
         select f.film_id, f.title, fc.category_id, frc.count
         from film_rent_count frc
                  join film_category fc on frc.film_id = fc.film_id
                  join film f on frc.film_id = f.film_id
     )
select film_id, title, c.category_id, name, count
from id_data i
         join category c on i.category_id = c.category_id
order by count desc;
$body$
    language sql;

select * from customer_history(1);