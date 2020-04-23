create or replace function get_recommendations(user_id int)
    returns table(
                    recommended_film_id smallint,
                    recommended_film_title varchar(255),
                    category varchar(25),
                    recommend_rate numeric
                 )
as
$body$
with
     watched_films as (
        select film_id, count(*) as rental_count
        from rental r
                 join inventory i on r.inventory_id = i.inventory_id
        where customer_id = user_id
        group by film_id
    ),
     watched_films_with_category as (
         select wf.film_id, wf.rental_count, fc.category_id
         from watched_films wf
                  join film_category fc on fc.film_id = wf.film_id
     ),
     watched_actors as (
         select wfc.film_id, fa.actor_id, wfc.rental_count, wfc.category_id
         from watched_films_with_category wfc
                  join film_actor fa on wfc.film_id = fa.film_id
     ),
     actors_with_genre as (
         select f.film_id, actor_id, category_id
         from film_actor fa3
                  join film_category f on fa3.film_id = f.film_id
     ),
     final_dataset as (
         select wa.film_id  as watched_film_id,
                wa.actor_id as watched_actor_id,
                wa.rental_count,
                awg.film_id as recommended_film_id
         from watched_actors wa
                  join actors_with_genre awg on wa.actor_id = awg.actor_id
         where wa.category_id = awg.category_id
           and not exists(select
                          from watched_films wf
                          where wf.film_id = awg.film_id)
     ),
     intermediate_rental as (
         select recommended_film_id, avg(rental_count) as rental_count
         from final_dataset ds
         group by watched_film_id, recommended_film_id
     ),
     final_rental as (
         select recommended_film_id, sum(rental_count) as rental_count
         from intermediate_rental
         group by recommended_film_id
     ),
     actor_count as (
         select recommended_film_id, count(distinct watched_actor_id) as actors_count
         from final_dataset ds
         group by recommended_film_id
     ),
     recommendation as (
         select ac.recommended_film_id,
                f2.title                       as recommended_film_title,
                fr.rental_count + actors_count as recommend_rate
         from final_rental fr
                  join actor_count ac on fr.recommended_film_id = ac.recommended_film_id
                  join film f2 on fr.recommended_film_id = f2.film_id
         order by recommend_rate desc
     )
select rec.recommended_film_id, rec.recommended_film_title, c.name as category, rec.recommend_rate
from film_category fc
         join recommendation rec on fc.film_id = rec.recommended_film_id
         join category c on fc.category_id = c.category_id;
$body$
    language sql;


select * from get_recommendations(12);