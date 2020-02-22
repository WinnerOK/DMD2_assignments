select fa1.actor_id as actor1_id, fa2.actor_id as actor2_id, count(*) as count
from film_actor fa1 join film_actor fa2 on fa1.film_id=fa2.film_id
where fa1.actor_id < fa2.actor_id
group by fa1.actor_id, fa2.actor_id
order by count desc