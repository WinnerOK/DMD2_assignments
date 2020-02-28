from time import time

from neo4j import GraphDatabase

import tabulate

from typing import Optional

NEO_LOGIN = "neo4j"
NEO_PASSWORD = "bitnami"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(NEO_LOGIN, NEO_PASSWORD))

# A recommendation rate for film R is how many times customer C rented a film F + how many actors of F were filmed in R,
# where R and F are in the same category
start = time()


def get_customer_recommendations(customer_id: int, limit: Optional[int] = None):
    with driver.session() as session:
        dataset = session.run(
            "match "
            "(:Customer{customer_id:$cust_id}) <- [:RENTED_TO] - (:Rental) "
            "- [:RENTS] -> (:Inventory) - [views:RENTS_FILM] -> (watched:Film)\n"
            "<-[:FILMED_IN] - (a:Actor)- [conn:FILMED_IN] -> (rec:Film), "

            "(watched) - [:IN_CATEGORY] -> (cat:Category) <- [:IN_CATEGORY] - (rec)\n"
            "where rec <> watched\n"
            "return rec.film_id as recommendation_film_id, rec.title as recommendation_film_title, "
            "cat.name as category,\n"
            "count(distinct conn) + count(distinct views) as recommendation_rate\n"
            "order by recommendation_rate desc\n"
            f"{'limit $lim' if limit else ''}",
            cust_id=customer_id, lim=limit
        )

        return tuple(
            (x["recommendation_film_id"], x["recommendation_film_title"], x["category"], x["recommendation_rate"]) for x
            in dataset)


print("Query4:")
data = get_customer_recommendations(12, limit=5)
print("Recommendations:\n", tabulate.tabulate(data, ["film_id", "title", "category", "recommendation_rate"]))

# Elapsed 0.12119197845458984 sec
print(f"Elapsed {time() - start} sec")
driver.close()
