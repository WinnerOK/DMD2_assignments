from time import time

from neo4j import GraphDatabase

import tabulate

NEO_LOGIN = "neo4j"
NEO_PASSWORD = "bitnami"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(NEO_LOGIN, NEO_PASSWORD))

# find chains between customer and film, count of chains represent number of times film has been rented
start = time()


def get_customer_history(customer_id: int):
    with driver.session() as session:
        dataset = session.run(
            "match (c:Customer{customer_id:$cust_id})<-[:RENTED_TO]-(:Rental)-[:RENTS]->(:Inventory)-[:RENTS_FILM]->\n"
            "(f:Film)-[:IN_CATEGORY]->(cat:Category)\n"
            "return f.film_id as film_id, f.title as title,\n"
            "cat.category_id as category_id, cat.name as category, count(*) as count\n"
            "order by count desc", cust_id=customer_id
        )

        return tuple((x["film_id"], x["title"], x["category_id"], x["category"], x["count"]) for x in dataset)


data = get_customer_history(12)
print("Query3:")
print(tabulate.tabulate(data, ["film_id", "title", "category_id", "category", "rental_count"]))

# Elapsed: 0.09195184707641602 sec
print(f"Elapsed: {time() - start} sec")
driver.close()
