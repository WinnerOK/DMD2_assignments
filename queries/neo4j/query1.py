from neo4j import GraphDatabase
from time import time

NEO_LOGIN = "neo4j"
NEO_PASSWORD = "bitnami"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(NEO_LOGIN, NEO_PASSWORD))

start = time()
with driver.session() as session:
    data = session.run(
        "MATCH (r:Rental)\n"
        "WITH r\n"
        "ORDER BY r.rental_date DESC LIMIT 1\n"
        "with datetime({epochmillis:r.rental_date}).year as most_recent_year\n"

        "match "
        "(cat2:Category)<-[:IN_CATEGORY]-(f2:Film)<-[:RENTS_FILM]-(:Inventory)<-[:RENTS]-(r2:Rental)-[:RENTED_TO]->\n"
        "(customer:Customer)<-[:RENTED_TO]-(r1:Rental)-[:RENTS]->(:Inventory)-[:RENTS_FILM]->(f1:Film)-[:IN_CATEGORY]->(cat1:Category)\n"

        "where cat2 <> cat1 and f1 <> f2 and\n"
        "datetime({epochmillis:r1.rental_date}).year = most_recent_year and datetime({epochmillis:r2.rental_date}).year = most_recent_year\n"
        "return DISTINCT customer.customer_id as customer_id, customer.first_name as first_name, customer.last_name as last_name"
    )
    q1 = [f'{x["customer_id"]}: {x["first_name"]} {x["last_name"]}' for x in data]
    print("Query1:\nPeople, who rented movies of at least two different categories during the most recent year\n\t",
          "\n\t".join(q1), sep="")

# Elapsed: 0.20471739768981934 sec
print(f"Elapsed: {time()-start} sec")
driver.close()
