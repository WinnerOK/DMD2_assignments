from time import time

import tabulate
from neo4j import GraphDatabase

NEO_LOGIN = "neo4j"
NEO_PASSWORD = "bitnami"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(NEO_LOGIN, NEO_PASSWORD))

# Find the shortest path between given actor and each other within 25 films.
# (The restriction of 25 hops should be more than enough for the real world db)
start = time()


def get_separation_degrees(actor_id: int):
    with driver.session() as session:
        data = session.run(
            "match p=shortestPath(\n"
            "	(actor:Actor{actor_id:$a_id}) -[r:FILMED_IN*..50] -(next:Actor)\n"
            ")\n"
            "where actor<>next\n"
            "return next.actor_id as to_id, next.first_name as to_name, "
            "next.last_name as to_lastname, size(r)/2 as hops\n"
            "order by to_id",
            a_id=actor_id
        )
        return tuple((x["to_id"], f'{x["to_name"]} {x["to_lastname"]}', x["hops"]) for x in data)


print("Query5:")
print(tabulate.tabulate(get_separation_degrees(12), ["to (id)", "name", "degree"]))

# Elapsed: 0.1719803810119629
print(f"Elapsed: {time() - start}")
driver.close()
