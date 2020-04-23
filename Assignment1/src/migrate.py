from time import time
from typing import List, Tuple, Optional

import psycopg2
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError

NEO_LOGIN = "neo4j"
NEO_PASSWORD = "bitnami"

PG_LOGIN = "DMD2user"
PG_PASS = "DMD2pgPass"
PG_DB = "dvdrental"

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=(NEO_LOGIN, NEO_PASSWORD))

conn = psycopg2.connect(database=PG_DB, user=PG_LOGIN,
                        password=PG_PASS, host="localhost", port="5432")

cursor = conn.cursor()

steps = 42
current_step = 1


def report():
    global steps, current_step
    print(f"{current_step}/{steps} done")
    current_step += 1


def proper_type(column_name: str, column_type: str) -> str:
    if column_type in ["integer", "int", "smallint", "bigint"]:
        return f"toInteger(row.{column_name})"
    elif column_type in ["numeric", "float", "decimal"]:
        return f"toFloat(row.{column_name})"
    elif column_type in ["boolean"]:
        return f"toBoolean(row.{column_name})"
    elif column_type in ["date"]:
        # return f'date(datetime({{epochmillis:apoc.date.parse("row.{column_name}", "s", "yyyy-MM-dd")}}))'
        return f'apoc.date.parse(row.{column_name}, \'s\', "yyyy-MM-dd")'
    elif column_type.startswith("timestamp"):
        # return f"datetime({{epochmillis:toInteger(apoc.date.parse(row.{column_name}))}})"
        return f"apoc.date.parse(row.{column_name})"
    else:
        return f"row.{column_name}"


def set_unique_constraint(table: str) -> str:
    return f"CREATE CONSTRAINT ON ({table}:{table.capitalize()}) ASSERT {table}.{table}_id IS UNIQUE"


def transfer_table(table_name: str) -> Tuple[str, str]:
    export = f"copy {table_name} to '/tmp/{table_name}.csv' DELIMITER ',' CSV HEADER;"
    cursor.execute(export)
    import_statement = "USING PERIODIC COMMIT\n" \
                       f"LOAD CSV WITH HEADERS FROM 'file:/{table_name}.csv' AS row\n" \
                       f"MERGE ({table_name.lower()}: {table_name.capitalize()} {{{table_name.lower()}_id:toInteger(row.{table_name.lower()}_id)}})\n" \
                       "\tON CREATE SET\n"

    cursor.execute("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = %s ;",
                   (table_name,))

    props = []
    for column in cursor.fetchall():
        column_name, column_type = column
        if column_name.endswith("_id"):
            continue
        props.append(f"{table_name}.{column_name} = {proper_type(column_name, column_type)}")

    return set_unique_constraint(table_name), import_statement + "\t\t{data};".format(data=",\n\t\t".join(props))


def transfer_many2many(table_name: str, from_table: str, to_table: str, rel_name: str) -> str:
    export = f"copy {table_name} to '/tmp/{table_name}.csv' DELIMITER ',' CSV HEADER;"
    cursor.execute(export)
    cursor.execute("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = %s ;",
                   (table_name,))

    props = []
    for column in cursor.fetchall():
        column_name, column_type = column
        if column_name.endswith("_id"):
            continue
        props.append(f"{column_name}: {proper_type(column_name, column_type)}")

    import_statement = "USING PERIODIC COMMIT\n" \
                       f"LOAD CSV WITH HEADERS FROM 'file:/{table_name}.csv' AS row\n" \
                       f"MATCH ( {from_table} :{from_table.capitalize()} {{ {from_table}_id: {proper_type(f'{from_table}_id', 'int')} }})\n" \
                       f"MATCH ( {to_table}   :{to_table.capitalize()} {{ {to_table}_id: {proper_type(f'{to_table}_id', 'int')} }})\n" \
                       f"MERGE ( {from_table}) - [:{rel_name.upper()}{{ {','.join(props)} }}] -> ({to_table});"
    return import_statement


def transfer_relation(table_name: str, to_table: str, rel_name: str,
                      last_update_to_rel: bool = True, fk: Optional[str] = None) -> str:
    if fk is None:
        fk = f"{to_table}_id"
    export = f"copy (select {table_name}_id, {fk} as {to_table}_id " \
             f"{', last_update' if last_update_to_rel else ''}" \
             f" from {table_name}) to '/tmp/{table_name}_{to_table}_rel.csv' DELIMITER ',' CSV HEADER;"
    cursor.execute(export)

    props = []
    if last_update_to_rel:
        props.append(f"last_update: {proper_type('last_update', 'timestamp')}")

    import_statement = "USING PERIODIC COMMIT\n" \
                       f"LOAD CSV WITH HEADERS FROM 'file:/{table_name}_{to_table}_rel.csv' AS row\n" \
                       f"MATCH ( {table_name} :{table_name.capitalize()} {{ {table_name}_id: {proper_type(f'{table_name}_id', 'int')} }})\n" \
                       f"MATCH ( {to_table}   :{to_table.capitalize()} {{ {to_table}_id: {proper_type(f'{to_table}_id', 'int')} }})\n" \
                       f"MERGE ( {table_name}) - [:{rel_name.upper()}{{ {','.join(props)} }}] -> ({to_table});"
    return import_statement


def execute_statements(statements: List[str]) -> None:
    with driver.session() as session:
        for statement in statements:
            if statement is None:
                continue
            try:
                session.run(statement)
                report()
            except ClientError as e:
                print(e.message)


start = time()

import_statements = []

tables = ["category", "film", "language", "actor", "staff",
          "payment", "rental", "inventory",
          "customer", "address", "city", "country", "store"]

for table in tables:
    import_statements.extend(transfer_table(table))

import_statements.extend([
    transfer_many2many("film_category", "film", "category", "IN_CATEGORY"),
    transfer_many2many("film_actor", "actor", "film", "FILMED_IN"),
    transfer_relation("film", "language", "IN_LANGUAGE"),
    transfer_relation("inventory", "film", "RENTS_FILM"),
    transfer_relation("rental", "inventory", "RENTS"),
    transfer_relation("rental", "customer", "RENTED_TO"),
    transfer_relation("rental", "staff", "RENTED_BY"),
    transfer_relation("payment", "rental", "PAID_FOR", last_update_to_rel=False),
    transfer_relation("payment", "customer", "PAID_BY", last_update_to_rel=False),
    transfer_relation("payment", "staff", "ACCEPTED_BY", last_update_to_rel=False),
    transfer_relation("customer", "address", "LIVES_AT"),
    transfer_relation("staff", "address", "LIVES_AT"),
    transfer_relation("store", "address", "LOCATED_AT"),
    transfer_relation("address", "city", "SITUATED_IN"),
    transfer_relation("city", "country", "SITUATED_IN"),
    transfer_relation("store", "staff", "MANAGED_BY", fk="manager_staff_id"),
])

execute_statements(import_statements)

# Time elapsed: 26.824103832244873 seconds
print(f"Time elapsed: {time() - start} seconds")
