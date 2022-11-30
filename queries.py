from neo4j import GraphDatabase

# Database Credentials
uri = "bolt://localhost:7687"
userName = "neo4j"
password = "neo4j_luca"

# Connect to the neo4j database server
driver = GraphDatabase.driver(uri, auth=(userName, password))
session = driver.session()

"""
    READING DATA FROM NEO4J
"""
# Cypher pattern: () --> ()
# Nodes: (:Person)
# Relations: (:Person) -- (:Movie), (:Person) --> (:Movie)
# Relation type: [:ACTED_IN]
# property (key: value): (:Movie {title: 'Cloud Atlas'})
# MATCH = SELECT = retrieve nodes
query = "MATCH (p:Person {name: 'Tom Hanks'}) - [:ACTED_IN] -> (m:Movie {title: 'Cloud Atlas'}) return p, m"
session.run(query)
print('#############################################')

# retrieve all Person node
query  = "MATCH (p:Person) return p.name"
nodes = session.run(query)
for node in nodes:
    print(node)
print('#############################################')
# retrieve one Node with property
query = "MATCH (p:Person {name: 'Tom Hanks'}) return p"
nodes = session.run(query)
for node in nodes:
    print(node)
print('#############################################')
# filter query
query = "MATCH (p:Person) WHERE p.name = 'Tom Hanks' return p.born"
query = "MATCH (p:Person) WHERE p.name = 'Tom Hanks' OR p.name = 'Rita Wilson' return p.born"
nodes = session.run(query)
for node in nodes:
    print(node)


print('#############################################')
# relationship query: retrieve all Person node that have a relation with Movie
query = "MATCH (p:Person) --> (:Movie) return p"
nodes = session.run(query)
for node in nodes:
    print(node)

print('#############################################')
# relationship query: retrieve nodes with specific relation
query = "MATCH (p:Person) -[:ACTED_IN]-> (:Movie) return p"
nodes = session.run(query)
for node in nodes:
    print(node)

print('#############################################')
# retrieve all movies where Tom Hanks acted
query = "MATCH (p:Person {name: 'Tom Hanks'}) -[:ACTED_IN]-> (m:Movie) return m.title as title"
nodes = session.run(query)
for node in nodes:
    print(node.get('title'))


print('#############################################')
# retrieve actor and movie (released in 2008 or 2009)
query = "MATCH (p:Person) -[:ACTED_IN]-> (m:Movie) " \
        "WHERE m.released = 2008 OR m.released = 2009 " \
        "RETURN p, m"
nodes = session.run(query)
for node in nodes:
    print(node)

print('#############################################')
query = "MATCH (p:Person) -[:ACTED_IN]-> (m:Movie) " \
        "WHERE m.title = 'The Matrix'" \
        "RETURN p.name as name"
nodes = session.run(query)
for node in nodes:
    print(node.get('name'))

print('#############################################')
# 2003 <= m.released <= 2003
query = "MATCH (p:Person) -[:ACTED_IN]-> (m:Movie) " \
        "WHERE 2003 <= m.released <= 2003 " \
        "RETURN p.name as name, m.title as movieTitle, m.released as movieReleased"
nodes = session.run(query)
for node in nodes:
    print(node.get('name'), " - ", node.get('movieTitle'), " - ", node.get('movieReleased'))


print('#############################################')
# filter: property is not null: m.tagline IS NOT NULL
query = "MATCH (p:Person) -[:ACTED_IN]-> (m:Movie) " \
        "WHERE p.name = 'Jack Nicholson' AND m.tagline IS NOT NULL " \
        "RETURN m.title, m.tagline"
nodes = session.run(query)
for node in nodes:
    print(node.get('m.title'), " - ", node.get('m.tagline'))
print('#############################################')
# filter: property is null: m.tagline IS NOT NULL
query = "MATCH (p:Person) -[:ACTED_IN]-> (m:Movie) " \
        "WHERE p.name = 'Jack Nicholson' AND m.tagline IS NULL " \
        "RETURN m.title, m.tagline"
nodes = session.run(query)
for node in nodes:
    print(node.get('m.title'), " - ", node.get('m.tagline'))
print('#############################################')
# filter: property STARTS WITH, end with, contains keyword - warning: CASE SENSITIVE
#  warning: CASE SENSITIVE: lowercase = toLower(p.name)
query = "MATCH (p:Person) -[:ACTED_IN]-> () " \
        "WHERE toLower(p.name) STARTS WITH 'michael' " \
        "RETURN p.name"
nodes = session.run(query)
for node in nodes:
    print(node.get('p.name'))

print('#############################################')
# filter: retrieve person that wrote a movie but not directed a movie
# NOT EXISTS
query = "MATCH (p:Person) -[:ACTED_IN]-> (m: Movie) " \
        "WHERE NOT EXISTS ( (p) -[:DIRECTED] -> (m) ) " \
        "RETURN p.name, m.title"
nodes = session.run(query)
for node in nodes:
    print(node.get('p.name'), " - ", node.get('m.title'))

print('#############################################')
# filter: retrieve person that wrote a movie but not directed a movie
# IN [1965, 1970, 1975]
query = "MATCH (p:Person) " \
        "WHERE p.born IN [1965, 1970, 1975] " \
        "RETURN p.name, p.born"
nodes = session.run(query)
for node in nodes:
    print(node.get('p.name'), " - ", node.get('p.born'))


"""
    WRITING DATA TO NEO4J
    Cypher command to create nodes: MERGE
    Cypher command to create relationships: MERGE
"""
# creating nodes
# specify PRIMARY KEY (e.g. name: 'Tom Hanks')
# if execute merge (create) multiple times -> not create node because already exist
# Cypher command to create nodes: CREATE -> neo4j create node with property even if they already exist
# best practise: MERGE
query = "MERGE (p:Person {name: 'Tom Hanks'}) RETURN p"
nodes = session.run(query)

# creating relationships
# Cypher command to create relationships: MERGE
# you need references to 2 existing nodes
# MATCH = RETRIEVE A NODE
query = "MATCH (p:Person {name: 'Tom Hanks'}) " \
        "MATCH (m:Movie {title: 'Apollo 13'})" \
        "MERGE (p)-[r:ACTED_IN]->(m)"
nodes = session.run(query)

print('#############################################')
query = "MATCH (p:Person {name: 'Tom Hanks'}) " \
        "MATCH (m:Movie {title: 'Apollo 13'})" \
        "MERGE (p)-[r:ACTED_IN {roles: ['Jim Lovell']}]->(m)"
nodes = session.run(query)
query = "MATCH (p:Person) -[r:ACTED_IN]-> (m: Movie) " \
        "WHERE r.roles = ['Jim Lovell']" \
        "RETURN p.name, m.title"
nodes = session.run(query)
for node in nodes:
    print(node.get('p.name'), " - ", node.get('m.title'))

print('#############################################')

# update properties to nodes
# Cypher command : SET
# when create node, you must set property primary key for identifier node (e.g  {name: 'Tom Hanks'})
query = "MERGE (p:Person {name: 'Tom Hanks'}) " \
        "SET p.born = 1965 " \
        "RETURN p"
# SET command is used for, also, update property
query = "MATCH (p:Person {name: 'Tom Hanks'}) " \
        "SET p.born = 1966 " \
        "RETURN p"
nodes = session.run(query)
query = "MATCH (p:Person) " \
        "WHERE p.born = 1965 " \
        "RETURN p"
for node in nodes:
    print(node)

print('#############################################')
# update properties to relationship
query = "MATCH (p:Person {name: 'Tom Hanks'}) " \
        "MATCH (m:Movie {title: 'Apollo 13'})" \
        "MERGE (p)-[r:ACTED_IN]->(m) " \
        "SET r.roles =['Forrest']"
nodes = session.run(query)
query = "MATCH (p:Person) -[r:ACTED_IN]-> (m: Movie) " \
        "WHERE p.name = 'Tom Hanks' AND m.title = 'Apollo 13' " \
        "RETURN p, m, r"
nodes = session.run(query)
for node in nodes:
    print(node)

# REMOVE properties to relationship: REMOVE r.role
query = "MATCH (p:Person {name: 'Tom Hanks'}) " \
        "MATCH (m:Movie {title: 'Apollo 13'})" \
        "MERGE (p)-[r:ACTED_IN]->(m) " \
        "REMOVE r.roles"
nodes = session.run(query)
# MATCH (p:Person) -[:ACTED_IN]-> (m: Movie) WHERE p.name = 'Tom Hanks' AND m.title = 'Apollo 13' return p, m
query = "MATCH (p:Person) -[r:ACTED_IN]-> (m: Movie) " \
        "WHERE p.name = 'Tom Hanks' AND m.title = 'Apollo 13' " \
        "RETURN p, m, r"
nodes = session.run(query)
for node in nodes:
    print(node)


# merge processing

# add or updating a Movie

# deleting data

