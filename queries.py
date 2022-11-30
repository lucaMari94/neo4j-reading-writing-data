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
"""
