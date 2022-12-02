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

print('#############################################')

# conditional
# merge processing
# ON MATCH
query = "MERGE (p:Person {name: 'Robin Williams'}) " \
        "ON MATCH " \
        "SET p.died=2014 " \
        "RETURN p"
nodes = session.run(query)
for node in nodes:
    print(node)

# ON CREATE
query = "MERGE (m:Movie {title: 'Freaky'}) " \
        "ON CREATE " \
        "SET m.released=2020 " \
        "ON MATCH " \
        "SET m.revenue=15920855 " \
        "RETURN m"
nodes = session.run(query)
for node in nodes:
    print(node)
"""
First execution: ON CREATE: 'released': 2020
<Record m=<Node element_id='172' labels=frozenset({'Movie'}) properties={'title': 'Freaky', 'released': 2020}>>

Second execution: ON MATCH: 'revenue': 15920855
<Record m=<Node element_id='172' labels=frozenset({'Movie'}) properties={'revenue': 15920855, 'title': 'Freaky', 'released': 2020}>>
"""

# deleting data

# delete relationship between two nodes
query = "MATCH (p:Person) -[r:ACTED_IN]-> (m: Movie) " \
        "WHERE p.name = 'Tom Hanks' AND m.title = 'Apollo 13' " \
        "DELETE r"
nodes = session.run(query)

# remove node
"""
query = "MATCH (m:Movie) " \
        "WHERE m.title = 'Apollo 13' " \
        "DELETE m"
nodes = session.run(query)

warning: NOT POSSIBLE REMOVE NODES WITH RELATIONSHIP because make inconsistent graph
DETACH DELETE remove first all relationship, and after remove node
"""
query = "MATCH (m:Movie) " \
        "WHERE m.title = 'Apollo 13' " \
        "DETACH DELETE m"
nodes = session.run(query)

"""
APOC
CALL db.schema.visualization() = view data model
CALL db.schema.nodeTypeProperties() = view property types for NODES
CALL db.schema.relTypeProperties() = view property types for RELATIONSHIPS
SHOW CONSTRAINTS = view the uniqueness constaint indexes in the graph
"""

# find all movies in which the actor "Tom Hanks" starred in 2023
query = "MATCH (p:Person) – [:ACTED_IN]->(m:Movie)" \
        "WHERE p.name = ‘Tom Hanks’" \
        "AND m.year = 2023" \
        "RETURN m.title"
nodes = session.run(query)

# test: return true or false
query = "MATCH (m:Movie) WHERE m.title = ‘Toy Story’ " \
      "RETURN " \
      "m.year < 1995 AS lessThan" \
      "m.year <= 1995 AS lessThanOrEqual" \
      "m.year > 1995 AS moreThan" \
      "m.year >= 1995 AS moreThanOrEqual"
nodes = session.run(query)

# IS NOT NULL / NULL
query = "MATCH (p:Person) WHERE p.died IS NOT NULL AND p.born >= 1985 RETURN p.name, p.born, p.died"
nodes = session.run(query)

# verify if node has a label (p:Actor, p:Director)
query = "MATCH (p:Person)" \
        "WEHRE p.born.year > 1960" \
        "AND p:Actor" \
        "AND p:Director" \
        "RETURN p. name, p.born, labels(p)"
nodes = session.run(query)

# find all people that have directed or acted in the same movie
query = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie)<-[:DIRECTED]-(p) " \
        "WHERE p.born.year > 1960" \
        "RETURN p.name, p.born, labels(p), m.title"
nodes = session.run(query)

# testing strings: starts with, ends with, contains
# toLower(), toUpper()
query = "MATCH (p:Person)" \
        "WHERE toLower(p.name) ENDS WITH ‘demille" \
        "RETURN p.name"
nodes = session.run(query)

"""
EXPLAIN: verify how use the INDEX
EXPLAIN MATCH (m:Movie)
WHERE m.title STARS WITH ‘Toy Story’
RETURN m.title, m.released
"""

"""
PROFILE: show total rows number extract of query
use for analyse performance of queries
"""

# order by
# What is the youngest actor that acted in the most highly-rated movie?
query = "MATCH (p:Person)-[:ACTED_IN]->(m:Movie)" \
        "WHERE p.born.year IS NOT NULL" \
        "AND m.imdbRating IS NOT NULL" \
        "RETURN p.name, p.born.year, m.imdbRating" \
        "ORDER BY m.imdbRating DESC, p.born.year DESC"
nodes = session.run(query)

# limit and skip
query = "MATCH (:Movie) " \
        "WHERE m.released IS NOT NULL " \
        "RETURN m.title AS title, " \
        "m.released AS releaseDate " \
        "ORDER BY m.released DESC LIMIT 100"
nodes = session.run(query)

query = "MATCH (:Movie) " \
        "WHERE m.released IS NOT NULL " \
        "RETURN m.title AS title, " \
        "m.released AS releaseDate " \
        "ORDER BY m.released DESC SKIP 40 LIMIT 100"
nodes = session.run(query)

# distinct
query = "MATCH (m:Movie)<-[:RATED]-()" \
        "RETURN DISTINCT m.title"
nodes = session.run(query)

# What is the lowest imdbRating?
query = "MATCH (m:Movie) " \
        "WHERE m.imdbRating IS NOT NUL" \
        "RETURN m.imdbRatin" \
        "ORDER BY m.imdbRating LIMIT 1"
nodes = session.run(query)

# projections
query = "MATCH (p:Person)" \
        "WHERE p.name CONTAINS ‘Thomas’" \
        "RETURN p {.*} AS person" \
        "ORDER BY p.name ASC"
nodes = session.run(query)

query = "MATCH (p:Person)" \
        "WHERE p.name CONTAINS ‘Thomas’" \
        "RETURN p {.name, .born} AS person" \
        "ORDER BY p.name ASC"
nodes = session.run(query)

# change results: date().year - p.born.year AS ageThisYear
query = "MATCH (m:Movie)<-[:ACTED_IN]-(p:Person) " \
        "WHERE m.title CONTAINS 'Toy Story' AND p.died IS NULL" \
        "RETURN m.title AS movie, p.name AS actor, p.born AS dob," \
        "date().year - p.born.year AS ageThisYear"
nodes = session.run(query)

# change results: CASE
"""
CASE test
  WHEN value THEN result
  [WHEN ...]
  [ELSE default]
END

MATCH (n)
RETURN
CASE n.eyes
  WHEN 'blue'  THEN 1
  WHEN 'brown' THEN 2
  ELSE 3
END AS result
"""

# AGGREGATION
# count()
query = "MATCH (a:Person)-[:ACTED_IN]->(m:Movie)" \
        "WHERE a.name = ‘Tom Hanks’" \
        "RETURN a.name AS actorName, count(*) AS numMovies"
nodes = session.run(query)

