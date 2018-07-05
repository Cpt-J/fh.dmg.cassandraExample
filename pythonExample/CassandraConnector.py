# pip install cassandra-driver
# docker run --name name -p 9042:9042 cassandra:latest

from cassandra.cluster import Cluster

cluster = Cluster(['localhost'], port = 9042)

session = cluster.connect()

rows = session.execute("Select keyspace_name from system_schema.keyspaces;")

for row in rows:
    print(row)

