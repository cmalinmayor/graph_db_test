import psycopg2
from psycopg2 import sql

# from graph_db_test.graph_api import NodeList, EdgeList
# from graph_db_test.postgre.postgre_graph import PostgreGraph
from ..graph_api import NodeList, EdgeList
from ..postgre.postgre_graph import PostgreGraph

# from ~/code/graph_db_test/graph_db_test> run $python3 -m test_scripts.test_direct
# ~/code/graph_db_test>python3 -m graph_db_test.test_scripts.test_basic_functionality

# try:
#     postgre_graph.cur.close()
#     postgre_graph.conn.close()
# except:
#     pass

# Connection hard-coded for now...
conn = psycopg2.connect(dbname="vaxenburgr",
                        user="vaxenburgr",
                        password="password",
                        host="10.150.100.18",
                        port="5432")

conn.cursor().execute('DROP TABLE IF EXISTS edges')
conn.cursor().execute('DROP TABLE IF EXISTS nodes')

nodes = NodeList(attr_names=['z', 'y', 'x', 'fish'])
nodes.add_nodes(
    [
        [0, 0, 0, 0, "tuna"],
        [1, 0, 0, 1, "swordfish"],
        [3, 1, 1, 5, "salmon"],
        [2, 10, 10, 10, "tuna"]
    ])
print('ids:', nodes.ids) # self.ids = []
print('attr_names:', nodes.attr_names) # self.attr_names = attr_names
print('attrs:', nodes.attrs) # self.attrs = {}

edges = EdgeList(attr_names=['category'])
edges.add_edges(
    [
        [1, 0, 'a'],
        [1, 2, 'b'],
        [2, 3, 'a'],
        [0, 3, 'c']
    ])
print('us:', edges.us)
print('vs:', edges.vs)
print('attr_names:', edges.attr_names)
print('attrs:', edges.attrs)

postgre_graph = PostgreGraph(conn,
                             ['z', 'y', 'x', 'fish'],
                             ['INTEGER', 'INTEGER', 'INTEGER', 'VARCHAR'],
                             ['type'],
                             ['VARCHAR'])

postgre_graph.write_nodes(nodes)
postgre_graph.write_edges(edges)

start = (0, 0, 0)
offset = (15, 15, 15)

output_nodes = postgre_graph.read_nodes(start, offset)
print('output_nodes.ids:', output_nodes.ids)

output_edges = postgre_graph.read_edges(start, offset, node_ids=output_nodes.ids)
print('output_edges.us:', output_edges.us)
print('output_edges.vs:', output_edges.vs)
print('output_edges.attrs:', output_edges.attrs)

output_edges = postgre_graph.read_edges(start, offset)
print('output_edges.us:', output_edges.us)
print('output_edges.vs:', output_edges.vs)
print('output_edges.attrs:', output_edges.attrs)

output_nodes, output_edges = postgre_graph.read_graph(start, offset)
print('output_nodes.ids:', output_nodes.ids)
print('output_edges.us:', output_edges.us)
print('output_edges.vs:', output_edges.vs)
print('output_edges.attrs:', output_edges.attrs)

conn.commit()
postgre_graph.cur.close()
postgre_graph.conn.close()

print('Done. Connection closed.')
