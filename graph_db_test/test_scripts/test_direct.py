import argparse

import psycopg2
from psycopg2 import sql

# from graph_db_test.graph_api import NodeList, EdgeList
# from graph_db_test.postgre.postgre_graph import PostgreGraph
from ..graph_api import NodeList, EdgeList
from ..postgre.postgre_graph import PostgreGraph

# from ~/code/graph_db_test/graph_db_test> run $python3 -m test_scripts.test_direct --pw=...
# ~/code/graph_db_test>python3 -m graph_db_test.test_scripts.test_basic_functionality

# try:
#     postgre_graph.cur.close()
#     postgre_graph.conn.close()
# except:
#     pass

parser = argparse.ArgumentParser()
parser.add_argument('--pw', type=str, required=True)
args = parser.parse_args()

# Connection hard-coded for now...
conn = psycopg2.connect(dbname="vaxenburgr",
                        user="vaxenburgr",
                        password=args.pw,
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

# ADD NEW ATTRIBUTES AND NEW NODES
print('-------')
print('BOFORE ADDING NEW ATTRIBUTES:')
postgre_graph.print_nodes()
postgre_graph.add_node_attrs(['new1', 'new2'], ['VARCHAR', 'BOOL'])
print('SAME TABLE WITH NEW ATTRIBUTES ADDED:')
postgre_graph.print_nodes()

new_nodes = NodeList(attr_names=['z', 'y', 'x', 'fish', 'new1', 'new2'])
new_nodes.add_nodes(
    [
        [4, 1, 2, 3, "zebrafish", "blue", True],
        [5, 4, 5, 6, "goldfish", "green", False],
    ])
postgre_graph.write_nodes(new_nodes) # write new nodes
print('AFTER WRITING NEW NODES WITH NEW ATTRIBUTES:')
postgre_graph.print_nodes()
# output_nodes = postgre_graph.read_nodes(start, offset)
# print('output_nodes.ids:', output_nodes.ids)

ids = [4, 5]
attrs = ['new1', 'new2']
values = [['black', False], ['white', True]]
print('BEFORE UPDATING NODES:')
postgre_graph.print_nodes()
postgre_graph.update_nodes(ids, attrs, values)
print('AFTER UPDATING NODES:')
postgre_graph.print_nodes()

conn.commit()
postgre_graph.cur.close()
postgre_graph.conn.close()

print('Done. Connection closed.')
