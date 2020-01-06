import unittest

import psycopg2
from psycopg2 import sql

from ..graph_api import NodeList, EdgeList
from ..postgre.postgre_graph import PostgreGraph

position_attrs = ['z', 'y', 'x']


class TestGraph(unittest.TestCase):

    def graph_io(self, graph):
        nodes = NodeList(attr_names=['z', 'y', 'x', 'fish'])
        nodes.add_nodes(
            [
                [0, 0, 0, 0, "tuna"],
                [1, 0, 0, 1, "swordfish"],
                [3, 1, 1, 5, "salmon"],
                [2, 10, 10, 10, "tuna"]
            ])

        edges = EdgeList(attr_names=['category'])
        edges.add_edges(
            [
                [1, 0, 'a'],
                [1, 2, 'b'],
                [2, 3, 'a'],
                [0, 3, 'c']
            ])

        graph.write_nodes(nodes)
        graph.write_edges(edges)
        start = (0, 0, 0)
        offset = (15, 15, 15)
        output_nodes, output_edges = graph.read_graph(start, offset)

        self.assertCountEqual(output_nodes.get_node_ids(),
                              nodes.get_node_ids())
        self.assertCountEqual(output_edges.get_edges(),
                              edges.get_edges())

    def test_graph_io_mongo(self):
        # TODO: create mongo graph and call graph_io on it
        pass

    def test_graph_io_postgre(self):
        graph = PostgreGraph(conn,
                             node_attr_names=['z', 'y', 'x', 'fish'],
                             node_attr_types=['INTEGER', 'INTEGER', 'INTEGER', 'VARCHAR'],
                             edge_attr_names=['category'],
                             edge_attr_types=['VARCHAR'])
        self.graph_io(graph)

# Connection hard-coded for now...
conn = psycopg2.connect(dbname="vaxenburgr",
                        user="vaxenburgr",
                        password="password",
                        host="10.150.100.18",
                        port="5432")

conn.cursor().execute('DROP TABLE IF EXISTS edges')
conn.cursor().execute('DROP TABLE IF EXISTS nodes')

unittest.main()
# conn.close()
# if __name__ == '__main__':
#     unittest.main()
