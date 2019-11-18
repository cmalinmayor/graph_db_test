import unittest
from ..graph_api import NodeList, EdgeList

position_attrs = ['z', 'y', 'x']


class TestGraph(unittest.TestCase):

    def graph_io(self, graph):
        nodes = NodeList(attrs=['z', 'y', 'x', 'fish'])
        nodes.add_nodes(
            [
                [0, 0, 0, 0, "tuna"],
                [1, 0, 0, 1, "swordfish"],
                [3, 1, 1, 5, "salmon"],
                [2, 10, 10, 10, "tuna"]
            ])

        edges = EdgeList(attrs=['category'])
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
        # TODO: create postgre graph and call graph_io on it
        pass
