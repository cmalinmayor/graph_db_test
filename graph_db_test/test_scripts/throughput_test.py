import random
import time
from ..graph_api import NodeList, EdgeList


# TODO: Add num workers
def test_througput(graph, num_nodes, num_edges):

    roi_start = (0, 0, 0)
    roi_size = (1000, 1000, 1000)

    nodes = []
    for i in range(num_nodes):
        nodes.append([
            i,
            random.randint(roi_start[0], roi_size[0] + roi_start[0] - 1),
            random.randint(roi_start[1], roi_size[1] + roi_start[1] - 1),
            random.randint(roi_start[2], roi_size[2] + roi_start[2] - 1)
            ])
    nodes_list = NodeList(['z', 'y', 'x'])
    nodes_list.add_nodes(nodes)

    edges = []
    for _ in range(num_edges):
        edges.append([
           random.randint(0, num_nodes - 1),
           random.randint(0, num_nodes - 1)
        ])
    edges_list = EdgeList(attrs=[])
    edges_list.add_edges(edges)

    start_time = time.time()
    graph.write_nodes(nodes_list)
    print("Took %d seconds to write %d nodes"
          % (time.time() - start_time, num_nodes))

    start_time = time.time()
    graph.write_edges(edges_list)
    print("Took %d seconds to write %d edges"
          % (time.time() - start_time, num_edges))

    start_time = time.time()
    graph.read_graph()
    print("Took %d seconds to read %d nodes and %d edges"
          % (time.time() - start_time, num_nodes, num_edges))


if __name__ == '__main__':
    # TODO: Create graphs and call with different numbers of workers to compare
    # performance in highly parallell environments
    pass
