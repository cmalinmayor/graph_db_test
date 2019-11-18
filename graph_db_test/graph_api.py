# TODO: Add way to add/update attributes for existing nodes
# TODO: filter by attribute when reading


class Graph():
    def __init__(self, position_attrs):
        self.position_attrs = position_attrs

    def write_nodes(nodes):
        '''Write nodes to database.

        Args:

            nodes (`NodeList`): a list of nodes to write to database
            Throw an error if position_attrs not in NodeList attrs,
            or if node ids are duplicated
        '''
        raise NotImplementedError("Write nodes not implemented")

    def write_edges(edges):
        '''Write edges to database.

        Args:

            edges (`EdgeList`): a list of edges to write to database
        '''

        raise NotImplementedError("Write edges not implemented")

    def read_nodes(start, offset):
        '''Read nodes in specified region from database.

        Args:

            start (tuple): the start coordiante of the region
                to read from

            offset (tuple): the size of the region to read from

        Returns:

            NodeList containing all nodes in region
        '''
        raise NotImplementedError("Read nodes not implemented")

    def read_edges(start, offset, node_ids=None):
        '''Read nodes in specified region from database.

        Args:

            start (tuple): the start coordiante of the region
                to read from

            offset (tuple): the size of the region to read from

            node_ids (list of int): The node ids for all nodes in the
                region

        Returns:

            NodeList containing all nodes in region
        '''
        raise NotImplementedError("Read edges not implemented")


class NodeList():
    ''' A list of nodes for a graph

    Args:
        attrs (list of strings):
            list of string attribute names that all nodes will have
    '''
    def __init__(self, attrs):
        self.ids = []
        self.attr_names = attrs
        self.attrs = {}
        for attr in self.attr_names:
            self.attrs[attr] = []

    def add_node(self, _id, attrs):
        ''' Add node to node list

        Args:
            _id (int): an integer id for the node

            attrs (dictionary with string keys): the attributes of this node.
                The attributes must match exactly those in the constructor,
                or else an error will be thrown.
        '''
        self.ids.append(_id)
        for key, value in attrs.items():
            if key not in self.attr_names:
                raise ValueError("Key %s not in attribute list %s"
                                 % (key, self.attr_names))
            self.attrs[key].append(value)

    def add_nodes(self, nodes):
        ''' Add nodes to node list

        Args:
            nodes (list of lists): a list of nodes,
                where each node is represented by a list
                where the first element is the id, and the rest are
                the attribute values in order of attr_names

        '''
        for node in nodes:
            assert len(node) == 1 + len(self.attr_names)
            self.ids.append(node[0])
            for i in range(1, len(node)):
                attr = self.attr_names[i-1]
                self.attrs[attr].append(node[i])

    def get_node_ids(self):
        return self.ids

    def get_node_attrs(self):
        return self.attrs


class EdgeList():

    def __init__(self, attrs):
        self.us = []
        self.vs = []
        self.attr_names = attrs
        self.attrs = {}
        for attr in self.attr_names:
            self.attrs[attr] = []

    def add_edge(self, u, v, attrs):
        ''' Add edge to edge list

        Args:
            u (int): id for the source node for the edge

            v (int): id for the target node for the edge

            attrs (dictionary with string keys): the attribtues of this edge.
                The attributes must match exactly those in the constructor,
                or an error will be thrown.
        '''
        self.us.append(u)
        self.vs.append(v)
        for key, value in attrs.items():
            if key not in self.attr_names:
                raise ValueError("Key %s not in attribute list %s"
                                 % (key, self.attr_names))
            self.attrs[key].append(value)

    def add_edges(self, edges):
        ''' Add edges to edge list

        Args:
            edges (list of lists): a list of edges,
                where each edge is represented by a list
                where the first two elements are the source and target ids,
                and the rest are the attribute values in order of attr_names

        '''
        for edge in edges:
            assert len(edge) == 2 + len(self.attr_names)
            self.us.append(edge[0])
            self.vs.append(edge[1])
            for i in range(2, len(edge)):
                attr = self.attr_names[i-2]
                self.attrs[attr].append(edge[i])

    def get_edge_sources(self):
        return self.us

    def get_edge_targets(self):
        return self.vs

    def get_edges(self):
        return list(zip(self.us, self.vs))

    def get_edge_attrs(self):
        return self.attrs
