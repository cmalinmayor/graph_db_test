import psycopg2
from psycopg2 import sql

from ..graph_api import Graph
from ..graph_api import NodeList, EdgeList

class PostgreGraph(Graph):
    """...

    Public methods:
        add_node_attrs: Adds new attributes (columns) to 'nodes' table.
        update_nodes: Updates attributes (columns) for given nodes (rows).
        write_nodes: Writes new nodes to database.
        write_edges: Writes new edges to database.
        read_nodes: Reads and returns nodes in specified region from database.
        read_edges: Reads and returns edges in specified region from database.
        read_graph: Wrapper for simultaneous read_nodes and read_edges.
    """

    def __init__(self, conn, node_attr_names, node_attr_types, edge_attr_names, edge_attr_types):
        '''Initializes PostgreGraph object. Creates two tables: 'nodes', 'edges'.

        Args:
            conn (psycopg2.connect object): Database session.
            node_attr_names (list of strings): Node attribute names that all nodes will have.
            node_attr_types (list of strings): Postgres types of node attributes
                ('INTEGER', 'REAL', 'VARCHAR', 'BOOL', etc.).
            edge_attr_names (list of string): Edge attribute names (in addition to u, v).
            edge_attr_types (list of strings): Postgres types of edge attributes
                ('INTEGER', 'REAL', 'VARCHAR', 'BOOL', etc.).
        '''
        super().__init__(node_attr_names) # DO WE NEED THIS???
        self.conn = conn
        self.cur = self.conn.cursor()
        self.node_attr_names = ['id'] + node_attr_names
        self.node_attr_types = ['INTEGER PRIMARY KEY'] + node_attr_types
        self.edge_attr_names = ['u', 'v'] + edge_attr_names
        self.edge_attr_types = ['INTEGER REFERENCES nodes(id)', 'INTEGER REFERENCES nodes(id)'] + edge_attr_types
        create_query = self.get_create_query('nodes', self.node_attr_names, self.node_attr_types)
        self.cur.execute(create_query) # Create table 'nodes'
        create_query = self.get_create_query('edges', self.edge_attr_names, self.edge_attr_types)
        self.cur.execute(create_query) # Create table 'edges'

    def add_node_attrs(self, node_attr_names, node_attr_types):
        """Adds new node attributes (columns) to the 'nodes' table.

        Args:
            node_attr_names (list of strings): New node attribute names that
                all nodes will have.
            node_attr_types (list of strings): Postgres types of new node
                attributes ('INTEGER', 'REAL', 'VARCHAR', 'BOOL', etc.).
        """
        self.node_attr_names += node_attr_names # update list of attributes
        self.node_attr_types += node_attr_types
        query = "ALTER TABLE nodes"
        for attr_, type_ in zip(node_attr_names, node_attr_types):
            query += " ADD COLUMN " + attr_ + " " + type_ + ","
        query = query[:-1] # remove comma
        self.cur.execute(query)

    def update_nodes(self, ids, attrs, values):
        """Updates attributes (columns) for given nodes (rows).

        Args:
            ids (list): List of node ids to update.
            attrs (list): List of attributes to update for each id.
            values (list of lists): New values to insert, where:
                len(values) = len(ids), len(values[0]) = len(attrs).
        Raises:
            TODO: Check that all 3 input lists are of the same length.
            TODO: Check that all ids exit in table
            TODO: Check that all attrs exist in table.
        """
        for id, values_ in zip(ids, values):
            query = "UPDATE nodes SET"
            for attr in attrs:
                query += f" {attr} = (%s),"
            query = query[:-1] # remove comma
            query += " WHERE id = (%s)"
            # print("UPDATE QUERY:", query)
            # print("VALUES:", values_ + [id])
            self.cur.execute(query, values_ + [id])

    def write_nodes(self, nodes):
        '''Writes new nodes to database.

        Args:
            nodes (`NodeList`): a list of nodes to write to database

        Raises:
            TODO: Throw an error if position_attrs not in NodeList attrs.
            TODO: Throw an error if node ids are duplicated.
        '''
        insert_query = self.get_insert_query('nodes', self.node_attr_names)
        for i in range(len(nodes.ids)):
            values = [nodes.ids[i]] + [nodes.attrs[attr][i] for attr in nodes.attr_names]
            self.cur.execute(insert_query, values)

    def write_edges(self, edges):
        '''Write edges to database.
        Args:
            edges (`EdgeList`): a list of edges to write to database
        '''
        insert_query = self.get_insert_query('edges', self.edge_attr_names)
        for i in range(len(edges.us)):
            values = [edges.us[i], edges.vs[i]] + [edges.attrs[attr][i] for attr in edges.attr_names]
            self.cur.execute(insert_query, values)

    def read_nodes(self, start, offset):
        '''Read nodes in specified region from database.
        Args:
            start (tuple): the start coordiante of the region to read from
            offset (tuple): the size of the region to read from
        Returns:
            NodeList containing all nodes in region
        '''
        select_query = sql.SQL('SELECT * FROM nodes WHERE {} AND {}').format(
            sql.SQL(' and ').join([sql.Identifier(col) + sql.SQL('>=') + sql.Placeholder()
                                   for col in self.node_attr_names[1:len(start)+1]]), # skip id column
            sql.SQL(' and ').join([sql.Identifier(col) + sql.SQL('<=') + sql.Placeholder()
                                   for col in self.node_attr_names[1:len(offset)+1]]),
        )
        values = [x for x in start] + [x+y for x, y in zip(start, offset)]
        self.cur.execute(select_query, values)
        nodes = NodeList(attr_names=self.node_attr_names[1:]) # skip id column
        nodes.add_nodes(self.cur.fetchall())
        return nodes

    def read_edges(self, start, offset, node_ids=None):
        '''Read edges in specified region from database.
        Args:
            start (tuple): the start coordiante of the region to read from
            offset (tuple): the size of the region to read from
            node_ids (list of int): The node ids for all nodes in the region
        Returns:
            EdgeList containing all edges in region
        '''
        if node_ids is not None:
            # Get edges for passed node_ids, no (start, offset) needed
            # Use 'OR' or 'AND' in the query depending on required kind of edges
            self.cur.execute('SELECT * FROM edges WHERE u IN %s OR v IN %s', [tuple(node_ids),
                                                                              tuple(node_ids)])
            edges = EdgeList(attr_names=self.edge_attr_names[2:]) # skip u,v columns
            edges.add_edges(self.cur.fetchall())
            return edges
        else:
            # node_ids is None, using (start, offset) to get nodes first
            nodes = self.read_nodes(start, offset)
            return self.read_edges(start, offset, node_ids=nodes.ids)

    def read_graph(self, start, offset):
        '''Wrapper for read_nodes and read_edges.
        Args:
            start (tuple): the start coordiante of the region to read from
            offset (tuple): the size of the region to read from
        Returns:
            NodeList and EdgeList containing all edges in region
        '''
        nodes = self.read_nodes(start, offset)
        edges = self.read_edges(start, offset, node_ids=nodes.ids)
        return nodes, edges

    def get_create_query(self, table_name, column_names, column_types):
        '''Generate create table query for cursor.execute(query, vars).
        Args:
            table_name (string)
            column_name (list of strings)
            column_types (list of strings):
                Postgres types of columns ('INTEGER', 'REAL', 'VARCHAR', 'BOOL', etc.)
        Returns:
            Create table query for cursor.execute(query, vars)
        '''
        create_query = sql.SQL("CREATE TABLE {} ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join([sql.Identifier(col) + sql.SQL(' ') + sql.SQL(type_)
                                for col, type_ in zip(column_names, column_types)])
        )
        return create_query

    def get_insert_query(self, table_name, column_names):
        '''Generate insert query for cursor.execute(query, vars).
        Args:
            table_name (string)
            column_names (list of stirngs)
        Returns:
            Insert query for cursor.execute(query, vars)
        '''
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.SQL(table_name),
            sql.SQL(', ').join([sql.Identifier(col) for col in column_names]),
            sql.SQL(', ').join([sql.Placeholder()] * len(column_names))
        )
        return insert_query

    def print_nodes(self):
        """Prints the 'nodes' table."""
        self.cur.execute("SELECT * FROM nodes")
        print('Table columns:', [d[0] for d in self.cur.description])
        print('Table rows:', self.cur.fetchall())
