from ..graph_api import Graph


class MongoGraph(Graph):
    def __init__(
            self,
            position_attrs,
            db_name,
            db_host):
        super().__init__(position_attrs)
        self.db_name = db_name
        self.db_host = db_host

    # TODO: implement functions from graph api
