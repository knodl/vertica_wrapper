import vertica_python
import json
import os


class Vertica:
    def __init__(self, config_path):
        file_exists = os.path.isfile(config_path)
        if file_exists:
            with open(config_path, 'r') as f:
                self.vertica_config = json.load(f)
        else:
            raise FileNotFoundError('No config file.')

        self._conn = None

    def new_vertica_connection(self):
        return vertica_python.connect(
            host=self.vertica_config['vertica']['host'],
            port=int(self.vertica_config['vertica']['port']),
            user=self.vertica_config['vertica']['user'],
            database=self.vertica_config['vertica']['database'],
            password=self.vertica_config['vertica']['password'],
            ssl=bool(self.vertica_config['vertica']['ssl'])
        )

    @property
    def connection(self):
        if self._conn is None or self._conn.closed():
            self._conn = self.new_vertica_connection()
        return self._conn

    # TODO: use connection.cursor('dict') to fetch colnames
    def query(self, q, return_columns=False):
        with self.connection as c, c.cursor() as cur:
            cur.execute(q)
            result = cur.fetchall()

        if not return_columns:
            return result
        return result, [d.name for d in cur.description]

    def execute(self, q):
        with self.connection as c, c.cursor() as cur:
            cur.execute(q)
            c.commit()

    def executemany(self, insert_query: str, data_list: list):
        with self.connection as c, c.cursor() as cur:
            cur.executemany(insert_query, data_list)

