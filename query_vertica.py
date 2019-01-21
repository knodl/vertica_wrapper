import numpy as np
import pandas as pd
import vertica_python
from read_config import read_yaml_config
import os


class Vertica(object):
    """
    Executes queries from vertica. Extracts rfm data, for each user from rfm data additionally extracts 
    timestamp of the last deal, timestamp of the last demo deal and timestamp of the last session.
    """

    def __init__(self, config):
        self.config = config

    def query_vertica(self, query):
        """
        Queries vertica. Returns list of lists as query result.
        :param query: SQL string
        """
        with vertica_python.connect(**self.config) as conn, conn.cursor() as c:
            c.execute(query)
            result = c.fetchall()
            result = np.array(result)
        return result

    @staticmethod
    def create_dataframe(query_result, colnames):
        """
        Creates pandas dataframe from quering vertica.
        :param query_result: object with vertica query result
        :param colnames: list of column names for pandas
        """
        dataframe = pd.DataFrame(query_result, colnames)
        return dataframe
    