from nkmfraud.core.pipe import Pipe
from sqlalchemy import create_engine
import pandas as pd


class MSSQLReader(Pipe):
    """Pipe to read and MSSQL table to a pandas.DataFrame"""

    @Pipe.timeit
    def run(self, ip, db, table):
        """The Pipe uses Windows Authentication to connect to MSSQL server. Log in to VPN if it's required.

        Args:
            ip (str): The IP address of the MSSQL server (Test server: 10.10.10.140\SQL2017).
            db (str): Name of the database (Test db: NKM_fraud).
            table (str): SQL table to read.

        Returns:
            pandas.DataFrame
        """

        # Create engine and connect to database
        engine = create_engine('mssql+turbodbc://{}/{}?driver=SQL+Server+Native+Client+11.0'.format(ip, db))

        # Read the defined table and dispose the engine
        table_df = pd.read_sql_table(table_name=table, con=engine)
        engine.dispose()
        self.logger.info('pandas.DataFrame is read from {}/{}.{}'.format(ip, db, table))

        return table_df
