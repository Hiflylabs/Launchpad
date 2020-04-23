import sys
sys.path.append('...')

from nkmfraud.core.pipe import Pipe
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class MSSQLWriter(Pipe):
    """Write pandas.DataFrame to an MSSQL table"""

    @Pipe.timeit
    def run(self, ip, db, table, df):
        """The Pipe uses Windows Authentication to connect to MSSQL server. Log in to VPN if it's required.

        If the a table with the same name (table argument) already exists, it will be overwritten.
        The index of the DataFrame is not written.

        Args:
            ip (str): The IP address of the MSSQL server (Test server: 10.10.10.140\SQL2017).
            db (str): Name of the database (Test db: NKM_fraud).
            table (str): Name of the table where the DataFrame will be written.
            df (pandas.DataFrame): DataFrame to be written to MSSQL

        """

        # Create engine and connect to database
        engine = create_engine('mssql+turbodbc://{}/{}?driver=SQL+Server+Native+Client+11.0'.format(ip, db))

        # Replace - and + inf with np.nan
        df = df.replace([np.inf, -np.inf], np.nan)

        # Write table to SQL and dispose the engine
        df.to_sql(name=table, con=engine, if_exists='replace', index=False)
        engine.dispose()
        self.logger.info('pandas.DataFrame is written to {}/{}.{}'.format(ip, db, table))
