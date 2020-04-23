from nkmfraud.core.pipe import Pipe
from sqlalchemy import create_engine
import pandas as pd
from pdb import set_trace
import numpy as np

class MySQLReader(Pipe):
    """Load MySQL table as a pandas.DataFrame.

    Args:
        ip (str): The IP address of the MySQL server
        port (str): The port of the MySQL server
        db (str): The db (schema) name
        table (str): The table name
        user (str): User with access to read
        password(str): User password
        rowlim (int): Number of rows to be read from the table.
        chunksize (int): Number of rows to be read in one chunk. Full table (with rowlim) is read if chunksize is None

    Returns:
        pandas.DataFrame

    """
    @Pipe.timeit
    def run(self, ip, port, db, table, user, password, rowlim=None, chunksize=None):

        # Create engine and connect to database
        self.logger.info('Connecting to {}:{}'.format(ip,port))
        # conn_string = 'mysql+mysqlconnector://{}:{}@{}:{}/?auth_plugin=mysql_native_password'.format(user, password, ip, port)
        conn_string = 'mysql+mysqldb://{}:{}@{}:{}'.format(user, password, ip, port)
        engine = create_engine(conn_string)

        #To shut down process on failure
        failed_load = False

        try:
            #Read in chunks         
            if chunksize is not None:
                
                #Get row numbers
                query = 'SELECT count(*) FROM {}.{}'.format(db, table)
                self.logger.info('Executing query: {}'.format(query))
                if rowlim is None:
                    rowlim = pd.read_sql(query, con=engine).values[0][0]
                self.logger.warning('{} is read in chunks with size {}. Number of total rows: {}'.format(table, chunksize, rowlim))

                df_cont = []
                for startindex in np.arange(0, rowlim, chunksize):
                    query = 'SELECT * FROM {}.{} LIMIT {},{};'.format(db, table, startindex, chunksize)
                    self.logger.info('Executing query: {}'.format(query))
                    # with engine.connect() as con:
                    #     res = con.execute(query)
                    #     table_df = pd.DataFrame(res.fetchall(), columns=res.keys())
                    table_df = pd.concat(pd.read_sql(query, con=engine, chunksize=100000))
                    self.logger.info('Memory usage of the loaded DataFrame: {}'.format(table_df.memory_usage(index=True).sum()))
                    df_cont.append(table_df)

                table_df = pd.concat(df_cont, axis=0)
                
            #Read whole table
            else:
                if rowlim:
                    query = 'SELECT * FROM {}.{} LIMIT {}'.format(db,table, rowlim)
                else:
                    query = 'SELECT * FROM {}.{}'.format(db,table)
                
                self.logger.info('Executing query: {}'.format(query))
                table_df = pd.concat(pd.read_sql(query, con=engine, chunksize=100000))
                self.logger.info('Memory usage of the loaded DataFrame: {}'.format(table_df.memory_usage(index=True).sum()))

        except:
            self.logger.exception('Table reading failed')
            failed_load=True

        finally:
            engine.dispose()

        if failed_load:
            raise Exception('Reading from table {} failed'.format(table))

        return table_df
