from phoenix.core.pipe import Pipe

import pandas as pd
import datetime as dt
from sqlalchemy import create_engine, Table, MetaData, and_
from sqlalchemy.sql.expression import bindparam
from pdb import set_trace


class PostgresManager(Pipe):
    """Manages reading & writing of external Postgres databases

    Args:
        host (str): Host name
        port (int): Port number
        db (str): Database name
        user (str): Username
        password (str): Password

    """

    def __init__(self, host, port, db, user, password, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password

        self.engine = None

    def create_engine(self):
        """Connect to PosgreSQL server"""

        try:
            # Create engine and connect to database
            conn_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.db)
            self.engine = create_engine(conn_string, connect_args={'connect_timeout': 10})
            self.logger.info('Connected to {}:{}'.format(self.host, self.port))

        except Exception as e:
            self.logger.error('Connecting to {}.{} failed: {}'.format(self.host, self.port, e))
            raise

    def write(self, df, table_name, dtype=None):
        """Write pandas DataFrame to PostgreSQL server. An existing table is always replaced.

        Args:
            table_name (str): Name of the table
            df (pd.DataFrame): pandas DataFrame to be written
            dtype (dict): type specifictaion

        """

        # Check if connection exists
        if self.engine is None:
            self.create_engine()

        with self.engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, dtype=dtype, if_exists='replace', index=False, chunksize=10000)
            self.logger.info(f'DataFrame is loaded into {self.db}.{table_name} with shape {df.shape}')

    def update_eta(self, eta_update_df, table_name):
        """Update PosgreSQL ETA output, and insert new records if it not exists.

        Args:
            table_name (str): Name of the table
            eta_update_df (pd.DataFrame): pandas dataframe with the updateable data
        """

        # Check if connection exists
        if self.engine is None:
            self.create_engine()

        # Do update
        with self.engine.connect() as conn:

            # Drop all predictions with the current ETA ID
            for id in eta_update_df.SPRINT_ID.unique():
                metadata = MetaData(bind=None)
                eta = Table(table_name, metadata, autoload=True, autoload_with=conn)
                stmt = eta.delete().where(eta.c.SPRINT_ID == id)
                conn.execute(stmt)
                self.logger.info(f'Record with [SPRINT_ID]: {id} is deleted from ETA live table')

            # Write records
            eta_update_df.to_sql(name=table_name, con=conn, if_exists='append', index=False, chunksize=10000)
            self.logger.info(f'Records with [SPRINT_ID]: {id} inserted to ETA live table. ROWNUM: {len(eta_update_df)}')

    def read(self, table_name):
        """Read table from PostgreSQL

        Args:
            table_name (str): Name of the table
        """

        # Check if connection exists
        if self.engine is None:
            self.create_engine()

        df = None

        with self.engine.begin() as conn:
            query = 'SELECT * FROM {}'.format(table_name)
            self.logger.info('Executing query: {}'.format(query))
            df = pd.read_sql(query, conn)
            self.logger.info(f'{self.db}.{table_name} is loaded with shape: {df.shape}')

        return df

    def truncate(self, table_name):
        """Truncate table

        Args:
            table_name (str): Name of the table
        """

        # Check if connection exists
        if self.engine is None:
            self.create_engine()

        with self.engine.begin() as conn:
            query = 'TRUNCATE TABLE {}'.format(table_name)
            self.logger.info('Executing query: {}'.format(query))
            conn.execute(query)
            self.logger.info(f'{self.db}.{table_name} is truncated')


    def drop_old_recs(self, table_name, col, delta_min=10):
        """Delete all records which are older than the given time period.

        Args:
            table_name (str): name of the table to drop the records from
            col (str): name of the table with timestamp
            delta_min (int): drop records older than this param
        """

        # Check if connection exists
        if self.engine is None:
            self.create_engine()

        with self.engine.begin() as conn:
            metadata = MetaData(bind=None)
            table = Table(table_name, metadata, autoload=True, autoload_with=conn)
            too_old = (dt.datetime.now() - dt.timedelta(minutes=delta_min)).strftime('%Y-%m-%d %H:%M:%S.%f %Z') + ' Europe/Budapest'
            conn.execute(table.delete().where(table.c[col] <= too_old))
            self.logger.info(f'Records where {col} is older than {too_old} are deleted')


    def truncate_insert(self, table_name, df):
        """Truncate table then inert records

        Args:
            table_name (str): name of the table to do insert into
            df (pd.DataFrame): DataFrame to be inserted
        """

        #Check if connection exists
        if self.engine is None:
            self.create_engine()

        with self.engine.begin() as conn:
            self.truncate(table_name)
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False, chunksize=10000)
            self.logger.info(f'Data is truncate-inserted into {table_name} with {df.shape[0]} records')

    def delete(self, table_name, filter):
        """Delete records from the postgres table.

        Args:
            collection (str): name of the collection where the deletion will take
                place
            filter (dict): filter of which records to delete

        """

        #Check if connection exists
        if self.engine is None:
            self.create_engine()

        with self.engine.begin() as conn:
            metadata = MetaData(bind=None)
            table = Table(table_name, metadata, autoload=True, autoload_with=conn)
            terms = [table.c[x] == filter[x] for x in filter.keys()]
            stmt = table.delete().where(and_(*terms))
            conn.execute(stmt)
            self.logger.info(f'Data is deleted from {table_name} on filter {filter}')
