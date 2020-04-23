from nkmfraud.core.pipe import Pipe
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd


class MySQLWriter(Pipe):
    """Write pandas.DataFrame to a MySQL table

    Args:
        ip (str): The IP address of the MySQL server
        port (str): The port of the MySQL server
        db (str): The db (schema) name
        table (str): The table name
        user (str): User with access to read
        password(str): User password

    Todo:
        Not tested, not finished

    """

    def run(self, ip, port, db, table, user, password, df):

        self.ip = ip
        self.port = port
        self.db = db
        self.table = table
        self.user = user
        self.password = password
        self.df = df

        # Create engine and connect to database
        self.logger.info('Connecting to {}:{}'.format(self.ip,self.port))
        conn_string = 'mysql+mysqlconnector://{}:{}@{}:{}/{}?auth_plugin=mysql_native_password'.format(self.user, self.password, self.ip, self.port,self.db)
        engine = create_engine(conn_string)

        # Write table to sql
        self.logger.info('DataFrame is loaded into {}:{}/{}.{}'.format(self.ip, self.port, self.db, self.table))
        df.to_sql(name=self.table, con=engine, if_exists='replace', index=False)

        # Break connection down
        engine.dispose()
