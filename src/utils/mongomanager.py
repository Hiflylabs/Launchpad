from phoenix.core.pipe import Pipe

from pymongo import MongoClient
from pymongo import ASCENDING
from copy import deepcopy
import pandas as pd
import numpy as np
import gridfs
from pdb import set_trace


class MongoManager(Pipe):
    """Manages calls and queries of MongoDB

    Args:
        host (str): Host name
        port (int): Port number
        db (str): db name (default phoenixpharma in the project)

    """

    def __init__(self, host, port, db='phoenixpharma', *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.db = db

        self.client=None

    def create_client(self):
        """Create Mongo client"""

        try:
            self.client = MongoClient(self.host, self.port, serverSelectionTimeoutMS=10000)
            #Force connection to raise exception in case of any problem
            _ = self.client.server_info()
            self.logger.info(f'MongoClient connected to {self.host}:{self.port}')

        except Exception as e:
            self.logger.error(f'MongoDB connection failed: {e}')
            raise


    def insert(self, data, collection, drop_collection=False):
        """
        Loads data to the given MongoDB collection

        Args:
            data: list, dict or pandas.DataFrame to be inserted
            collection (str):  collection name
            drop_collection (boolean): If True, collection is dropped before insert. Thus it's a full replace.
        Returns:
            None
        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        # pymongo collection.insert() modifies dict in place, thus the copy()!
        data = deepcopy(data)

        try:
            # Drop before insert to recreate collection
            if drop_collection:
                db.drop_collection(collection)

            # insert_one must be called on dict
            if isinstance(data, dict):
                db[collection].insert_one(data)

            # insert_many must be called on a list
            elif isinstance(data, list):
                db[collection].insert_many(data)

            # use insert_many if data is a dataframe
            elif isinstance(data, pd.DataFrame):
                for col in data.dtypes[data.dtypes == 'datetime64[ns]'].index.tolist():
                    data[col] = pd.to_datetime(data[col]).astype(object)
                    data[col] = data[col].where(data[col].notnull(), None)
                data = data.to_dict(orient='records')
                db[collection].insert_many(data)

        except Exception as e:
            self.logger.error('MongoDB insert failed: {}'.format(e))

    def insert_model(self, data, collection):
        """
        Loads a model into the given MongoDB collection

        args:
            Args:
                data: an sklearn model
                collection (str):  collection name
                drop_collection (boolean): If True, collection is dropped before insert. Thus it's a full replace.
        """
        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        fs = gridfs.GridFS(db, collection)
        # pymongo collection.insert() modifies dict in place, thus the copy()!
        # data = deepcopy(data)
        try:
            a = fs.put(data)

        except Exception as e:
            self.logger.error('MongoDB model insert failed: {}'.format(e))
            raise

    def read_model(self, collection, id):
        """Reads a serialized sklearn model from MongoDB.

        Args:
            collection (str): collection name
            id (ObjectId): the id of the file
        Returns:
            list
        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        fs = gridfs.GridFSBucket(db, collection)

        try:
            with fs.open_download_stream(id) as handler:
                out = handler.read()
            self.logger.info(f'[GRIDFS_CHUNK_NUM]: {len(out)} chunks are returned from [COLLECTION]: {collection} with [ID]: {id} ')

        except Exception as e:
            self.logger.error('MongoDB read failed: {}'.format(e))
            raise

        return out


    def delete_model(self, collection, id):
        """Delete moded (GridFS) from collection with id

        NOTE This is tested in model module tests

        Args:
            collection (str): collection name
            id (ObjectId): the id of the file

        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        #Delete model with id
        try:
            fs = gridfs.GridFS(db, collection)
            fs.delete(id)
            self.logger.info(f'Model is deleted from [COLLECTION]: {collection} with [MODEL_ID]: {id}')

        except Exception as e:
            self.logger.error('MongoDB model deletion failed: {}'.format(e))
            raise


    def read(self, collection, filter=None, drop_id=True, limit=0, sort=None):
        """Reads data from MongoDB.

        Args:
            collection (str): collection name
            filter (dict): Filter to be applied
            drop_id (bool): If True, Mongo default _id is dropped from every result
            limit (int): Number of documents to be retrieved. Default 0 means that all records are retrieved.
            sort (list): Valid definition of mongo filtering. E.g. [("field1", pymongo.ASCENDING), ("field2", pymongo.DESCENDING)]. Default None means there is no sort.

        Returns:
            list
        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]


        try:
            results = []

            # If sort is applied
            if sort:
                for item in db[collection].find(filter).limit(limit).sort(sort):
                    # Drop id
                    if drop_id:
                        del item['_id']

                    results.append(item)

            else:
                for item in db[collection].find(filter).limit(limit):
                    # Drop id
                    if drop_id:
                        del item['_id']

                    results.append(item)

            self.logger.info(f'[DOC_NUM]: {len(results)} elements are returned from [COLLECTION]: {collection}')

        except Exception as e:
            self.logger.error('MongoDB read failed: {}'.format(e))
            raise

        return results

    def drop_collection(self, collection):
        """Drop collection.

        Args:
            collection (str): Collection name

        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        try:
            db.drop_collection(collection)

        except Exception as e:
            self.logger.error('MongoDB drop collection failed: {}'.format(e))
            raise

    def delete(self, collection, filter):
        """
        Delete records from the collection based on the filter.

        Args:
            collection (str): name of the collection to delete from
            filter (dict): filter of which records to delete
        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        try:
            db[collection].delete_many(filter)

        except Exception as e:
            self.logger.error('MongoDB insert failed: {}'.format(e))
            raise

    def get_median(self, collection, filters, output_var):
        """
        Returns the median vlaue of a variable.
        Args:
            filters (dict): filters to apply before median calculation
            output_var (str): name of the variable to count the median
        Returns:
            : median values
        """

        #Establish connection
        if self.client is None:
            self.create_client()

        #Connect to db
        db = self.client[self.db]

        try:
            cursor = db[collection]

            cnt = cursor.count_documents(filters)
            med = cursor.find(filters).sort([(output_var, ASCENDING)])
            med = med.skip(int(np.floor(cnt / 2))).limit(1)
            med = pd.DataFrame(list(med))
            out = med[output_var].values[0]
            if isinstance(out, dict):
                out = float([*out.values()][0])

        except KeyError as e:
            self.logger.error(f'MongoDB median calculation failed: {e}')
            return None

        return out
