from src.utils.mongomanager import MongoManager

from pdb import set_trace
import pandas as pd
import pymongo

# Network config
host = 'mongo'
port = 27017
db = 'phoenixpharma'

# Initialize MongoManager
mongoman = MongoManager(logname='test', host=host, port=port, db=db)


# TESTS
# =====
def test_read_write_mongo_dict():
    """Test if inserted dictionary is returned the same"""

    test_dict = {'one': 1, 'two': 2}

    mongoman.insert(data=test_dict, collection='test', drop_collection=True)
    res_dict = mongoman.read(collection='test', filter=None, drop_id=True)[0]  # Get the first element in the results

    assert res_dict == test_dict, 'Inserted and read data from MongoDB are different'


def test_read_write_mongo_list():
    """Test if inserted list is returned the same"""

    test_list = [{'one': 1, 'two': 2}, {'one': 1, 'two': 2}]

    mongoman.insert(data=test_list, collection='test', drop_collection=True)
    res_list = mongoman.read(collection='test', filter=None, drop_id=True)[:2]  # Get the first two element in the results

    assert res_list == test_list, 'Inserted and read data from MongoDB are different'


def test_read_write_mongo_dataframe():
    """Test if inserted list is returned the same"""

    test_df = pd.DataFrame([{'one': 1, 'two': 2}]*2)

    mongoman.insert(data=test_df, collection='test', drop_collection=True)
    res_list = mongoman.read(collection='test', filter=None, drop_id=True)[:2]  # Get the first two element in the results
    res_df = pd.DataFrame(res_list)
    assert res_df.equals(test_df), 'Inserted and read data from MongoDB are different'


def test_read_write_mongo_filter():
    """Test if read with filter is functional"""

    test_list = [{'one': 1, 'two': 2}, {'one': 3, 'two': 4}]

    mongoman.insert(data=test_list, collection='test', drop_collection=True)
    res_list = mongoman.read(collection='test', filter={'one': 1}, drop_id=True)[0]  # Get the first element in the results

    assert res_list == test_list[0], 'Inserted and filtered read data from MongoDB are different'


def test_mongo_drop_collection():
    """Test dropping collection"""

    test_list = [{'one': 1, 'two': 2}, {'one': 3, 'two': 4}]

    mongoman.insert(data=test_list, collection='test', drop_collection=True)
    mongoman.drop_collection('test')

    assert True, 'Inserted and filtered read data from MongoDB are different'


def test_get_median():
    """Test median calculation"""

    test_list = [{'one': 1, 'two': 2},
                 {'one': 1, 'two': 4},
                 {'one': 1, 'two': 7},
                 {'one': 6, 'two': 5}]

    mongoman.insert(data=test_list, collection='test')

    med = mongoman.get_median('test', {'one': 1}, 'two')

    mongoman.drop_collection('test')

    assert med == 4


def test_get_median_2():
    """Test median calculation"""

    test_list = [{'one': 1, 'two': 2},
                 {'one': 1, 'two': 4},
                 {'one': 1, 'two': 7},
                 {'one': 1, 'two': 5},
                 {'one': 1, 'two': 9},
                 {'one': 1, 'two': 9},
                 {'one': 6, 'two': 5}]

    mongoman.insert(data=test_list, collection='test')

    med = mongoman.get_median('test', {'one': 1}, 'two')

    mongoman.drop_collection('test')

    assert med == 7


def test_mongo_read_limit():
    """Test if read limit is functional"""

    test_list = [{'one': 1, 'two': 2}, {'one': 3, 'two': 4}]

    mongoman.insert(data=test_list, collection='test', drop_collection=True)
    res_list = mongoman.read(collection='test', drop_id=True, limit=1)  # Get the first element in the results

    assert len(res_list) == 1, f'Number of read documents with limit 1: {len(res_list)}'


def test_mongo_read_sort():
    """Test if read sort is functional"""

    test_list = [{'one': 4, 'two': 2}, {'one': 2, 'two': 4}, {'one': 3, 'two': 2}]

    mongoman.insert(data=test_list, collection='test', drop_collection=True)
    res_list = mongoman.read(collection='test', drop_id=True, sort=[('one', pymongo.ASCENDING)])  # Get the first element in the results

    values = [d['one'] for d in res_list]

    assert values == [2, 3, 4], f'Sorting has failed. Returned value order: {values}'


def test_mongo_delete():
    """Test if delete method works correctly"""

    test_df = pd.DataFrame.from_dict({'one': [1, 1, 1, 1, 1, 2, 2],
                                      'two': ['a', 'a', 'a', 'a', 'a', 'a', 'x'],
                                      'ind': [1, 2, 3, 4, 5, 6, 7]})

    mongoman.insert(data=test_df, collection='test', drop_collection=True)

    mongoman.delete(collection='test', filter={'one': 1})

    result = pd.DataFrame(mongoman.read(collection='test')).sort_values(['one', 'two', 'ind']).reset_index(drop=True)
    expected = pd.DataFrame.from_dict({'one': [2, 2],
                                       'two': ['a', 'x'],
                                       'ind': [6, 7]}).sort_values(['one', 'two', 'ind']).reset_index(drop=True)

    assert all(result.sort_index(axis=1) == expected.sort_index(axis=1)), 'delete method failed'
