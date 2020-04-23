from src.utils.postgresmanager import PostgresManager

from pdb import set_trace
import datetime
import pandas as pd
import pytest

# Network config
host = 'postgres'
port = 5432
db = 'phoenixpharma'
user = 'postgres'
password = 'XXX'

# Initialize PostgresManager
postgresman = PostgresManager(logname='test', host=host, port=port, db=db, user=user, password=password)

# Create test eta df
eta_test = pd.DataFrame([
    {'PHARMACY_ID': 1, 'ETA_ID': '1', 'TOUR_ID': 102, 'ETA': datetime.datetime.now()},
    {'PHARMACY_ID': 31, 'ETA_ID': '2', 'TOUR_ID': 114, 'ETA': datetime.datetime.now()},
    {'PHARMACY_ID': 41, 'ETA_ID': '3', 'TOUR_ID': 115, 'ETA': datetime.datetime.now()},
    {'PHARMACY_ID': 11, 'ETA_ID': '4', 'TOUR_ID': 14, 'ETA': datetime.datetime.now()}
])


@pytest.fixture()
def setup():
    postgresman.truncate(table_name='eta')
    test_df = pd.DataFrame().from_dict({'PHARMACY_ID': [1, 1, 1, 2],
                                        'ETA_ID': ['1', '2', '3', '1'],
                                        'TOUR_ID': [103, 103, 103, 107],
                                        'ETA': [datetime.datetime(2019, 1, 1, 11, 0, 0),
                                                datetime.datetime(2019, 1, 1, 11, 10, 0),
                                                datetime.datetime(2019, 1, 1, 11, 20, 0),
                                                datetime.datetime(2019, 1, 1, 11, 0, 0)]})

    postgresman.write(test_df, 'eta')


# TESTS
# =====
def test_write_read_postgres():
    """Test if written dataframe is returned the same"""

    # Read and write table
    postgresman.write(df=eta_test, table_name='eta')
    eta_resp = postgresman.read(table_name='eta')

    assert eta_resp.equals(eta_test), 'Written and read data from PostgreSQL are different'


def test_update_eta_postgres():
    """Test if updated dataframe is returned the same"""

    # Write table
    postgresman.write(df=eta_test, table_name='eta')

    # New eta df
    eta_test_update = pd.DataFrame([
        {'PHARMACY_ID': 1, 'ETA_ID': '1', 'TOUR_ID': 102, 'ETA': datetime.datetime.now()},
        {'PHARMACY_ID': 31, 'ETA_ID': '2', 'TOUR_ID': 114, 'ETA': datetime.datetime.now()},
        {'PHARMACY_ID': 41, 'ETA_ID': '3', 'TOUR_ID': 115, 'ETA': datetime.datetime.now()},
        {'PHARMACY_ID': 11, 'ETA_ID': '4', 'TOUR_ID': 14, 'ETA': datetime.datetime.now()},
        {'PHARMACY_ID': 115, 'ETA_ID': '4', 'TOUR_ID': 14, 'ETA': datetime.datetime.now()}  # This is a new record to append
    ])

    # Update table
    postgresman.update_eta(eta_test_update, 'eta')

    # Read table
    eta_resp = postgresman.read(table_name='eta')

    assert eta_resp.equals(eta_test_update), 'Written and read data from PostgreSQL are different'


def test_truncate_postgres():
    """Test table truncate"""

    # Write table
    postgresman.write(df=eta_test, table_name='eta')
    # Truncate table
    postgresman.truncate(table_name='eta')

    # Read data
    df = postgresman.read(table_name='eta')

    assert df.empty, f'Table is not truncated, returned df head: {df.head(3)}'


def test_update_eta_new_id(setup):
    """Test update method when only new ID-s present"""

    to_load = pd.DataFrame().from_dict({'PHARMACY_ID': [1, 1],
                                        'ETA_ID': ['4', '5'],
                                        'TOUR_ID': [103, 103],
                                        'ETA': [datetime.datetime(2019, 1, 1, 11, 30, 0),
                                                datetime.datetime(2019, 1, 1, 11, 40, 0)]})
    postgresman.update_eta(to_load, 'eta')

    result = postgresman.read('eta').sort_values(['PHARMACY_ID', 'ETA_ID', 'TOUR_ID', 'ETA']).reset_index(drop=True)
    expected = pd.DataFrame().from_dict({'PHARMACY_ID': [1, 1, 1, 1, 1, 2],
                                         'ETA_ID': ['1', '2', '3', '4', '5', '20'],
                                         'TOUR_ID': [103, 103, 103, 103, 103, 107],
                                         'ETA': [datetime.datetime(2019, 1, 1, 11, 0, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 10, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 20, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 30, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 40, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 0, 0)]})
    expected = expected.sort_values(['PHARMACY_ID', 'ETA_ID', 'TOUR_ID', 'ETA']).reset_index(drop=True)

    assert all(result == expected), 'Inserting with new ID-s is not working properly'


def test_update_eta_old_id(setup):
    """Test update method when only already existing ID-s present"""

    to_load = pd.DataFrame().from_dict({'PHARMACY_ID': [1, 3],
                                        'ETA_ID': ['1', '1'],
                                        'TOUR_ID': [103, 103],
                                        'ETA': [datetime.datetime(2019, 1, 1, 11, 30, 0),
                                                datetime.datetime(2019, 1, 1, 11, 40, 0)]})
    postgresman.update_eta(to_load, 'eta')

    result = postgresman.read('eta').sort_values(['PHARMACY_ID', 'ETA_ID', 'TOUR_ID', 'ETA']).reset_index(drop=True)
    expected = pd.DataFrame().from_dict({'PHARMACY_ID': [1, 1, 1, 3],
                                         'ETA_ID': ['1', '2', '3', '1'],
                                         'TOUR_ID': [103, 103, 103, 103],
                                         'ETA': [datetime.datetime(2019, 1, 1, 11, 30, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 10, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 20, 0),
                                                 datetime.datetime(2019, 1, 1, 11, 40, 0)]})
    expected = expected.sort_values(['PHARMACY_ID', 'ETA_ID', 'TOUR_ID', 'ETA']).reset_index(drop=True)

    assert all(result.sort_index(axis=1) == expected.sort_index(axis=1)), 'Inserting with already existing ID-s is not working properly'


def test_drop_old_recs():
    """Test drop_old_recs method"""

    now = datetime.datetime.now()
    df = pd.DataFrame().from_dict({'one': [1, 2, 3, 4, 5, 6],
                                   'two': ['a', 'b', 'c', 'd', 'e', 'f'],
                                   'ts': [now,
                                          now - datetime.timedelta(minutes=2),
                                          now - datetime.timedelta(minutes=15),
                                          now - datetime.timedelta(days=10),
                                          datetime.datetime(2010, 1, 1, 0, 0, 0),
                                          datetime.datetime(2033, 1, 1, 0, 0, 0)]})

    postgresman.write(df, 'test_drop_old_recs')
    postgresman.drop_old_recs('test_drop_old_recs', 'ts', delta_min=10)
    result = postgresman.read('test_drop_old_recs')

    expected = pd.DataFrame().from_dict({'one': [1, 2, 6],
                                         'two': ['a', 'b', 'f'],
                                         'ts': [now,
                                                now - datetime.timedelta(minutes=2),
                                                datetime.datetime(2033, 1, 1, 0, 0, 0)]})

    assert all(result == expected)


def test_delete(setup):
    """Test delete method."""

    postgresman.delete('eta', {'TOUR_ID': 103, 'ETA_ID': '2'})
    result = postgresman.read('eta')

    expected = pd.DataFrame().from_dict({'PHARMACY_ID': [1, 1, 2],
                                        'ETA_ID': ['1', '3', '1'],
                                        'TOUR_ID': [103, 103, 107],
                                        'ETA': [datetime.datetime(2019, 1, 1, 11, 0, 0),
                                                datetime.datetime(2019, 1, 1, 11, 20, 0),
                                                datetime.datetime(2019, 1, 1, 11, 0, 0)]})

    assert all(result == expected)
