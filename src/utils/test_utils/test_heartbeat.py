from src.utils.postgresmanager import PostgresManager
from src.utils.heartbeat import HeartBeat

from pdb import set_trace
import datetime
import pandas as pd
import time

# Network config
host = 'postgres'
port = 5432
db = 'phoenixpharma'
user = 'postgres'
password = 'XXX'

# Initialize
postgresman = PostgresManager(logname='test', host=host, port=port, db=db, user=user, password=password)


def test_hearbeat():
    """Test heartbeat functionality with postgres"""

    hb = HeartBeat(postgresmanager=postgresman, table_name='hiflylabs_eta_heartbeat_test')

    # Send heartbeats
    hb_resp_cont = []
    for i in range(3):
        hb.run_heartbeat(interval=2)
        time.sleep(3)
        hb_resp = postgresman.read('hiflylabs_eta_heartbeat_test').values[0, 0]
        hb_resp_cont.append(hb_resp)

    # Check time differences in seconds
    hd_td = [(x1-x0).seconds for (x0, x1) in zip(hb_resp_cont[:-1], hb_resp_cont[1:])]

    assert all([x > 1 for x in hd_td]), 'Heartbeat is sent with higher frequency than set'
