from phoenix.core.pipe import Pipe

import pandas as pd
import datetime
import sched
import time


class HeartBeat(Pipe):
    """Heartbeat functionality.

    Heartbeat is sent as timestamp continuously updated in a table.
    Table schema:
        TIMESTAMP: timestamptz

    Note, that this class is not suitable for high frequency and precision heartbeat function implementation!

    Args:
        postgresmanager (PostgresManager): Postgres connection object
        table_name: Postgres table where heartbeat is sent

    Example:

        hb = HeartBeat(postgresmanager, 'heartbeat')
        while True:
            hb.run_heartbeat(60) #Send heartbeat every 60 seconds

    """

    def __init__(self, postgresmanager, table_name, *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.postgresmanager = postgresmanager
        self.table_name = table_name
        self.timestamp = None

    def send_heartbeat(self, current_time):
        """Send one heartbeat to the target table"""

        hb_df = pd.DataFrame({'timestamp': current_time}, index=[0])

        #Make heartbeat timezone aware. Be careful, that this does not do datetime conversion from UTC to local time.
        #Use datetime.now() where necessary and set system timezone to localtime (in docker debian: -e TZ=Europe/Budapest)
        hb_df['timestamp'] = hb_df['timestamp'].dt.tz_localize('Europe/Budapest')

        try:
            self.postgresmanager.truncate_insert(self.table_name, hb_df)
            self.logger.info(f'Heartbeat is sent to {self.postgresmanager.host}:{self.postgresmanager.port}: {current_time}')
        except Exception as e:
            self.logger.error(f'Heartbeat forwarding to {self.postgresmanager.host}:{self.postgresmanager.port} failed: {e}')


    def run_heartbeat(self, interval):
        """Send heartbeat if given interval (delay) has passed before the last heartbeat.

        Args:
            interval (int): Interval of heartbeat in seconds

        """

        # Register current time
        current_time = datetime.datetime.now()

        # Send heartbeat if interval has passed, and update timestamp
        # self.timestamp is updated only if heartbeat is sent, otherwise it would not send heartbeat if run_heartbeat is always called within interval
        if self.timestamp:
            if self.timestamp < (current_time-datetime.timedelta(seconds=interval)):
                self.send_heartbeat(current_time)
                self.timestamp = current_time

        else:  # If this is the first heartbeat, send it always
            self.send_heartbeat(current_time)
            self.timestamp = current_time
