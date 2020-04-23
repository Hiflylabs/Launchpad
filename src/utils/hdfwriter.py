import pandas as pd

from nkmfraud.core.pipe import Pipe


class HDFWriter(Pipe):
    """HDF5 file system reader Pipe"""

    @Pipe.timeit
    def run(self, hdf_path, table_name, df):
        """
        Args:
            hdf_path (str): Path of the .h5 file
            table_name: Name of the table in the .h5 file
            df (pandas.DataFrame): DataFrame to be written to HDF5

        Examples:
            HDFWriter('hdf_writer','loaders').run('hdf_test.h5', 'test_table', df)

        """

        df.to_hdf(hdf_path, table_name, append=False, quoting = 3)
        self.logger.info('DataFrame is written to {}, with shape: {}'.format(hdf_path, df.shape))
