import pandas as pd

from nkmfraud.core.pipe import Pipe


class HDFReader(Pipe):
    """HDF5 file system reader Pipe"""

    @Pipe.timeit
    def run(self, hdf_path, table_name):
        """
        Args:
            hdf_path (str): Path of the .h5 file
            table_name: Name of the table in the .h5 file

        Returns:
            pandas.DataFrame

        Examples:
            df = HDFReader('hdf_reader','loaders').run('hdf_test.h5', 'test_table')

        """

        df = pd.read_hdf(hdf_path, table_name)
        self.logger.info('DataFrame is loaded from {} with shape: {}'.format(hdf_path, df.shape))

        return df
