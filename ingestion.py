"""
Summary
-------
    This module implements functions and classes to ingest data.

Author
------
    Farouk Boukil
"""
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster

from typing import List

class CSVReader:
    """
        Summary
        -------
        Reads CSV files in a distributed lazy manner.
    """
    
    cluster = LocalCluster()
    
    def __init__(self):
        self.__client = Client(CSVReader.cluster)
        
    def lazy_extract(self, path: str, keepcols: List =None, **kwargs):
        """
        Extracts columns from a CSV file into a Dask Dataframe.

        Args:
            path (str): 
                The path to the CSV file.
            keepcols (List[int] or List[str]): 
                Columns to be kept. Refer to the "usecols" attribute of pandas.Dataframe. 
                Defaults to None, in which case no column is discarded.
                The behavior on an empty list is the same as that on None.

        Returns:
            (Dask.Dataframe): The resulting dataframe.
        """
        # if no column is specified, keep all
        keep_all = keepcols is None or len(keepcols) == 0
        
        lazy_df = dd.read_csv(urlpath=path) if keep_all \
            else dd.read_csv(urlpath=path, usecols=keepcols)
            
        return lazy_df

    def extract(self, path: str, keepcols: List =None, **kwargs):
        """
        Extracts columns from a CSV file into a Dask Dataframe.

        Args:
            path (str): 
                The path to the CSV file.
            keepcols (List[int] or List[str]): 
                Columns to be kept. Refer to the "usecols" attribute of pandas.Dataframe. 
                Defaults to None, in which case no column is discarded.

        Returns:
            (Dask.Dataframe): The resulting dataframe.
        """
        return self.lazy_extract(path, keepcols, **kwargs).compute()        