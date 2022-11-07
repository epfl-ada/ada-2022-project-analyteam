"""
    Summary
    -------
    
"""

from dask.distributed import Client, LocalCluster

import nltk

###################################################################
# NLTK SETUP
###################################################################

def nltk_init():
    nltk.download("stopwords")
    nltk.download("wordnet")
    nltk.download('omw-1.4')
    
###################################################################
# DASK SETUP
###################################################################

def dask_init():
    """
    Initializes Dask.

    Returns:
        (dask.distributed.Client, dask.distributed.LocalCluster):
            A Dask client and local cluster.
    """
    cluster = LocalCluster()
    client = Client(cluster)
    return client, cluster

def dask_shutdown(client):
    """
    Kills a dask client.

    Args:
        client (dask.dataframe.Client): client to shutdown.
    """
    client.shutdown()