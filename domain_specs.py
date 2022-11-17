"""
    This module contains domain specific functions
"""

# weights taken from:
# https://www.beeradvocate.com/community/threads/how-to-review-a-beer.241156/
__CRITERIUM_TO_WEIGHT = {
    "appearance": 0.06,
    "aroma": 0.24,
    "palate": 0.1,
    "taste": 0.4,
    "overall": 0.2
}

def beeradvocate_ratings_ddf(ddf):
    """
        Add a column to ddf containing the overall rating of the beer, according to certain
        criteriums and weights 
    Args:
        ddf (dask.dataframe.Dataframe): framework containing informations about the beers
    """
    ratings = None
    for criterium, weight in __CRITERIUM_TO_WEIGHT.items():
        if ratings is None:
            ratings = weight * ddf[criterium]
        else:
            ratings += weight * ddf[criterium]