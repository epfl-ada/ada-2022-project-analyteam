"""
    This module contains functions and classes for data processing and transformation
"""
import dask.distributed as dd
import dask.dataframe as ddf
import pandas as pd
import numpy as np

import ingestion as ing

import re

##########################################################################
# REGULAR EXPRESSIONS
##########################################################################

__USA_REGEX = r".*(USA).*|.*(United States)(of America)?.*"
__NAN_REGEX = r"[ ]*[nN][a][nN][ ]*"

##########################################################################
# ELEMENT-WISE DATA TRANSFORMATION FUNCTIONS
##########################################################################

def get_state(location: str):
    if location is None:
        return None
    
    if re.match(pattern=__USA_REGEX, string=location):
        return location.split(sep=",")[1].strip()

def get_country(location: str):
    """_summary_

    Args:
        location (str): _description_

    Returns:
        _type_: _description_
    """
    if location is None or re.match(pattern=__NAN_REGEX, string=location):
        return None
    
    if re.match(pattern=__USA_REGEX, string=location):
        return "United States"
    
    return location

def str_nan_to_none(text: str):
    """_summary_

    Args:
        text (str): _description_

    Returns:
        _type_: _description_
    """
    if text is None:
        return None
    
    if re.match(pattern=__NAN_REGEX, string=text):
        return None
    
    return text

def str_nan_to_nan(text: str):
    """_summary_

    Args:
        text (str): _description_
    """
    if text is None:
        return np.nan
    
    if re.match(pattern=__NAN_REGEX, string=text):
        return np.nan
    
##########################################################################
# DATA HANDLING PIPELINE
##########################################################################

############
# users.csv
############
__USERS_COLS = [
    "nbr_ratings",
    "nbr_reviews",
    "user_id",
    "user_name",
    "joined",
    "location"]
__USERS_COLS_RENAMING = {
    "nbr_ratings": "n_ratings",
    "nbr_reviews": "n_reviews",
    "user_id": "uid",
    "user_name": "username"
}
__USERS_DTYPES = {
    "n_ratings": np.int32, 
    "n_reviews": np.int32, 
    "uid": str, 
    "username": str,
    "location": str
}

##############
# ratings.csv
##############
__RATINGS_COLS=[
    "date",
    "beer_id",
    "user_id",
    "appearance",
    "aroma",
    "palate",
    "taste",
    "overall",
    "rating"
]
__RATINGS_COLS_RENAMING = {
    "beer_id": "bid",
    "user_id": "uid"
}
__RATINGS_DTYPES = {
    "bid": str,
    "uid": str
}
__COUNTRIES_OF_INTEREST = [
    "United States", "Canada", "England", "Australia"]

def pipeline(mode: str ="lazy"):
    """_summary_
    """
    ###############
    # users.csv
    # provided
    ###############
    
    # load the data
    users_ddf = ing.read_csv(
        path=ing.build_path(folderind="ba", fileind="users"),
        assume_missing=True,
        keepcols=__USERS_COLS,
        mode="lazy")
    # rename columns
    users_ddf = users_ddf.rename(columns=__USERS_COLS_RENAMING)
    # type conversion
    users_ddf = users_ddf.astype(__USERS_DTYPES)
    # convert "nan" to None in "location"
    users_ddf["location"] = users_ddf.location.apply(str_nan_to_none)
    # convert "joined" floats to pandas.Timestamp
    users_ddf["joined"] = ddf.to_datetime(users_ddf.joined, unit="s")
    # append country column
    users_ddf["country"] = users_ddf["location"].apply(get_country)
    
    ##############################################
    # ratings.csv
    # obtained by parsing the provided ratings.txt
    ##############################################
    
    # load the data
    ratings_ddf = ing.read_csv(
        path=ing.build_path(folderind="ba", fileind="ratings", basepath="./RefinedData"),
        assume_missing=True,
        keepcols=__RATINGS_COLS,
        mode="lazy")
    # rename columns
    ratings_ddf = ratings_ddf.rename(columns=__RATINGS_COLS_RENAMING)
    # type conversion
    ratings_ddf = ratings_ddf.astype(__RATINGS_DTYPES)
    # keep only ratings from users located in the countries of interest
    users_w_ratings_ddf = ddf.merge(
        ratings_ddf, 
        users_ddf, 
        how="inner", left_on="uid", right_on="uid", suffixes=("","_u"))
    ratings_ddf = users_w_ratings_ddf[users_w_ratings_ddf.country_u.isin(__COUNTRIES_OF_INTEREST)]
    ratings_ddf = ratings_ddf[__RATINGS_COLS] # drops the join columns
    # convert "date" float values to pandas.Timestamp
    ratings_ddf["date"] = ddf.to_datetime(ratings_ddf.date, unit="s")
    # 
    
    if mode == "lazy":
        return (ratings_ddf, users_ddf)
    
    if mode == "eager":
        return (ratings_ddf.compute(), users_ddf.compute())
    
    raise ValueError("Mode (%s) is not supported. Supported modes are \"eager\" or \"lazy\"."%(mode))
        