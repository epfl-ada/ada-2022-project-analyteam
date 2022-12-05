"""
    This module contains functions and classes for data processing and transformation
"""
import dask.dataframe as ddf

import numpy as np

import ingestion as ing
from nlp import SentimentAnalyser
from domain_specs import beeradvocate_ratings_ddf

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
    """
    Gets and returns the state (if the location is in USA)

    Args:
        location (str): the location

    Returns:
        str: the state if the location corresponds to __USA_REGEX, 
            None otherwise
    """
    if location is None:
        return None
    
    if re.match(pattern=__USA_REGEX, string=location):
        return location.split(sep=",")[1].strip()

def get_country(location: str):
    """
    Gets and returns the country ("United States" if it corresponds to __USA_REGEX, 
        None if there is no country and location otherwise)

    Args:
        location (str): the location

    Returns:
        str: the country ("United States" if it corresponds to __USA_REGEX, 
            None if there is no country and location otherwise)
    """
    if location is None or re.match(pattern=__NAN_REGEX, string=location):
        return None
    
    if re.match(pattern=__USA_REGEX, string=location):
        return "United States"
    
    return location

def str_nan_to_none(text: str):
    """
    Change the NàN values to None

    Args:
        text (str): the text

    Returns:
        str: None if the text is NàN and otherwise text
    """
    if text is None:
        return None
    
    if re.match(pattern=__NAN_REGEX, string=text):
        return None
    
    return text

def str_nan_to_nan(text: str):
    """
    Change the NàN/None values to np.nan values

    Args:
        text (str): the text

    Returns:
        str: np.nan if the text is NàN/None and otherwise text
    """
    if text is None:
        return np.nan
    
    if re.match(pattern=__NAN_REGEX, string=text):
        return np.nan

    return text

def to_none_ifnot_str(elem: object):
    """
    Change the element of type not string to None

    Args:
        elem (object): the element to look at

    Returns:
        str: the elem if it is of type str, None if it is not
    """
    if elem is None:
        return None
    
    if elem == np.nan:
        return None 
        
    if isinstance(elem, str):
        return elem
    
    return None
    
#####################
# users.csv pipeline
#####################

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

def users_pipeline(persist: bool =False):
    """
    Processes the users provided CSV file to generate a dataframe of users.
    Optionally persists the generated dataframe in parquet format.

    Args:
        persist (bool, optional): Persist the dataframe of processed data. Defaults to False.

    Returns:
        (dask.dataframe.Dataframe): The dataframe of processed data. 
    """
    # load the data
    users_ddf = ing.read_csv(
        path=ing.build_path(folderind="ba", filename="users"),
        assume_missing=True,
        keepcols=__USERS_COLS,
        mode="lazy")        
    # rename columns
    users_ddf = users_ddf.rename(columns=__USERS_COLS_RENAMING)
    # type conversion
    users_ddf = users_ddf.astype(__USERS_DTYPES)
    # convert "nan" to None in "location"
    users_ddf["location"] = users_ddf.location.apply(str_nan_to_none, meta=("location", "object"))
    # convert "joined" floats to pandas.Timestamp
    users_ddf["joined"] = ddf.to_datetime(users_ddf.joined, unit="s")
    # append country column
    users_ddf["country"] = users_ddf["location"].apply(get_country, meta=("country" , "object"))
    
    # persist to disk
    if persist:
        users_ddf.to_parquet(
            ing.build_path(folderind="ba", filename="users", ext=".parquet", basepath=ing.REFINED_PATH))
    
    return users_ddf

#######################
# ratings.csv pipeline
#######################

__RATING_ASPECTS = [
    "appearance", "aroma", "palate", "taste", "overall"]
__RATINGS_COLS=[
    "date",
    "beer_id",
    "user_id",
    "appearance",
    "aroma",
    "palate",
    "taste",
    "overall",
    "rating",
    "review",
    "text"
]
__RATINGS_COLS_RENAMING = {
    "beer_id": "bid",
    "user_id": "uid",
    "review" : "has_review",
    "text" : "review"
}
__RATINGS_DTYPES = {
    "bid": np.int32,
    "uid": "str",
    "has_review": "bool"
}
__COUNTRIES_OF_INTEREST = [
    "United States", "Canada", "England", "Australia"]

def ratings_pipeline(persist: bool =False, **kwargs):
    """
    Processes the raw ratings.csv file into a dataframe.
    It needs the resulting dataframe of users for its operations.
    The resulting dataframe can be persisted in parquet format.

    Args:
        persist (bool): Persist the resulting dataframe. Defaults to False.
        users_persisted (bool, optional): Load the users dataframe from memory. Defaults to False.
        users (dask.dataframe.Dataframe, optional): the dataframe of users. Ignored when users_persisted is True.

    Returns:
        (dask.dataframe.Dataframe): The dataframe of processed data. 
    """
    users_persisted= kwargs.get("users_persisted", False)
    
    # load the data
    users_ddf = None
    if users_persisted:
        users_ddf = ing.read_parquet(
            path=ing.build_path(folderind="ba", filename="users", ext=".parquet", basepath=ing.REFINED_PATH),
            mode="lazy"
        )
    else:
        users_ddf = kwargs["users"]

    ratings_ddf = ing.read_csv(
        path=ing.build_path(folderind="ba", filename="ratings", basepath=ing.REFINED_PATH),
        assume_missing=True,
        keepcols=__RATINGS_COLS,
        mode="lazy")
        
    # rename columns
    ratings_ddf = ratings_ddf.rename(columns=__RATINGS_COLS_RENAMING)
    new_rating_colnames = [__RATINGS_COLS_RENAMING.get(old_colname, old_colname) for old_colname in __RATINGS_COLS]
    # drop beer ratings with missing beer ID since we do not know to which beer the rating corresponds
    ratings_ddf = ratings_ddf[ratings_ddf.bid.notnull()]
    # drop beer ratings with missing user ID since we are interested in user classification
    ratings_ddf = ratings_ddf[ratings_ddf.uid.notnull()]
    # type conversion
    ratings_ddf = ratings_ddf.astype(__RATINGS_DTYPES)
    # keep only ratings with computable beer rating
    # a beer rating is computable <=> all beer aspects' ratings are available
    # if the beer rating is available, do not drop the rating
    computable_rating_mask = True # (True & X) == X
    for rating_aspect in __RATING_ASPECTS:
        computable_rating_mask &= ratings_ddf[rating_aspect].notnull()
    ratings_ddf = ratings_ddf[computable_rating_mask | ratings_ddf.rating.notnull()]
    # keep only ratings from users located in the countries of interest
    users_w_ratings_ddf = ddf.merge(
        ratings_ddf,
        users_ddf, 
        how="inner", left_on="uid", right_on="uid")
    ratings_ddf = users_w_ratings_ddf[users_w_ratings_ddf.country.isin(__COUNTRIES_OF_INTEREST)]
    ratings_ddf = ratings_ddf[new_rating_colnames] # drops the join columns
    # recover beer ratings when missing, if possible
    # a beer rating is recoverable <=> it is computable and the rating is missing
    # do not recompute the already computed ratings, so check that the rating is missing
    recoverable_missing_ratings_mask = computable_rating_mask & ratings_ddf.rating.isnull()
    ratings_ddf[recoverable_missing_ratings_mask] = beeradvocate_ratings_ddf(ratings_ddf[recoverable_missing_ratings_mask])
    # convert "date" float values to pandas.Timestamp
    ratings_ddf["date"] = ddf.to_datetime(ratings_ddf.date, unit="s")
    # convert "nan" values in "review" (new name for "text") to None
    ratings_ddf["review"] = ratings_ddf.review.apply(to_none_ifnot_str, meta=("review", "object"))
    
    # add sentiment column
    ratings_ddf = batch_sentiment_pipeline(ratings_ddf)
    # persist
    if persist:
        ratings_ddf.to_parquet(
            ing.build_path(folderind="ba", filename="ratings", ext=".parquet", basepath=ing.REFINED_PATH))
    
    return ratings_ddf

#################
# beers pipeline
#################

__BEERS_CSV_COLS_OF_INTEREST = [
    "beer_id",
    "beer_name",
    "style",
    "abv",
    "ba_score",
    "bros_score"]
__BEERS_CSV_COLS_RENAMING = {
    "beer_id": "bid",
    "beer_name": "name"
}
__BEERS_CSV_DTYPES = {
    "bid": np.int32,
    "abv": np.float32,
    "ba_score": np.float32,
    "bros_score": np.float32, 
}

def beerscsv_pipeline(persist: bool =False):
    """
    Processes the users provided CSV file to generate a dataframe of users.
    Optionally persists the generated dataframe in parquet format.

    Args:
        persist (bool, optional): Persist the dataframe of processed data. Defaults to False.

    Returns:
        (dask.dataframe.Dataframe): The dataframe of processed data. 
    """
    # load the data
    beerscsv_ddf = ing.read_csv(
        path=ing.build_path(folderind="ba", filename="beers", ext=".csv"),
        keepcols=__BEERS_CSV_COLS_OF_INTEREST,
        mode="lazy")
    # rename the columns
    beerscsv_ddf = beerscsv_ddf.rename(columns=__BEERS_CSV_COLS_RENAMING)
    # drop beers with unknown beer ID
    beerscsv_ddf = beerscsv_ddf[beerscsv_ddf["bid"].notnull()]
    # convert data types
    beerscsv_ddf = beerscsv_ddf.astype(__BEERS_CSV_DTYPES)
    
    if persist:
        beerscsv_ddf.to_parquet(
            ing.build_path(folderind="ba", filename="beers_csv", ext=".parquet", basepath=ing.REFINED_PATH))
    
    return beerscsv_ddf
    
__BEERS_COLS_FROM_RATINGS = [
    "bid",
    "rating",
    "has_review"
]
__BEERS_DTYPES = {
    "bid": np.int32,
    "avg_rating": np.float32,
    "review_rate": np.float32
}
__BEERS_COLS_ORDERED = [
    "bid",
    "n_ratings",
    "avg_rating",
    "n_reviews",
    "review_rate",
    "ba_score",
    "bros_score",
    "name",
    "style",
    "abv"
    ]

def beers_pipeline(persist: bool =False, **kwargs):
    """
    Reproduces a version of beers.csv with only relevant data using the processed ratings.
    It needs the resulting (after processing) dataframe of ratings and that of the beers.csv for its operations.
    The resulting dataframe can be persisted in parquet format.

    Args:
        persist (bool): Persist the resulting dataframe. Defaults to False.
        ratings_persisted (bool, optional): Load the ratings dataframe from memory. Defaults to False.
        ratings (dask.dataframe.Dataframe, optional): the dataframe of ratings. Ignored when users_persisted is True.
        beerscsv_persisted (bool, optional): Load the dataframe resulting from processing beers.csv from memory. 
        Defaults to False, in which case processed beers.csv dataframe is computed.

    Returns:
        (dask.dataframe.Dataframe): The dataframe of processed data. 
    """
    # load the data
    ratings_pesisted = kwargs.get("ratings_persisted", False)
    beerscsv_persisted = kwargs.get("beerscsv_persisted", False)
    ratings_ddf = None
    if ratings_pesisted:
        ratings_ddf = ing.read_parquet(
            path=ing.build_path("ba", "ratings", ext=".parquet", basepath=ing.REFINED_PATH),
            mode="lazy")
    else:
        ratings_ddf = kwargs["ratings"]
        
    # group by beer id the selected features
    beers_base_ddf = ratings_ddf[__BEERS_COLS_FROM_RATINGS].groupby("bid")
    # append average ratings (rating and aspects' ratings) and the review rate
    beers_ddf = beers_base_ddf.agg("mean")
    beers_ddf = beers_ddf.rename(columns={
        "rating": "avg_rating",
        "has_review": "review_rate"})
    # append the number of ratings per beer
    beers_ddf["n_ratings"] = beers_base_ddf.size()
    # append the number of reviews
    beers_ddf["n_reviews"] = beers_base_ddf["has_review"].sum()
    # reseting the index
    beers_ddf = beers_ddf.reset_index()
    # convert dtypes
    beers_ddf = beers_ddf.astype(__BEERS_DTYPES)
    
    # loading and processing beers.csv via its pipeline
    beerscsv_ddf = None
    if beerscsv_persisted:
        beerscsv_ddf = ing.read_parquet(
            ing.build_path(folderind="ba", filename="beers_csv", ext=".parquet", basepath=ing.REFINED_PATH),
            mode="lazy")
    else:
        beerscsv_ddf = beerscsv_pipeline()
        
    # merge columns of interest with the current beers dataframe
    beers_ddf = ddf.merge(beers_ddf, beerscsv_ddf, on="bid", how="inner")
    
    # reorder columns
    beers_ddf = beers_ddf[__BEERS_COLS_ORDERED]
    
    # persist
    if persist:
        beers_ddf.to_parquet(ing.build_path(folderind="ba", filename="beers", ext=".parquet", basepath=ing.REFINED_PATH))
    
    return beers_ddf

#####################
# sentiment pipeline
#####################

__SENTIMENT_COLS = [
    "date", "bid", "uid", "rating", "has_review", "review"
]

def sentiment_pipeline(ratings_ddf):
    """
    This is a milestone 3 pipeline for sentiment analysis of textual reviews.
    """
    analyser = SentimentAnalyser()
    def pos_sentiment_in(text: str):
        label, score = analyser.compute(text)
        return score if label == "POSITIVE" else 1 - score
    
    # select columns of interest
    sentiment_ddf = ratings_ddf[__SENTIMENT_COLS]
    # initialize sentiment
    sentiment_ddf["s+"] = 0
    sentiment_ddf["s-"] = 0
    # compute and set sentiment for ratings with reviews
    with_reviews = sentiment_ddf["has_review"]

    # line to optimize using batch processing (batch inference)
    sentiment_ddf[with_reviews, "s+"] = sentiment_ddf[with_reviews, "review"].apply(
        pos_sentiment_in, meta=("review", "float32"))
    sentiment_ddf[with_reviews, "s-"] = 1 - sentiment_ddf[with_reviews, "s+"] 
    
    return sentiment_ddf

def batch_sentiment_pipeline(ratings_ddf):
    """
    Same as sentiment_pipeline but using batch processing.
    """
    analyser = SentimentAnalyser()
    # select columns of interest
    sentiment_ddf = ratings_ddf[__SENTIMENT_COLS]
    # initialize sentiment
    sentiment_ddf["s+"] = 0
    sentiment_ddf["s-"] = 0    
    def pos_sentiment_in(reviews: list[str]):
        print(type(reviews))
        print(reviews[:10])
        #label, score = analyser.compute(text)
        labels_scores = analyser.batch_compute(reviews)
        pos_sentiments = [score if label == "POSITIVE" else 1 - score for label, score in labels_scores]
        print(pos_sentiments[:10])
        return pos_sentiments

    # compute and set sentiment for ratings with reviews
    # get first row of sentiment_ddf
    print("correct_before")
    with_reviews = sentiment_ddf[sentiment_ddf.has_review == True]
    print("correct_before1")
    reviews = with_reviews["review"]
    print(reviews[0].compute())
    print("correct_after1")
    pos_sentiments = pos_sentiment_in(reviews.values.compute().tolist())

    sentiment_ddf[with_reviews, "s+"] = pos_sentiments
    sentiment_ddf[with_reviews, "s-"] = 1 - sentiment_ddf[with_reviews, "s+"] 
    


        