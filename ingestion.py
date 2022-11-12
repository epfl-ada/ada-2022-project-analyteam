"""
Summary
-------
    This module implements functions and classes to ingest data.

Author
------
    Farouk Boukil
    Juliette Parchet
"""

import dask.dataframe as dd

from nlp import SentimentAnalyser, Tokenizer

import datetime as dt

import csv
from typing import List

###################################################################
# PATHS
###################################################################

BASE_PATH = "./Data"
REFINED_PATH = "./RefinedData"

FOLDERS = {
    "ba": "BeerAdvocate",
    "rb": "RateBeer",
    "mbd": "MatchedBeerData"
}

def build_path(folderind: str, filename: str, ext: str =".csv", basepath=BASE_PATH):
    return "/".join([basepath, FOLDERS[folderind], filename + ext])

###################################################################
# STATIC VARIABLES
###################################################################

__ENCODING = "utf-8"

###################################################################
# CSV READERS
# need dask: call dask_init first, then dask_shutdown when done
###################################################################

def read_csv(
    path: str,
    keepcols: List[str] =None, 
    assume_missing: bool =False,
    mode: str ="lazy"):
    """
    Reads columns from a CSV file into a Dask Dataframe.

    Args:
        path (str): 
            The path to the CSV file.
        keepcols (List[int] or List[str]): 
            Columns to be kept. Refer to the "usecols" attribute of pandas.Dataframe. 
            Defaults to None, in which case no column is discarded.
            The behavior on an empty list is the same as that on None.

    Returns:
        (dask.dataframe.Dataframe): The resulting dataframe.
    """
    # if no column is specified, keep all
    keep_all = keepcols is None or len(keepcols) == 0

    lazy_ddf = dd.read_csv(urlpath=path, assume_missing=assume_missing) if keep_all \
        else dd.read_csv(urlpath=path, usecols=keepcols, assume_missing=assume_missing)

    if mode == "lazy":
        return lazy_ddf
    
    if mode == "eager":
        return lazy_ddf.compute()
    
    raise ValueError("Mode (%s) is not supported. Supported modes are \"eager\" or \"lazy\"."%(mode))

###################################################################
# PARQUET READERS
# need dask: call dask_init first, then dask_shutdown when done
###################################################################

def read_parquet(
    path: str,
    keepcols: List[str] =None, 
    assume_missing: bool =False,
    mode: str ="lazy"):
    """
    Reads columns from a parquet file into a Dask Dataframe.

    Args:
        path (str): 
            The path to the parquet file.
        keepcols (List[int] or List[str]): 
            Columns to be kept. Refer to the "usecols" attribute of pandas.Dataframe. 
            Defaults to None, in which case no column is discarded.
            The behavior on an empty list is the same as that on None.

    Returns:
        (dask.dataframe.Dataframe): The resulting dataframe.
    """
    # if no column is specified, keep all
    keep_all = keepcols is None or len(keepcols) == 0

    print(path)
    lazy_ddf = dd.read_parquet(path=path, assume_missing=assume_missing) if keep_all \
        else dd.read_parquet(path=path, columns=keepcols, assume_missing=assume_missing)

    if mode == "lazy":
        return lazy_ddf
    
    if mode == "eager":
        return lazy_ddf.compute()
    
    raise ValueError("Mode (%s) is not supported. Supported modes are \"eager\" or \"lazy\"."%(mode))

###################################################################
# PARSING
###################################################################

def _rating_vals_from(
    rating_lines      : List[str], 
    selected_tags     : List[str]):
    """_summary_

    Args:
        rating_lines (List[str]): _description_
        selected_tags (List[str]): _description_
        tokenizer (Tokenizer): _description_
        sentiment_analyser (SentimentAnalyser): _description_

    Returns:
        _type_: _description_
    """
    # assumes that every line in rating_lines list has the format tag:value
    rating = {}
        
    # getting the rating's attributes of interest
    sep = ":"
    for line in rating_lines:
        # split tag and value
        line_split = line.split(sep)
        tag, value = line_split[0], sep.join(line_split[1:])
        
        # write all selected tag values except for the review value, because it requires processing
        if tag in selected_tags:
            rating[tag] = value.strip() 
            
    return rating

def _next_rating(file):
    """_summary_

    Args:
        file (_type_): _description_

    Returns:
        _type_: _description_
    """
    # <> assumes that different ratings are spaced by at least one "\n"
    # but not necessarily exactly one "\n"
    # <> assumes that no comment has a "\n" in it
    
    
    rating_started = False # indicates whether the while loop below has started reading a rating
    rating_lines = [] # the lines of one rating
    
    next_line = file.readline()
    while len(next_line) >= 1:
        # if line has one caracter (could be "\n" or else)
        if len(next_line) == 1:
            # if rating has not started, keep reading
            if not(rating_started):
                next_line = file.readline() 
                continue
            # if rating has started, stop reading
            if rating_started:
                return rating_lines
            
        # strip and append the line
        rating_started = True
        rating_lines.append(next_line.strip())
        next_line = file.readline()
        
    return rating_lines

def txt2csv(
    from_path      : str,
    to_path        : str,
    selected_tags  : List[str], 
    all_tags       : List[str]):
    """_summary_

    Args:
        from_path (str): _description_
        to_path (str): _description_
        selected_tags (List[str]): _description_
        all_tags (List[str]): _description_
        sentiment_sets (Tuple[List[str], List[str]]): _description_
    """

    assert not(to_path is None) and len(to_path) > 0
    assert not(from_path is None) and len(from_path) > 0    
    assert not(all_tags is None) and len(all_tags) > 0
    
    # select all tags by default
    selected_tags = selected_tags if not(selected_tags is None) and len(selected_tags) > 0 \
        else all_tags
    
    with open(from_path, 'r', encoding=__ENCODING) as txt_file,\
        open(to_path, 'w', encoding=__ENCODING) as csv_file:
        
        tags = selected_tags
        
        writer = csv.writer(csv_file)
        writer.writerow(tags)
        
        rating_lines = _next_rating(txt_file)
        while len(rating_lines) > 0:
            rating_dict = _rating_vals_from(rating_lines, selected_tags)
            ordered_rating = [rating_dict[tag] for tag in tags]
            writer.writerow(ordered_rating)
            rating_lines = _next_rating(txt_file)
