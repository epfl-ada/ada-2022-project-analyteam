"""
Summary
-------
    This module implements functions and classes to ingest data.

Author
------
    Farouk Boukil
    Juliette Parchet
"""

# DASK IMPORTS
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
# NLTK IMPORTS
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
# OTHER IMPORTS
import csv
from typing import List, Tuple, Set

###################################################################
# GLOBALS
###################################################################

__ENCODING = "utf-8"

###################################################################
# DASK METHODS
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

###################################################################
# CSV READERS
# need dask: call dask_init first, then dask_shutdown when done
###################################################################

def read_csv_lazy(self, path: str, keepcols: List = None, **kwargs):
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

def read_csv(self, path: str, keepcols: List = None, **kwargs):
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

###################################################################
# TXT READERS
###################################################################

def read_words(path: str):
    assert not(path is None)
    
    with open(path) as file:
        words = set(file.read().splitlines())
    return words

###################################################################
# PARSERS
###################################################################

def __sentiment_word_count(text: str, sentiment_set: Set[str]):
    text = text.lower()
    tokenizer = RegexpTokenizer(r'\w+')
    words = set(tokenizer.tokenize(text))
    return len(set(words).intersection(sentiment_set))

def __rating_vals_from(rating_lines: List[str], selected_tags: List[str], pos_set: Set[str], neg_set: Set[str]):
    # assumes that every line in rating_lines list has the format tag:value
    
    rating = []
    
    has_review = False
    review = None
    
    for _, line in enumerate(rating_lines):
        line_split = line.split(":")
        tag, value = line_split[0], ":".join(line_split[1:])
        
        if tag in selected_tags:
            rating.append(value)
            
        if tag == "text":
            review = value
        elif tag == "review":
            has_review = bool(value)
        
    n_pos = __sentiment_word_count(review, pos_set) if has_review else 0
    rating.append(n_pos)
    
    n_neg = __sentiment_word_count(review, neg_set) if has_review else 0
    rating.append(n_neg)
    
    return rating

def __next_rating(file):
    # <> assumes that different ratings are spaced by at least one "\n"
    # but not necessarily exactly one "\n"
    # <> assumes that no comment has a "\n" in it
    rating_lines = []
    next_line = file.readline()
    while len(next_line) > 1:
        rating_lines.append(next_line.strip())
        next_line = file.readline()
    return rating_lines

def txt2csv(
    from_path      : str,
    to_path        : str,
    selected_tags  : List[str], 
    all_tags       : List[str], 
    sentiment_sets : Tuple[List[str], List[str]]):

    assert not(to_path is None) and len(to_path) > 0
    assert not(from_path is None) and len(from_path) > 0    
    assert not(all_tags is None) and len(all_tags) > 0
    assert not(sentiment_sets is None) and len(sentiment_sets) == 2
    
    # select all tags by default
    selected_tags = selected_tags if not(selected_tags is None) and len(selected_tags) > 0 \
        else all_tags
    
    # sentiment sets
    pos_set, neg_set = sentiment_sets
    
    with open(from_path, 'r', encoding=__ENCODING) as txt_file,\
        open(to_path, 'w', encoding=__ENCODING, newline='') as csv_file:
        
        writer = csv.writer(csv_file)
        writer.writerow(selected_tags)
        
        rating_lines = __next_rating(txt_file)
        while len(rating_lines) > 0:
            rating = __rating_vals_from(rating_lines, selected_tags, pos_set, neg_set)         
            writer.writerow(rating)
            rating_lines = __next_rating(txt_file)
