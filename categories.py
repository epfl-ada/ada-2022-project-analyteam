
import dask.dataframe as ddf
import pandas as pd
import numpy as np
from copy import copy

import ingestion as ing
import sys

import time

REFINED_PATH = "RefinedData"

T = 3.25

BID_STR = "bid"
UID_STR = "uid"
RATING_STR = "rating"
AVG_RATING_STR = "avg_rating"
STD_RATING_STR = "std_rating"
N_RATING_STR = "n_ratings"
BA_SCORE_STR = "ba_score"
BA_SCORE_BALANCED_STR = 'ba_score_balanced'
DATE_STR = "date"

RANK_STR = "rank"
DATED_RATING_STR = "dated_rating"
#ASSOCIATED_BEERS_STR = "associated_beers"

CFM_SCORE_STR = "cfm_score"
XPL_SCORE_STR = "xpl_score"
EXP_SCORE_STR = "exp_score"
ADV_SCORE_STR = "adv_score"

IS_CFM_STR = "is_cfm"
IS_XPL_STR = "is_xpl"
IS_EXP_STR = "is_exp"
IS_ADV_STR = "is_adv"

class Categorization():

    def __init__(self, ratings_parquets_path, beers_parquet_path, users_parquet_path):
        __COUNTRIES_OF_INTEREST = [
    "United States", "Canada", "England", "Australia"]
        self.ratings_ddf = ing.read_parquet(ratings_parquets_path)[[DATE_STR, UID_STR, BID_STR, RATING_STR]].compute()
        self.ratings_ddf.set_index(BID_STR)
        print("ratings loaded")

        self.beers_ddf = pd.read_parquet(beers_parquet_path)[[BID_STR, N_RATING_STR, AVG_RATING_STR, BA_SCORE_STR]]
        self.beers_ddf = self.beers_ddf.sort_values(by=BID_STR)
        print("beers loaded")
        
        self.users_ddf = pd.read_parquet(users_parquet_path)
        print("users loaded")

        self.users_ddf = self.users_ddf[self.users_ddf.country.isin(__COUNTRIES_OF_INTEREST)]
        self.users_ddf = self.users_ddf[self.users_ddf.n_ratings>=5]
       # self.beers_ddf[BA_SCORE_BALANCED_STR] = self.beers_ddf[BA_SCORE_STR].apply(lambda x: self.mapping(x))
        t = time.time()
        self._ratings_dated()
        print("ratings dated (", time.time()-t,")")

        t = time.time()
        self.beers_per_users = self._beers_per_user()
        print("beers per user (", time.time()-t,")")
        
        t = time.time()
        self._std()
        self.ratings_beers_merge = ddf.merge(self.ratings_ddf, self.beers_ddf, how="inner", left_on=BID_STR, right_on=BID_STR)
        self.ratings_beers_merge = self.ratings_beers_merge[self.ratings_beers_merge[STD_RATING_STR] != 0]
        print("std (", time.time()-t,")")
     
        self.ratings_beers_merge[BA_SCORE_BALANCED_STR] = (self.ratings_beers_merge[BA_SCORE_STR] - self.ratings_beers_merge[BA_SCORE_STR].mean())/20+ self.ratings_beers_merge[RATING_STR].mean()

    def _std(self):
        """
        Adds one column in the dataframe beers_ddf which calculates the std rating of the beer
        """   
        beerscsv_ddf_w_ratings_ddf = ddf.merge(self.ratings_ddf, self.beers_ddf, how="inner", left_on=BID_STR, right_on=BID_STR)

        std_score = beerscsv_ddf_w_ratings_ddf.groupby(BID_STR).apply(
            lambda beer,rating,avg_rating: sum((beer[rating] - beer[avg_rating])**2), RATING_STR, AVG_RATING_STR).rename(STD_RATING_STR).reset_index()
        
        self.beers_ddf = ddf.merge(self.beers_ddf, std_score, how="inner", left_on=BID_STR, right_on=BID_STR)
        self.beers_ddf[STD_RATING_STR]=self.beers_ddf.apply(
            lambda beers : np.sqrt(beers[STD_RATING_STR]/beers[N_RATING_STR]),axis=1)
    def mapping(self,x):
        if(x<=60):
            return x *(2 / 60)
        if(x<=70):
            return 2 + (x-60)*0.1
        if(x<=80):
            return 3 + (x-70)*0.05
        if(x<=85):
            return 3.5 + (x-80)*0.1
        if(x<=90):
            return 4 + (x-85)*0.1
        if(x<=95):
            return 4.5 + (x-90)*0.05

        return 4.75 + (x-95)*0.05


    def _ratings_dated(self):    
        """
        Adds two columns in the dataframe ratings_ddf: one which calculates the average score of the beer at the time of the rating,
        and the other one which calculates the time order of the rating (1 if it was the first rating on the beer for example)
        """    
        self.ratings_ddf = self.ratings_ddf.sort_values([BID_STR, DATE_STR])

        previous_bid = -1
        summation = 0
        number = 0
        l_avg_ratings = []
        l_rank = []

        for _, row in self.ratings_ddf.iterrows():
            actual_bid = row['bid']
            if previous_bid != actual_bid:
                number = 0
                summation = 0

            number += 1
            summation += row["rating"]

            l_avg_ratings.append(summation / number)
            l_rank.append(number)
            
            previous_bid = actual_bid

        self.ratings_ddf["dated_rating"] = l_avg_ratings
        self.ratings_ddf["rank"] = l_rank



    def _beers_per_user(self):        
        """
        Creates and returns a dictionary with users as keys, and a list of beers rated with the user as value
        """
        self.ratings_ddf = self.ratings_ddf.sort_values(["uid"])

        previous_uid = -1
        beers_per_users = dict()
        l = None
        for _, row in self.ratings_ddf.iterrows():
            actual_uid = row[UID_STR]
            if previous_uid != actual_uid:
                if l is not None:
                    beers_per_users[previous_uid] = l
                l = []
            l.append(row[BID_STR])
            previous_uid = actual_uid
        return beers_per_users

              
    ### HELPERS

    def get_userIds_list(self):
        return list(self.users_ddf["uid"])

    def compute_all_scores(self):
        """
        Computes all scores for all users, adds all the scores to the dataframe users_ddf, from these scores categorizes the users into the 
        conformist, expert-like, explorer, and adventurer categories (by adding columns IS_CFM_STR, IS_EXP_STR, IS_XPL_STR, IS_ADV_STR to the dataframe users_ddf)
        Writes the dataframe users_ddf as a .parquet file
        Returns the dataframe users_ddf
        """
        self.get_all_scores()
        self.categorize_all_users()
        self.users_ddf.replace([np.inf, -np.inf], sys.maxsize, inplace=True)
        return self.users_ddf

    def categorize_all_users(self):
        cfm_threshold = self.users_ddf[CFM_SCORE_STR].quantile(0.9)
        exp_threshold = -0.2 # +/-8% error tolerated
        xpl_threshold = self.users_ddf[XPL_SCORE_STR].quantile(0.88)
        adv_threshold = self.users_ddf[ADV_SCORE_STR].quantile(0.9)
        
        self.users_ddf[IS_CFM_STR] = np.where(self.users_ddf[CFM_SCORE_STR] >= cfm_threshold, 1, 0)
        self.users_ddf[IS_EXP_STR] = np.where(self.users_ddf[EXP_SCORE_STR] >= exp_threshold, 1, 0)
        self.users_ddf[IS_XPL_STR] = np.where(self.users_ddf[XPL_SCORE_STR] >= xpl_threshold, 1, 0)
        self.users_ddf[IS_ADV_STR] = np.where(self.users_ddf[ADV_SCORE_STR] >= adv_threshold, 1, 0)
        

    ### SCORE GETTERS

    def get_all_scores(self):
        """
        Computes and returns (as a tuple of lists of int) all the scores for all users 
        Stores these values in new columns CFM_SCORE_STR, EXP_SCORE_STR, XPL_SCORE_STR, ADV_SCORE_STR in the dataframe users_ddf
        """
        cfm_scores = self.get_cfm_scores()
        exp_scores = self.get_exp_scores()
        xpl_scores = self.get_xpl_scores()
        adv_scores = self.get_adv_scores()

        return cfm_scores, exp_scores, xpl_scores, adv_scores


    def get_cfm_scores(self): 
        """
        Computes and returns (as a list of int) the conformist score for all users 
        Stores these values in a new column CFM_SCORE_STR in the dataframe users_ddf
        """
        conformist_table = copy(self.ratings_beers_merge)

        cmf_per_rating_str = 'cfm_per_rating'
        conformist_table[cmf_per_rating_str] = ((conformist_table[RATING_STR] - conformist_table[AVG_RATING_STR]) / conformist_table[STD_RATING_STR])**2
        conformist_table = conformist_table.groupby(UID_STR).mean()[cmf_per_rating_str].reset_index()
        
        self.users_ddf = ddf.merge(self.users_ddf, conformist_table, how="left", on='uid')
        self.users_ddf = self.users_ddf.rename(columns={cmf_per_rating_str: CFM_SCORE_STR})
        self.users_ddf[CFM_SCORE_STR] = - self.users_ddf[CFM_SCORE_STR]

        return self.users_ddf[CFM_SCORE_STR]

    def get_exp_scores(self):
        """
        Computes and returns (as a list of int) the expert-like score for all users 
        Stores these values in a new column EXP_SCORE_STR in the dataframe users_ddf
        """
        exp_per_rating_str = 'exp_per_rating'
        self.ratings_beers_merge[exp_per_rating_str] = (self.ratings_beers_merge[RATING_STR] - self.ratings_beers_merge[BA_SCORE_BALANCED_STR]).abs()

        self.users_ddf = ddf.merge(self.users_ddf, self.ratings_beers_merge.groupby(UID_STR).mean()[exp_per_rating_str].reset_index(), how="left", on=UID_STR)
        self.users_ddf = self.users_ddf.rename(columns={exp_per_rating_str: EXP_SCORE_STR})
        self.users_ddf[EXP_SCORE_STR] = - self.users_ddf[EXP_SCORE_STR]
        return self.users_ddf[EXP_SCORE_STR]


    def get_adv_scores(self):
        """
        Computes and returns (as a list of int) the adventurer score for all users 
        Stores these values in a new column ADV_SCORE_STR in the dataframe users_ddf
        """
        adventurer_score_fast_table = self.ratings_ddf.loc[self.ratings_ddf[DATED_RATING_STR] <= T].groupby(UID_STR).count()[[RANK_STR]]
        adventurer_score_fast_table = adventurer_score_fast_table.reset_index()

        self.users_ddf = ddf.merge(self.users_ddf, adventurer_score_fast_table, how="left", on=UID_STR)

        self.users_ddf = self.users_ddf.rename(columns={RANK_STR: ADV_SCORE_STR}).fillna(0)
        self.users_ddf[ADV_SCORE_STR] = self.users_ddf[ADV_SCORE_STR].astype('Int64') / self.users_ddf[N_RATING_STR]
        return self.users_ddf[ADV_SCORE_STR]

       
    def get_xpl_scores(self):
        """
        Computes and returns (as a list of int) the explorer score for all users 
        Stores these values in a new column XPL_SCORE_STR in the dataframe users_ddf
        """
        grouped_ratings_ddf = self.ratings_ddf[[UID_STR,RANK_STR]] # keep only selection of columns
        grouped_ratings_ddf = grouped_ratings_ddf.loc[grouped_ratings_ddf[RANK_STR] <= 10].groupby(UID_STR).count()
        # keep only ratings which rank is <= 10
        grouped_ratings_table = grouped_ratings_ddf.reset_index()

        self.users_ddf = ddf.merge(self.users_ddf, grouped_ratings_table, how="left", on=UID_STR)

        self.users_ddf = self.users_ddf.rename(columns={RANK_STR: XPL_SCORE_STR}).fillna(0)
        self.users_ddf[XPL_SCORE_STR] = self.users_ddf[XPL_SCORE_STR].astype('Int64') / self.users_ddf[N_RATING_STR]

        return self.users_ddf[XPL_SCORE_STR]


### UNUSED / TO DELETE 

    def get_adventurer_score_for_user(self, user_id):
        rating_of_user_dated = self.ratings_ddf.loc[self.ratings_ddf[dated_rating_str] < T]
        rating_of_user_dated = rating_of_user_dated.loc[rating_of_user_dated["uid"] == user_id][[dated_rating_str]]
        return len(rating_of_user_dated.index)


    def get_explorer_score_for_user(self, user_id):
        first_ratings = self.ratings_ddf.loc[self.ratings_ddf["rank"] <= 10][["uid"]]
        first_ratings = first_ratings.loc[first_ratings["uid"] == user_id]
        return len(first_ratings.index)


    def get_all_scores_for_user(self, user_id):
        
        conformist_score = self.get_conformist_score(user_id)
        adventurer_score = self.get_adventurer_score(user_id)
        expertlike_score = self.get_expertlike_score(user_id)
        explorer_score = self.get_explorer_score(user_id)

        return conformist_score, adventurer_score, expertlike_score, explorer_score


    def _beers_associated_with_user(self):
        # self.associated_beers=self.ratings_ddf.groupby('uid')['bid'].apply(list).reset_index('associated_beers')
        self.associated_beers=self.ratings_ddf.groupby(UID_STR).apply(list)[BID_STR].reset_index(ASSOCIATED_BEERS_STR)
            
    
    def get_ratings_head(self, h = 30):
        return self.ratings_ddf.sort_values(by="bid").head(h)

    def get_beers_head(self, h=30):
        return self.beers_ddf.sort_values(by="bid").head(h)
    
    def get_users_head(self, h=30):
        return self.users_ddf.head(h)
        