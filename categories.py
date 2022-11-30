
import dask.dataframe as ddf
import pandas as pd
import numpy as np

import ingestion as ing

import time

T = 2 # TO CHANGE !!

class Categorization():

    
    def _std(self):
        beerscsv_ddf_w_ratings_ddf = ddf.merge(
            self.ratings_ddf,
            self.beers_ddf, 
            how="inner", left_on="bid", right_on="bid")

        std_score=beerscsv_ddf_w_ratings_ddf.groupby('bid').apply(lambda beer,rating,avg_rating: sum((beer[rating] - beer[avg_rating])**2), 'rating', 'avg_rating').rename('std_rating').reset_index()
        
        self.beers_ddf = ddf.merge(
                self.beers_ddf,
                std_score,
                how="inner", left_on="bid", right_on="bid")
        self.beers_ddf['std_rating']=self.beers_ddf.apply(lambda beers : 0 if (beers['n_reviews']==0 or beers['n_reviews']==1) else np.sqrt(beers['std_rating']/beers['n_reviews']),axis=1,meta=('x', 'f8'))
    
    def _avg_at_date(self):
        a = self.ratings_ddf[["date", "uid", "bid", "rating"]]



    def __init__(self, ratings_parquets_path, beers_parquet_path, users_parquet_path):
        self.ratings_ddf = ing.read_parquet(ratings_parquets_path)
        self.ratings_ddf.set_index("bid")
        print("RATINGS DDF")
        #print(self.ratings_ddf.head(3))
        print("---------------------------------------------")
        self.beers_ddf = pd.read_parquet(beers_parquet_path)
        print("BEERS DDF")
        #print(self.beers_ddf.head(3))
        print("---------------------------------------------")
        self.users_ddf = pd.read_parquet(users_parquet_path)
        print("USERS DDF")
        #print(self.users_ddf.head(3))
        print("---------------------------------------------")

        self._std()
        print(self.beers_ddf.head(2))
        

    def get_userIds_list(self):
        return list(self.users_ddf["uid"])


    def get_all_scores(self, user_id):
        beers_associated_list = self._beers_associated_with_user(user_id)

        conformist_score = self.get_conformist_score(user_id, beers_associated_list=beers_associated_list)
        adventurer_score = self.get_adventurer_score(user_id, beers_associated_list=beers_associated_list)
        expertlike_score = self.get_expertlike_score(user_id, beers_associated_list=beers_associated_list)
        explorer_score = self.get_explorer_score(user_id, beers_associated_list=beers_associated_list)

        return conformist_score, adventurer_score, expertlike_score, explorer_score


    def get_conformist_score(self, user_id, beers_associated_list=None):
        if beers_associated_list is None:
            beers_associated_list = self._beers_associated_with_user(user_id)

        summation = 0
        for beer_id in beers_associated_list: # iterate over all the beers rated by the user
            std = self.beers_ddf.loc[self.beers_ddf["bid"] == beer_id]["std"]
            std = list(std)[0] if len(std.items) > 0 else 0
            if std != 0: # no point measuring conformity score over beers that have a single rating 
                r_b = self.beers_ddf.loc[self.beers_ddf["bid"]]["avg_rating"]
                r_b = list(r_b)[0] if len(r_b.items) > 0 else 0
                
                r_u_b = self.ratings_ddf.loc[self.beers_ddf["bid"] == beer_id].loc[self.beers_ddf["uid"] == user_id]
                r_u_b = list(r_u_b)[0] if len(r_u_b) > 0 else r_b
                summation += ((r_u_b - r_b)/std)**2

        return summation / len(beers_associated_list)


    def get_adventurer_score(self, user_id, beers_associated_list=None):
        t = time.time()

        print("ADVENTURER SCORE FOR ", user_id)

        if beers_associated_list is None:
            beers_associated_list = self._beers_associated_with_user(user_id)

        print("   time prep user : ", time.time()-t)

        summation = 0

        t=time.time()
        ratings_of_beers_dated = self.ratings_ddf.loc[self.ratings_ddf["bid"].isin(beers_associated_list)][["date", "uid", "bid", "rating"]].compute()
        print("   time ratings for all beers for user : ", time.time()-t)

        t = time.time()
        for beer_id in beers_associated_list:
            
            ratings_of_beer_dated = ratings_of_beers_dated.loc[ratings_of_beers_dated["bid"] == beer_id][["date", "uid", "rating"]]
            
            review_time_of_user = ratings_of_beer_dated.loc[ratings_of_beer_dated["uid"] == user_id]["date"]
            review_time_of_user_list = list(review_time_of_user)

            if len(review_time_of_user_list) > 0 : 
                review_time_of_user = review_time_of_user_list[0] 
                ratings_of_beer_dated = ratings_of_beer_dated.loc[ratings_of_beer_dated["date"] < review_time_of_user]["rating"]
                avg_ratings_of_beer_dated = sum(list(map(int,ratings_of_beer_dated))) 
                #print("   Review time of user :",review_time_of_user)
                #print("   Number of beers rated before user : ", ratings_of_beer_dated.size.compute())
                #print("   avg ratings of beer dated : ", avg_ratings_of_beer_dated)
                if ratings_of_beer_dated.size > 0 and avg_ratings_of_beer_dated < T: 
                    summation += 1

        print("   total time for user : ", time.time()-t)
        return summation


    """def get_adventurer_score(self, user_id, beers_associated_list=None):
        t = time.time()

        print("ADVENTURER SCORE FOR ", user_id)

        if beers_associated_list is None:
            beers_associated_list = self._beers_associated_with_user(user_id)

        print("   time prep user : ", time.time()-t)

        summation = 0
        for beer_id in beers_associated_list:
            
            t = time.time()
            ratings_of_beer_dated = self.ratings_ddf.loc[self.ratings_ddf["bid"] == beer_id][["date", "uid", "rating"]].compute()
            print("   time elapse 1: ", time.time()-t)
            
            review_time_of_user = ratings_of_beer_dated.loc[ratings_of_beer_dated["uid"] == user_id]["date"]
            review_time_of_user_list = list(review_time_of_user)

            if len(review_time_of_user_list) > 0 : # takes a long time 15 sec
                review_time_of_user = review_time_of_user_list[0] # takes very long time 30 sec
                ratings_of_beer_dated = ratings_of_beer_dated.loc[ratings_of_beer_dated["date"] < review_time_of_user]["rating"]
                avg_ratings_of_beer_dated = sum(list(map(int,ratings_of_beer_dated))) # takes long time 15 sec
                #print("   Review time of user :",review_time_of_user)
                #print("   Number of beers rated before user : ", ratings_of_beer_dated.size.compute())
                #print("   avg ratings of beer dated : ", avg_ratings_of_beer_dated)
                if ratings_of_beer_dated.size > 0 and avg_ratings_of_beer_dated < T: # takes a very long time 30 sec
                    summation += 1
        return summation

    """

    def get_expertlike_score(self, user_id, beers_associated_list=None):
        if beers_associated_list is None:
            beers_associated_list = self._beers_associated_with_user(user_id)

        summation = 0
        for beer_id in beers_associated_list:
            std = self.beers_ddf.loc[self.beers_ddf["bid"] == beer_id]["std"]
            std = list(std)[0] if len(std.items) > 0 else 0
            if std != 0: # no point measuring exper_like score over beers that have a single rating 
                ref = self.beers_ddf.loc[self.beers_ddf["bid"] == beer_id]["ba_score"]
                # BA_score in [0, 100] and we want it in [0, 5]
                ref = list(ref)[0]*(5/100) if len(ref.items) > 0 else 0 
               
                r_u_b = self.ratings_ddf.loc[self.beers_ddf["bid"] == beer_id].loc[self.beers_ddf["uid"] == user_id]
                r_u_b = list(r_u_b)[0] if len(r_u_b) > 0 else r_b
                summation += ((r_u_b - ref)/std)**2

        return len(beers_associated_list) / summation


    def get_explorer_score(self, user_id, beers_associated_list=None):
        print("EXPLORER SCORE FOR USER ", user_id)
        t = time.time()
        if beers_associated_list is None:
            beers_associated_list = self._beers_associated_with_user(user_id)
        print("   Time prep user beers : ", time.time()-t)

        t = time.time()
        sorted_dates_userId_beerId = self.ratings_ddf.loc[self.ratings_ddf["bid"].isin(beers_associated_list)][["uid","bid","date"]].sort_values("date").compute()
        print("   Time for all beers id :", time.time()-t)

        t = time.time()
        summation = 0
        for beer_id in beers_associated_list:
            sorted_dates_userId = sorted_dates_userId_beerId.loc[sorted_dates_userId_beerId["bid"] == beer_id][["uid","date"]].sort_values("date")
            sorted_userId = list(sorted_dates_userId["uid"])
            
            if user_id in sorted_userId[:min(len(sorted_userId), 10)]:
                summation += 1

        print("   total time ", time.time()-t)
        return summation

    
    def _beers_associated_with_user(self, user_id):
        return list(self.ratings_ddf.loc[self.ratings_ddf["uid"] == user_id]["bid"])
        