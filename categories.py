
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
        self.beers_ddf['std_rating']=self.beers_ddf.apply(lambda beers : 0 if (beers['n_ratings']==0 or beers['n_ratings']==1) else np.sqrt(beers['std_rating']/beers['n_ratings']),axis=1)#,meta=('x', 'f8'))
    
    def _avg_at_date(self):
        a = self.ratings_ddf[["date", "uid", "bid", "rating"]]

    def _beers_associated_with_user(self):
        # self.associated_beers=self.ratings_ddf.groupby('uid')['bid'].apply(list).reset_index('associated_beers')
        self.associated_beers=self.ratings_ddf.groupby('uid').apply(list)['bid'].reset_index('associated_beers')
        

    def _ratings_dated(self):
        dated_ratings_str = "dated_rating"
        
        self.ratings_ddf = self.ratings_ddf.sort_values(['bid','date'])
        self.ratings_ddf[dated_ratings_str] = -1

        previous_bid = -1
        summation = 0
        number = 0
        for index, row in self.ratings_ddf.iterrows():
            if previous_bid != row['bid']:
                number = 0
                summation = 0

            number += 1
            summation += row["rating"]
            self.ratings_ddf.at[index, dated_ratings_str] = summation / number

            #self.ratings_ddf[dated_ratings_str][index] = summation / number
            previous_bid = row["bid"]


    def _beers_per_user(self):
        
        #self.ratings_ddf=self.ratings_ddf.sort_values(['bid','date'])
        self.ratings_ddf = self.ratings_ddf.sort_values(["uid"])

        #previous_bid = self.ratings_ddf["bid"]
        previous_uid = -1
        beers_per_users = dict()
        l = None
        for _, row in self.ratings_ddf.iterrows():
            if previous_uid != row['uid']:
                if l is not None:
                    beers_per_users[previous_uid] = l
                    #beers_per_users.append((previous_uid, l))
                l = []

            l.append(row["bid"])
            previous_uid = row["uid"]
        return beers_per_users

              

    def __init__(self, ratings_parquets_path, beers_parquet_path, users_parquet_path, SIZE=300):
        self.ratings_ddf = ing.read_parquet(ratings_parquets_path)[["date", "uid", "bid","rating"]].compute()
        
        #self.ratings_ddf = self.ratings_ddf.head(SIZE)
        self.ratings_ddf.set_index("bid")
        print("ratings loaded")

        self.beers_ddf = pd.read_parquet(beers_parquet_path)[["bid", "n_ratings", "avg_rating", "ba_score", "abv"]]
        #self.beers_ddf = self.beers_ddf.head(SIZE)
        print("beers loaded")
        
        self.users_ddf = pd.read_parquet(users_parquet_path)[["uid"]]
        #self.users_ddf = self.users_ddf.head(SIZE)
        print("users loaded")

        t = time.time()
        self.beers_per_users = self._beers_per_user()
        print("beers per user (", time.time()-t,")")
        
        t = time.time()
        self._std()
        print("std (", time.time()-t,")")
        
        t = time.time()
        self._ratings_dated()
        print("ratings dated (", time.time()-t,")")

        
    def get_ratings_head(self, h = 30):
        return self.ratings_ddf.sort_values(by="bid").head(h)

    def get_beers_head(self, h=30):
        return self.beers_ddf.sort_values(by="bid").head(h)
    
    def get_users_head(self, h=30):
        return self.users_ddf.head(h)

    def get_userIds_list(self):
        return list(self.users_ddf["uid"])


    def get_all_scores(self, user_id):
        self.user=self.users_ddf['uid'==user_id]
        self.beers=self.beers_ddf[self.beers_ddf['bid'].isin(self.user['associated_list'])]
        
        summation = 0

        conformist_score = self.get_conformist_score(user_id)
        adventurer_score = self.get_adventurer_score(user_id)
        expertlike_score = self.get_expertlike_score(user_id)
        explorer_score = self.get_explorer_score(user_id)

        return conformist_score, adventurer_score, expertlike_score, explorer_score


    def get_conformist_score(self, user_id):

        #for beer_id in self.associated_beers['uid']: # iterate over all the beers rated by the user
        self.beers_ddf.sort_values(by="bid")
        
        for beer_id in self.beers_per_users[user_id]:
            beer_with_id = self.beers_ddf.loc[self.beers_ddf["bid"] == beer_id][["std", "avg_rating"]]
            std = list(beer_with_id["std"])
            std = std[0] if len(std) > 0 else 0
            if std != 0: # no point measuring conformity score over beers that have a single rating 
                r_b = list(beer_with_id["avg_rating"])
                r_b = r_b[0] if len(r_b) > 0 else 0
                
                r_u_b = self.ratings_ddf.loc[self.ratings_ddf["bid"] == beer_id].loc[self.ratings_ddf["uid"] == user_id]
                r_u_b = list(r_u_b)[0] if len(r_u_b) > 0 else r_b
                summation += ((r_u_b - r_b)/std)**2

        return summation / len(self.associated_beers['uid'])


    def get_adventurer_score2(self, user_id):
        print("ADVENTURER SCORE FOR ", user_id)

        for beer_id in self.beers_per_users[user_id]:
            
            rating_of_beer_dated = list(self.ratings_ddf.loc[self.ratings_ddf["bid"] == beer_id].loc[self.ratings_ddf["uid"] == user_id][[dated_rating]])
            rating_of_beer_dated = rating_of_beer_dated[0] if len(rating_of_beer_dated) > 0 else np.inf
            if rating_of_beer_dated < T:
                summation += 1
        return summation


    def get_adventurer_score(self, user_id):


        print("ADVENTURER SCORE FOR ", user_id)

        t=time.time()
        ratings_of_beers_dated = self.ratings_ddf.loc[self.ratings_ddf["bid"].isin(self.associated_beers['uid'])][["date", "uid", "bid", "rating"]]
        print("   time ratings for all beers for user : ", time.time()-t)

        t = time.time()
        for beer_id in self.associated_beers['uid']:
            
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


    def get_expertlike_score(self, user_id):

        summation = 0
        for beer_id in self.associated_beers['uid']:
            std = self.beers_ddf.loc[self.beers_ddf["bid"] == beer_id]["std"]
            std = list(std)[0] if len(std.items) > 0 else 0
            if std != 0: # no point measuring exper_like score over beers that have a single rating 
                ref = self.beers_ddf.loc[self.beers_ddf["bid"] == beer_id]["ba_score"]
                # BA_score in [0, 100] and we want it in [0, 5]
                ref = list(ref)[0]*(5/100) if len(ref.items) > 0 else 0 
               
                r_u_b = self.ratings_ddf.loc[self.beers_ddf["bid"] == beer_id].loc[self.beers_ddf["uid"] == user_id]
                r_u_b = list(r_u_b)[0] if len(r_u_b) > 0 else r_b
                summation += ((r_u_b - ref)/std)**2

        return len(self.associated_beers['uid']) / summation


    def get_explorer_score(self, user_id):
        print("EXPLORER SCORE FOR USER ", user_id)
        t = time.time()
        sorted_dates_userId_beerId = self.ratings_ddf.loc[self.ratings_ddf["bid"].isin(self.associated_beers['uid'])][["uid","bid","date"]].sort_values("date")
        print("   Time for all beers id :", time.time()-t)

        t = time.time()
        summation = 0
        for beer_id in self.associated_beers['uid']:
            sorted_dates_userId = sorted_dates_userId_beerId.loc[sorted_dates_userId_beerId["bid"] == beer_id][["uid","date"]].sort_values("date")
            sorted_userId = list(sorted_dates_userId["uid"])
            
            if user_id in sorted_userId[:min(len(sorted_userId), 10)]:
                summation += 1

        print("   total time ", time.time()-t)
        return summation

    
    
        