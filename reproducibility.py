"""
    Run this script from the root respecting the file hierarchy below to reproduce the processed data.
    Please create the necessary folders before running the script.
    "root" refers to where the script should be with respect to the folders and files, and is not a folder itself.
    
    Notation: X{Y, Z} is a folder of name X containing files/folders Y and Z
    
    Expected File Hierarchy (before running this script)
    root {
        Data {
            BeerAdvocate {
                users.csv,
                beers.csv,
                ratings.txt
            }
        }
    }
    
    Produced File Hierarchy (after running this script)
    root {
        Data {
            BeerAdvocate {
                users.csv,
                beers.csv,
                ratings.txt
            }
        }
        RefinedData {
            BeerAdvocate {
                users.parquet,
                beers.parquet,
                ratings.parquet
                ratings.csv
            }
        }
    }
"""

from processing import beers_pipeline, users_pipeline, ratings_pipeline
from ingestion import txt2csv

__ALL_TAGS = [
    "beer_name", 
    "beer_id", 
    "brewery_name",
    "brewery_id",
    "style",
    "abv",
    "date",
    "user_name",
    "user_id",
    "appearance",
    "aroma",
    "palate",
    "taste",
    "overall",
    "rating",
    "text",
    "review"
]
__SELECTED_TAGS = [
    "date",
    "beer_id", 
    "user_id",
    "brewery_id",
    "appearance",
    "aroma",
    "palate",
    "taste",
    "overall",
    "rating",
    "review",
    "text"
]

if __name__ == "__main__":
    print("reproduce data...")
    
    print("parsing: ./Data/BeerAdvocate/ratings.txt")
    txt2csv(
        from_path="./Data/BeerAdvocate/ratings.txt", 
        to_path="./RefinedData/BeerAdvocate/ratings.csv",
        selected_tags=__SELECTED_TAGS,
        all_tags=__ALL_TAGS)
    print("file parsed to: ./RefinedData/BeerAdvocate/ratings.csv")
    print("parsing done.")
    
    print("processing users")
    users_pipeline(persist=True)
    print("done! in ./RefinedData/BeerAdvocate/users.parquet")
    
    print("processing ratings")
    ratings_pipeline(persist=True, users_persisted=True)
    print("done! persisted in ./RefinedData/BeerAdvocate/ratings.parquet")
    
    print("processing beers")
    beers_pipeline(persist=True, ratings_persisted=True)
    print("done! persisted in ./RefinedData/BeerAdvocate/beers.parquet")
    
    print("data reproduced.")