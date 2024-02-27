import pandas as pd

# loading imdb data

data_rating = pd.read_csv(
    "/Users/pranavsukumaran/Desktop/netflix/project/data/data-2.tsv", sep="\t"
)

data_basic = pd.read_csv(
    "/Users/pranavsukumaran/Desktop/netflix/project/data/data-3.tsv", sep="\t"
)
