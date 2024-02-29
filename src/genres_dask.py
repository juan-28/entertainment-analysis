import dask.dataframe as dd
import os
import time


#
# def load_imdb(filepath1, filepath2, netflix_path):
#     """Loads the datasets using Dask DataFrames."""
#     data_rating = dd.read_csv(filepath1, sep="\t")
#     data_basic = dd.read_csv(filepath2, sep="\t")
#     netflix_data = dd.read_csv(netflix_path)
#     return data_rating, data_basic, netflix_data
#


def load_imdb(filepath1, filepath2, netflix_path):
    data_rating = dd.read_csv(filepath1, sep="\t")
    data_basic = dd.read_csv(
        filepath2,
        sep="\t",
        dtype={"runtimeMinutes": "object", "startYear": "object", "isAdult": "object"},
    )
    netflix_data = dd.read_csv(netflix_path)
    return data_rating, data_basic, netflix_data


def merge_and_save(data_basic, data_rating, netflix_data, output_path):
    """Performs merging and saving, optimized with Dask."""
    final_data = data_basic[
        [
            "tconst",
            "titleType",
            "primaryTitle",
            "isAdult",
            "startYear",
            "runtimeMinutes",
            "genres",
        ]
    ]
    merged_data = dd.merge(final_data, data_rating, on="tconst")

    merged_data["Title_lower"] = merged_data["primaryTitle"].str.lower()
    netflix_data["Title_lower"] = netflix_data["Title"].str.lower()

    idxmax_computed = merged_data.groupby("Title_lower")["numVotes"].idxmax().compute()
    merged_data["Title_lower"] = merged_data["Title_lower"].astype(str)  #
    merged_data = merged_data.set_index("Title_lower")
    imdb_max_votes = merged_data.loc[idxmax_computed]

    final_merge = dd.merge(imdb_max_votes, netflix_data, on="Title_lower", how="inner")
    final_merge.to_csv(f"{output_path}final_merged_data_dask.csv", index=False)


# ------------------------------
# Main Execution (Sample Paths)
# ------------------------------
IMDB_RATINGS_PATH = "/Users/pranavsukumaran/Desktop/netflix/project/data/data-2.tsv"
IMDB_BASICS_PATH = "/Users/pranavsukumaran/Desktop/netflix/project/data/data-3.tsv"
NETFLIX_PATH = "/Users/pranavsukumaran/Desktop/netflix/project/data/processed_data.csv"
OUTPUT_PATH = "/Users/pranavsukumaran/Desktop/netflix/project/data/"
start_time = time.time()
# Load the datasets
data_rating, data_basic, netflix_data = load_imdb(
    IMDB_RATINGS_PATH, IMDB_BASICS_PATH, NETFLIX_PATH
)

# Perform merging and saving
merge_and_save(data_basic, data_rating, netflix_data, OUTPUT_PATH)
end_time = time.time()

total_time = end_time - start_time

print(f"Processing time using Dask: {total_time:.2f} seconds")
