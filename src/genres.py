import pandas as pd
import time

### File Paths ####
filepath1 = "/Users/pranavsukumaran/Desktop/Personal_dev/netflix/project/data/data.tsv"
filepath2 = (
    "/Users/pranavsukumaran/Desktop/Personal_dev/netflix/project/data/data-2.tsv"
)
netflix_path = "/Users/pranavsukumaran/Desktop/Personal_dev/netflix/project/data/processed_data.csv"
output_path = "/Users/pranavsukumaran/Desktop/Personal_dev/netflix/project/data/"
# ----------------------------------------------------------------------------------------#


# Function to load data
def load_imdb(filepath1, filepath2, netflix_path):

    data_rating = pd.read_csv(filepath1, sep="\t")
    data_basic = pd.read_csv(filepath2, sep="\t")
    netflix_data = pd.read_csv(netflix_path)
    return data_rating, data_basic, netflix_data


def merge_and_save(data_basic, data_rating, netflix_data, output_path):
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
    merged_data = pd.merge(final_data, data_rating, on="tconst")
    merged_data["Title_lower"] = merged_data["primaryTitle"].str.lower()
    netflix_data["Title_lower"] = netflix_data["Title"].str.lower()

    def map_to_netflix_type(imdb_type):
        if imdb_type in ["movie", "short"]:
            return "Movie"
        elif imdb_type in ["tvSeries", "tvMiniSeries"]:
            return "TV Show"
        return None

    merged_data["Mapped_Type"] = merged_data["titleType"].apply(map_to_netflix_type)
    type_matched_data = pd.merge(
        merged_data, netflix_data[["Title_lower", "Type"]], on="Title_lower"
    )
    type_matched_data = type_matched_data[
        type_matched_data["Mapped_Type"] == type_matched_data["Type"]
    ]
    imdb_max_votes = type_matched_data.loc[
        type_matched_data.groupby("Title_lower")["numVotes"].idxmax()
    ]
    final_merge = imdb_max_votes.merge(netflix_data, on="Title_lower", how="inner")
    final_merge = final_merge[
        ~final_merge["titleType"].isin(["tvMovie", "video", "videoGame"])
    ]
    final_merge = final_merge.drop(
        [
            "tconst",
            "isAdult",
            "runtimeMinutes",
            "Title_lower",
            "Title",
            "Type_x",
            "Mapped_Type",
        ],
        axis=1,
    )
    final_merge.to_csv(f"{output_path}final_merged_data.csv", index=False)


# execution
start_time = time.time()
data_rating, data_basic, netflix_data = load_imdb(filepath1, filepath2, netflix_path)
final_merge = merge_and_save(data_basic, data_rating, netflix_data, output_path)
end_time = time.time()
total_time = end_time - start_time
print(f"Processing time using Pandas: {total_time:.2f} seconds")
print(" ")
print("---------------------------------------------------------")
print(" ")
