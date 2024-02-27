import pandas as pd
import os

def load_and_filter_data(data_path, selected_profiles):
    """
    Loads the CSV data, filters by selected profiles, and converts the 'Duration' column.

    Args:
        data_path (str): Path to the CSV file.
        selected_profiles (list): List of profile names to include.

    Returns:
        pandas.DataFrame: Filtered and preprocessed DataFrame.
    """

    df = pd.read_csv(data_path)
    filtered_data = df[df['Profile Name'].isin(selected_profiles)]
    filtered_data['Duration'] = pd.to_timedelta(filtered_data['Duration'])
    return filtered_data

def preprocess_titles(df):
    """
    Preprocesses titles and classifies them as 'TV Show' or 'Movie'.

    Args:
        df (pandas.DataFrame): DataFrame with a 'Title' column.

    Returns:
        pandas.DataFrame: DataFrame with 'Title' and 'Type' columns.
    """

    def preprocess_and_classify_title(title):
        main_title = title.split(':')[0].strip()
        content_type = 'TV Show' if 'Season' in title or 'Episode' in title or ':' in title else 'Movie'
        return main_title, content_type

    titles_types = df['Title'].apply(preprocess_and_classify_title)
    df['Title'] = titles_types.apply(lambda x: x[0])
    df['Type'] = titles_types.apply(lambda x: x[1])
    return df

def clean_and_format_data(df):
    """
    Cleans the DataFrame, converts 'Duration' to hours, and extracts time components.

    Args:
        df (pandas.DataFrame): DataFrame to be cleaned and formatted.

    Returns:
        pandas.DataFrame: Cleaned and formatted DataFrame.
    """

    df = df.drop(columns=['Attributes', 'Supplemental Video Type', 'Bookmark', 'Latest Bookmark'])
    df['Duration'] = df['Duration'].dt.total_seconds() / 3600
    df['Start Time'] = pd.to_datetime(df['Start Time'])
    df['Hour'] = df['Start Time'].dt.hour
    df['Day'] = df['Start Time'].dt.day
    df['Month'] = df['Start Time'].dt.month
    df['Year'] = df['Start Time'].dt.year
    df = df.drop(columns="Start Time")
    return df

def categorize_devices(df):
    """
    Categorizes devices based on their names.

    Args:
        df (pandas.DataFrame): DataFrame with a 'Device Type' column.

    Returns:
        pandas.DataFrame: DataFrame with a 'Device Category' column.
    """

    def categorize_device(device_name):
        device_name = device_name.lower()  # Convert to lowercase for case-insensitive matching

        if 'iphone' in device_name or 'ipad' in device_name:
            return 'Apple Mobile Devices'
        elif 'chrome' in device_name or 'edge' in device_name or 'safari' in device_name:
            return 'Web Browsers'
        elif 'android tv' in device_name or 'smart tv' in device_name or 'tv' in device_name:
            return 'Smart TVs'
        elif 'streaming stick' in device_name or 'chromecast' in device_name or 'firetv' in device_name or 'apple tv' in device_name or 'roku' in device_name:
            return 'Streaming Devices'
        elif 'ps4' in device_name or 'ps3' in device_name:
            return 'Game Consoles'
        elif 'set top box' in device_name:
            return 'Set Top Boxes'
        else:
            return 'Other'

    df['Device Category'] = df['Device Type'].apply(categorize_device)
    df.drop(columns="Device Type", inplace=True)  # Assuming you don't need it anymore
    return df

# ------------------------------
# Main Execution
# ------------------------------
DATA_PATH = "/Users/pranavsukumaran/Desktop/netflix/data/ViewingActivity.csv"
SELECTED_PROFILES = ["Pranav", "Home", "Priya"]

data = load_and_filter_data(DATA_PATH, SELECTED_PROFILES)
data = preprocess_titles(data.loc[data['Duration'] >= pd.Timedelta(minutes=5)].copy())
data = clean_and_format_data(data)
data = categorize_devices(data)

print(data) 

output_dir = "/Users/pranavsukumaran/Desktop/netflix/project/data/"
output_filepath = os.path.join(output_dir, "processed_data.csv")

data.to_csv(output_filepath, index=False)  

print(f"Data saved to {output_filepath}")
