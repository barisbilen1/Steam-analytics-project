from pathlib import Path
import constants as constants
from datetime import datetime as dt


base_path_str = constants.BASE_PATH_STR
# define paths
base_path = Path(base_path_str)

data_path = base_path / Path("data")

RAW_DATA_PATH_DICT = {
    "app_info_df_path": data_path / Path("applicationInformation.csv"),
    "dev_df_path": data_path / Path("applicationDevelopers.csv"),
    "genre_df_path": data_path / Path("applicationGenres.csv"),
    "publishers_df_path": data_path / Path("applicationPublishers.csv"),
    "languages_df_path": data_path / Path("applicationSupportedlanguages.csv"),
    "count_bottom1000_path": data_path / Path("Playercount_bottom1000.csv"),
    "count_top1000_path": data_path / Path("Playercount_top1000.csv"),
    "price_df_path": data_path / Path("Priceshistory.csv"),
    "tag_df_path": data_path / Path("applicationTags.csv"),
    "packages_df_path": data_path / Path("applicationPackages.csv"),
}


def create_path(file_name: str) -> Path():
    return RAW_DATA_PATH_DICT.get(file_name)


# def read_data(file_name: str, header=0) -> pd.DataFrame():
#     return pd.read_csv(create_path(file_name), encoding='unicode_escape', on_bad_lines='skip', names=)


# Function to check if the date string matches the format
def is_valid_date(date_str):
    try:
        dt.strptime(date_str, "%d-%b-%y")
        return True
    except ValueError:
        return False


def get_non_unique_rows(df, group_cols):
    """
    Return rows that are part of non-unique groups based on group_cols.

    Parameters:
    - df (pd.DataFrame): The input DataFrame.
    - group_cols (list or str): Column(s) to group by.

    Returns:
    - pd.DataFrame: Rows that are in groups appearing more than once.
    """
    mask = df.duplicated(subset=group_cols, keep=False)
    return df[mask]
