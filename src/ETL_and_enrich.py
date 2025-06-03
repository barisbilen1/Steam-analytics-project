from pathlib import Path

import pandas as pd

import utils as utils
import constants as constants

BASE_PATH = Path(constants.BASE_PATH_STR)
(BASE_PATH / Path("cleaned_data")).mkdir(exist_ok=True)

WRITE_DATA_PATH = BASE_PATH / Path("cleaned_data")

# This script will be used to clean the data, put them into a structured format before the data modelling phase.

def clean_app_info():
    app_info_df = pd.read_csv(
        utils.create_path("app_info_df_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
    )

    # I have observed in exploration_notebook.ipynb that when a releasedate or type info is NaN, that record is mostly for a kit, or test version of a game. I don't think they will add any value to our analysis thus I'll exlcude them (in reality, we'd need to check this with client)
    # Exclude records with NaN in 'type' or 'releasedate'
    app_info_df = app_info_df[
        ~app_info_df["type"].isnull() | ~app_info_df["releasedate"].isnull()
    ]

    # Remove duplicate names
    app_info_df = app_info_df.drop_duplicates("name") # need to remove duplicate Civilization IV records (see exploration notebook cell 10). this keeps the first observation for that name by default

    # we also need to convert date field into somewhat a more standard format like YYYY-MM-DD
    # above line gives error because there are some observations in the table which has only day and month without having a year info. we need to clean them up.
    
    # Keep only valid date rows
    app_info_df["has_two_dashes"] = (
        app_info_df["releasedate"].astype(str).str.count("-") == 2
    )
    app_info_df = app_info_df[app_info_df.has_two_dashes]

    # Convert to datetime in 'DD-MM-YYYY' format
    app_info_df["releasedate"] = pd.to_datetime(
        app_info_df["releasedate"], format="%d-%b-%y"
    ).dt.strftime("%Y-%m-%d")
    app_info_df = app_info_df.drop("has_two_dashes", axis=1)

    # now we have a clean data for app_info and we are ready to save this as our "silver" table. Silver here means the intermediate table (raw data=bronze and final tables on db=gold)

    # Convert 'freetoplay' to int
    app_info_df["freetoplay"] = app_info_df["freetoplay"].astype(int)

    app_info_df.to_csv(WRITE_DATA_PATH / "app_info_cleaned.csv", index=False)


def clean_developers():
    col_names_dev_df = ["appid", "developerfirm"]
    # assign colnames first
    dev_df = pd.read_csv(
        utils.create_path("dev_df_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
        names=col_names_dev_df,
    )
    # remove NaN entries
    dev_df = dev_df.dropna(subset=["developerfirm"])
    dev_df.to_csv(WRITE_DATA_PATH / "dev_info_cleaned.csv", index=False)


def clean_genres():
    # this one is tricky as I want to change the structure of this dataframe. with the current structure, genres being listed in columns, is weird and hard to use for reporting and/or machine learning purposes.
    col_names_genre_df = ["appid", "genre1", "genre2", "genre3"]
    genre_df = pd.read_csv(
        utils.create_path("genre_df_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
        names=col_names_genre_df,
    )

    # first melt the df to bring dataframe into long format
    melted_df = genre_df.melt(
        id_vars=["appid"], value_vars=["genre1", "genre2", "genre3"], value_name="genre"
    ).drop(["variable"], axis=1)

    melted_df = melted_df.dropna()
    melted_df = melted_df.sort_values("appid")
    melted_df = melted_df[melted_df.genre != "60"]

    # IMPORTANT: save this df first as we will trim some information from this df later on. this df will serve as the original table for genre information used in "REPORTING".
    melted_df.to_csv(
        WRITE_DATA_PATH / "genre_info_melted_original_cleaned.csv", index=False
    )

    # I will one-hot encode them. however, there are a lot of genres we have and one-hot encoding them will produce lots of columns, which will reduce interpretability and increase data size unnecessarily. so, before doing that, I will group some of the similar genres together, to reduce genre count.
    # apply custom dimensionality reduction (PCA can be used as well but I don't want to complicate things in the beginning) for machine learning purposes
    # I will group genres into 7 groups:

    action_group = ["Action", "Adventure", "Gore", "Violent"]
    utility_group = [
        "Animation & Modeling",
        "Audio Production",
        "Design & Illustration",
        "Education",
        "Photo Editing",
        "Software Training",
        "Utilities",
        "Video Production",
        "Web Publishing",
    ]
    rpg_group = ["RPG", "Simulation"]
    casual_games_group = ["Casual", "Indie"]
    sex_cont_group = ["Nudity", "Sexual Content"]
    free_to_play_group = ["Early Access", "Free to Play"]
    racing_and_sports = ["Racing", "Sports"]

    def map_genre(x):
        if x in action_group:
            return "Action"
        if x in utility_group:
            return "Utilities"
        if x in rpg_group:
            return "RPG"
        if x in casual_games_group:
            return "Casual"
        if x in sex_cont_group:
            return "Sexual_and_violence"
        if x in free_to_play_group:
            return "Free_to_play"
        if x in racing_and_sports:
            return "Racing_and_sports"
        return x

    melted_df["genre"] = melted_df["genre"].apply(map_genre)
    melted_df = melted_df.drop_duplicates()

    # with 9 categories in total, I think we can now one-hot encode them.
    # One-hot encode genres
    one_hot_encoded_genre_df = pd.get_dummies(melted_df, columns=["genre"])
    one_hot_encoded_genre_df.to_csv(
        WRITE_DATA_PATH / "genre_info_cleaned.csv", index=False
    )


def clean_publishers():
    col_names_publishers_df = ["appid", "publisher"]
    publishers_df = pd.read_csv(
        utils.create_path("publishers_df_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
        names=col_names_publishers_df,
    )
    # if publisher is NA, impute them with "unknown"
    publishers_df["publisher"].fillna("unknown", inplace=True)
    publishers_df.to_csv(WRITE_DATA_PATH / "publisher_info_cleaned.csv", index=False)


def clean_languages():
    # this one is probably the hardest one to model and handle. hard to one-hot encode 17 (we have more languages but a game can have 17 language at most) possible languages per game.
    languages_df = pd.read_csv(
        utils.create_path("languages_df_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
        header=None,
    )
    languages_df = languages_df.rename({0: "appid"}, axis=1)

    # First I tried to trim the language df so that it has fewer categorical variables (by assigning a binary indicator whether a game is supported in more than 5 languages or not) in order to one-hot encode for ML purposes. however I changed my mind because that would lead important piece of information to be lost. instead I'll just melt the df, and found the number of languages that a game is offered in. this way is more convenient both for reporting and machine learning purposes. Keeping individual languages is too much burden.
    # melted df will be used in reporting, and number of languages per game is more of a feature engineering effort that will be used when training the model.

    dummy_list_1_17 = list(range(1, 17))
    languages_melted_df = languages_df.melt(
        id_vars=["appid"], value_vars=dummy_list_1_17, value_name="language"
    ).drop(["variable"], axis=1)

    languages_melted_df = languages_melted_df.dropna()
    languages_melted_df = languages_melted_df.sort_values("appid")

    # WARNING: I am observing some leading space characters the beginning. we should remove them
    languages_melted_df["language"] = languages_melted_df["language"].str.lstrip()

    # enrich df with language count per game (can be used both in ML and reporting)

    # Language count per game
    languages_per_game = (
        languages_melted_df.groupby("appid")["language"]
        .apply(lambda x: x.notnull().sum())
        .reset_index()
        .rename({"language": "language_count"}, axis=1)
    )

    languages_melted_df = languages_melted_df.merge(
        languages_per_game, on=["appid"], how="left"
    )

    languages_melted_df.to_csv(WRITE_DATA_PATH / "languages_per_game.csv", index=False)

    # also, I want to see which languages are supported the most
    language_counts_df = languages_melted_df["language"].value_counts().reset_index()
    language_counts_df.rename(
        {"index": "language", "language": "lang_occurrence_count"}, axis=1, inplace=True
    )

    # we see that almost entirety of the games are supported in English (not surprising isn't it), and around half of them are supported in German, French and Spanish.
    language_counts_df.to_csv(
        WRITE_DATA_PATH / "language_counts_cleaned.csv", index=False
    )


def clean_player_counts():
    count_bottom1000_df = pd.read_csv(
        utils.create_path("count_bottom1000_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
    )
    # first I have noticed that app_id column needs to be appid to be consistent with the others
    count_bottom1000_df.rename({"app_id": "appid"}, axis=1, inplace=True)

    # FIRST STEP: VERY IMPORTANT TO CHECK UNIQUENESS ON FACT TABLES
    # I expect one observation per game per day
    utils.get_non_unique_rows(df=count_bottom1000_df, group_cols=["appid", "Time"])

    # empty. great. same check will be performed over top 1000 count dataste as well.

    # I want to assign a new column that shows the count of observations per game.
    # in such fact tables, it is important to check this as we want to make sure of the completeness

    count_bottom1000_df["date_count"] = count_bottom1000_df.groupby("appid")[
        "Time"
    ].transform("count")

    count_bottom1000_df[count_bottom1000_df.date_count != 973]  # all has 973 dates, good.

    # NA check
    count_bottom1000_df[
    count_bottom1000_df.isna().any(axis=1)
]  # full of NAs for a specific game, let's exclude.

    count_bottom1000_df = count_bottom1000_df.dropna()
    count_bottom1000_df.rename({"Time": "date"}, axis=1, inplace=True)
    count_bottom1000_df["date"] = pd.to_datetime(count_bottom1000_df["date"])
    # let's enrich the dataset with week and year columns
    count_bottom1000_df["year"] = count_bottom1000_df["date"].dt.isocalendar().year
    count_bottom1000_df["week"] = count_bottom1000_df["date"].dt.isocalendar().week

    count_top1000_df = pd.read_csv(
        utils.create_path("count_top1000_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
    )
    utils.get_non_unique_rows(df=count_top1000_df, group_cols=["app_id", "Time"])

    # great, also unique at game-hour level.

    # I will aggregate this dataset to day-level. My reasoning is as follows;
    # 1) I have some questions in my mind that I'd like to answer out of this data, such as, what kind of attributes we can use to infer game's success, or, the seasonalities of a game's player count, or, effect of promotions on player counts. So, my end goal is to answer these high level business questions, this is why I am not interested in hour-level granualarity.

    # 2) I am not sure about whether these hours are coming from player's timezone or a fixed timezone like CET. This makes it even harder to trust the information.

    # 3) Some other critical data like player count data for bottom 1000 games and price info have day-level granularity, so I want to aggregate this data as well to bring it on the same page. It'll also help me reduce the data size.
    count_top1000_df["Time"] = pd.to_datetime(count_top1000_df["Time"])
    count_top1000_df["date"] = count_top1000_df["Time"].dt.strftime("%Y-%m-%d")
    count_top1000_df.rename({"app_id": "appid"}, axis=1, inplace=True)

    # I'll take daily averages and believe that it is fairly a good and risk-free approach. we will simply ignore intra-day shifts.
    count_top1000_df_agg = (
        count_top1000_df.groupby(["appid", "date"])["Playercount"].mean().reset_index()
    )
    count_top1000_df_agg = count_top1000_df_agg.dropna()
    count_top1000_df_agg["Playercount"] = (
        count_top1000_df_agg["Playercount"].round(0).astype(int)
    )
    count_top1000_df_agg["date_count"] = count_top1000_df_agg.groupby("appid")[
        "date"
    ].transform("count")

    count_top1000_df_agg[count_top1000_df_agg.date_count != 973]  # all has 973 dates, good.

    count_top1000_df_agg["date"] = pd.to_datetime(count_top1000_df_agg["date"])
    count_top1000_df_agg["year"] = count_top1000_df_agg["date"].dt.isocalendar().year
    count_top1000_df_agg["week"] = count_top1000_df_agg["date"].dt.isocalendar().week

    ## I don't think keeping top and bottom counts separately adds any value, thus I will concat them and write as a single file. However, before writing the file, there is one very important check I want to perform, which is uniqueness check. I want my data to be unique at appid-date level, otherwise it will distort analyses.
    player_count_concat = pd.concat([count_top1000_df_agg, count_bottom1000_df], axis=0)

    dup_row_count = len(
        utils.get_non_unique_rows(df=player_count_concat, group_cols=["appid", "date"])
    )
    print("Duplicate row count in player count df is:", dup_row_count)

    player_count_concat["week_start_date"] = player_count_concat[
        "date"
    ] - pd.to_timedelta(player_count_concat["date"].dt.weekday, unit="d")
    player_count_concat.drop("date_count", axis=1, inplace=True)
    player_count_concat.to_csv(
        WRITE_DATA_PATH / "player_counts_cleaned.csv", index=False
    )


def clean_price():
    price_df = pd.read_csv(
        utils.create_path("price_df_path"),
        encoding="unicode_escape",
        on_bad_lines="skip",
    )
    price_df.rename({"app_id": "appid"}, axis=1, inplace=True)
    price_df.rename({"Date": "date"}, axis=1, inplace=True)

    # I noticed that price data covers a smaller time window than player count datasets. Player counts cover from 2017 to 2020 August while price df covers 2019-04 to 2020-08. We need to keep in mind that we don't have price information before 2019-04 when doing out analysis.
    # I am planning to join this data with player count data later, thus, it is VERY important that each game has a single price information for each date. Also if there are any NAsin between non-NA prices, NA ones can safely be filled with the latest non-NA observation.

    price_df["date_count"] = price_df.groupby("appid")["date"].transform("count")
    
    # IMPORTANT OBSERVATION: some games have price information for a very limited time frame, appid=330830 is an example, it has only 45 days of price data. Probably these games were removed from the platform after X days thus a price information is not available. We cannot dummy fill price information for games, thus we need to settle with the nulls when we join player counts with price data. There is nothing we can do. This dataset is far from complete.     # I'll remove Finalprice column as it can easily be computed based on InitialPrice and Discount fields if needed.
    price_df.drop("Finalprice", axis=1, inplace=True)
    price_df.drop("date_count", axis=1, inplace=True)

    price_df[
    price_df.isna().any(axis=1)
]  # dataset is NA free, no need to assign leading/lagging non-NAs to NAs.

    price_df.to_csv(WRITE_DATA_PATH / "price_cleaned.csv", index=False)

# Tag and package dfs will not be used thus will not be cleaned here.
# I have limited time and I don't think I can extract valuable piece of information from them. The information contained in Tags is very similar to genres_df, and package df is hard to interpret.
# one more note: Tags.csv can be useful for searching/querying purposes in the website, but hard to visualize in a dashboard.

def main():
    clean_app_info()
    clean_developers()
    clean_genres()
    clean_publishers()
    clean_languages()
    clean_player_counts()
    clean_price()


if __name__ == "__main__":
    main()
