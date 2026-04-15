import pandas as pd
import time
from tqdm import tqdm

from nba_api.stats.endpoints import leaguegamelog

# Set working directory
WORK_DIR = "/Users/noemivalle/Documents/School/STA6636-HighDimensionDataAnylisis/Project"

# -----------------------------
# Last 10 Seasons
# -----------------------------

seasons = [
    "2015-16","2016-17","2017-18","2018-19","2019-20",
    "2020-21","2021-22","2022-23","2023-24","2024-25"
]


all_games = []

# -----------------------------
# Get Team Game Logs
# -----------------------------

for season in tqdm(seasons):

    try:
        games = leaguegamelog.LeagueGameLog(
            season=season,
            season_type_all_star="Regular Season",
            player_or_team_abbreviation="T"
        )

        df = games.get_data_frames()[0]

        df["SEASON"] = season

        all_games.append(df)

        time.sleep(1)

    except Exception as e:
        print("Error:", season, e)


games_df = pd.concat(all_games, ignore_index=True)


# -----------------------------
# Select Columns
# -----------------------------

games_df = games_df[
    [
        "SEASON",
        "GAME_ID",
        "GAME_DATE",
        "TEAM_ID",
        "TEAM_ABBREVIATION",
        "PTS",
        "REB",
        "AST",
        "STL",
        "BLK",
        "FG_PCT",
        "FG3_PCT",
        "FT_PCT"
    ]
]


# -----------------------------
# Create Team Number (1 or 2)
# -----------------------------

games_df["TEAM_NUM"] = games_df.groupby("GAME_ID").cumcount() + 1


# -----------------------------
# Pivot to One Row Per Game
# -----------------------------

final_df = games_df.pivot(
    index=["SEASON", "GAME_ID", "GAME_DATE"],
    columns="TEAM_NUM"
)


# -----------------------------
# Flatten Columns
# -----------------------------

final_df.columns = [
    f"{col[0]}_TEAM{col[1]}" for col in final_df.columns
]

final_df = final_df.reset_index()


# -----------------------------
# Rename Team Columns
# -----------------------------

final_df = final_df.rename(
    columns={
        "TEAM_ABBREVIATION_TEAM1": "TEAM1",
        "TEAM_ABBREVIATION_TEAM2": "TEAM2",
        "TEAM_ID_TEAM1": "TEAM1_ID",
        "TEAM_ID_TEAM2": "TEAM2_ID"
    }
)


# -----------------------------
# Save CSV
# -----------------------------
# Build filepath
file_path = os.path.join(WORK_DIR, "nba_games.csv")

final_df.to_csv(file_path, index=False)

print(final_df.head())
print("Total games:", len(final_df))
