import os
import numpy as np
import pandas as pd



DATA_PATH  = "Data/nba_games_team_split.csv"

OUT_PATH_split   = "Data/nba_data_split_final.csv"
OUT_PATH_MERGED   = "Data/nba_data_MERGED_final.csv"

TEAM_COL   = "TEAM"
DATE_COL   = "GAME_DATE"
SEASON_COL = "SEASON"


# Metrics to generate rolling averages for
METRICS = [
    "PTS", "REB", "AST", "STL", "BLK",
    "FG_PCT", "FG3_PCT", "FT_PCT",
    "avg_center", "avg_forward", "avg_guard",
    "min_center", "max_center",
    "min_forward", "max_forward",
    "min_guard", "max_guard",
]

# Rolling windows to compute — one column per metric per window
WINDOWS = [3, 5, 10, 15, 30]


print("Loading data ...")
data = pd.read_csv(DATA_PATH)
data = data[data[SEASON_COL] != "2015-16"].copy() #exclude becase no good player history

data[DATE_COL] = pd.to_datetime(data[DATE_COL])
data = data.sort_values([TEAM_COL, DATE_COL]).reset_index(drop=True)

seasons = sorted(data[SEASON_COL].unique())
teams   = sorted(data[TEAM_COL].unique())
print(f" Games:{len(data)/2} \n teams:{len(teams)}  \n seasons:{len(seasons)} ")


# Time since last game per team
data["days_rest"] = (
    data.groupby([TEAM_COL, SEASON_COL])[DATE_COL]
    .transform(lambda gamedates: gamedates.diff().dt.days)
)

data["is_b2b"]  = (data["days_rest"] == 1).astype(float)   # back-to-back game

data["days_rest"] = data["days_rest"].fillna(data["days_rest"].max()) #max the 1st game of the season big rested
n_b2b = data["is_b2b"].sum()
print(f"  Back-to-back games: {int(n_b2b)} ({100*n_b2b/len(data):.1f}% of all games)")
print(f"  Median rest days:   {data['days_rest'].median():.1f}")


# Rolling averages

print(f"  Metrics  : {len(METRICS)}")
print(f"  Windows  : {WINDOWS}")
print(f"  Total new columns: {len(METRICS) * len(WINDOWS)}\n")

feature_cols = []   # track generated column names for summary

for col in METRICS:
    for n in WINDOWS:
        feat_name = f"roll_{col}_{n}"
        data[feat_name] = (
            data.groupby(TEAM_COL)[col]
            .transform(
                lambda g, n=n: g.shift(1).rolling(n, min_periods=max(1, n // 2)).mean()
            )
        )
        feature_cols.append(feat_name)
    print(f"  {col:<20} -> roll_{col}_3, _5, _10, _15, _30")

# Rolling win% — same leakage-safe approach
for n in WINDOWS:
    feat_name = f"roll_win_{n}"
    data[feat_name] = (
        data.groupby(TEAM_COL)["win"]
        .transform(
            lambda g, n=n: g.shift(1).rolling(n, min_periods=max(1, n // 2)).mean()
        )
    )
    feature_cols.append(feat_name)
print(f"  {'win':<20} -> roll_win_3, _5, _10, _15, _30")



os.makedirs(os.path.dirname( OUT_PATH_split), exist_ok=True)
data.to_csv(OUT_PATH_split, index=False)
print(f"\n  Saved : {OUT_PATH_split}")





keep_cols = [
"h2h_win_rate",
"is_home",
]




print("\nCollapsing to one row per game ...")
 
game_level_cols = ["GAME_ID", DATE_COL, SEASON_COL,"h2h_games_played"]
 
#passthrough_present = [c for c in PASSTHROUGH if c in data.columns]
 
team_specific_cols = (
    [TEAM_COL, "win", "days_rest", "is_b2b","player_ct"]
    #+ passthrough_present
    + feature_cols
)
 
home = data[data["is_home"] == 1][game_level_cols + team_specific_cols].copy()
away = data[data["is_home"] == 0][game_level_cols + team_specific_cols].copy()
 
home = home.rename(columns={c: f"{c}_team1" for c in team_specific_cols})
away = away.rename(columns={c: f"{c}_team2" for c in team_specific_cols})
 
games = home.merge(away, on=game_level_cols, how="inner")
 
games["win_team1"] = games["win_team1"].astype(int)
games = games.drop(columns=["win_team2"], errors="ignore")
 
os.makedirs(os.path.dirname( OUT_PATH_MERGED), exist_ok=True)
games.to_csv(OUT_PATH_MERGED, index=False)
print(f"  Saved : {OUT_PATH_MERGED}")


games_trainset = games[games[SEASON_COL] != "2023-24"]
games_trainset.to_csv("Data/nba_data_MERGED_trainset.csv", index=False)
print(f"  Saved : Data/nba_data_MERGED_trainset.csv")
games_testset = games[games[SEASON_COL] == "2023-24"]
games_testset.to_csv("Data/nba_data_MERGED_testset.csv", index=False)
print(f"  Saved : Data/nba_data_MERGED_testset.csv")