#splititng game sin to two rows to help with the EDA rolling averages
import pandas as pd
import numpy as np


INPUT_PATH  = "Data/nba_games_with_player_details.csv"
OUTPUT_PATH = "Data/nba_games_team_split.csv"

DATE_COL   = "GAME_DATE"
SEASON_COL = "SEASON"

# stat base names — script will look for {base}_TEAM1 and {base}_TEAM2
# any base not found in the data is silently skipped
STAT_BASES = [
    "PTS",
    "REB",
    "AST",
    "STL",
    "BLK",
    "FG_PCT",
    "FG3_PCT",
    "FT_PCT",
    "player_ct",
    "avg_center",
    "min_center",
    "max_center",
    "avg_forward",
    "min_forward",
    "max_forward",
    "avg_guard",
    "min_guard",
    "max_guard",
]

# h2h columns handled separately (non-standard naming in source data)
H2H_WIN_RATE_COL    = "h2h_win_rate_TEAM1"   # only TEAM1 perspective stored
H2H_GAMES_PLAYED_COL = "h2h_games_played"    # single shared column

print("Loading data ...")
data = pd.read_csv(INPUT_PATH)
data[DATE_COL] = pd.to_datetime(data[DATE_COL])
data = data.sort_values(DATE_COL).reset_index(drop=True)
print(f"  {len(data)} rows loaded")
print(f"  Columns: {list(data.columns)}\n")


# only keep stat bases that have both _TEAM1 and _TEAM2 columns
valid_bases = [
    b for b in STAT_BASES
    if f"{b}_TEAM1" in data.columns and f"{b}_TEAM2" in data.columns
]
skipped = set(STAT_BASES) - set(valid_bases)
if skipped:
    print(f"  Skipped (not found in both sides): {sorted(skipped)}")
print(f"  Valid stat bases: {valid_bases}\n")


#unstack splitting the two teams for a game
# shared columns that apply to both rows of the same game
SHARED_COLS = [DATE_COL, SEASON_COL]
if "GAME_ID" in data.columns:
    SHARED_COLS.append("GAME_ID")
if H2H_GAMES_PLAYED_COL in data.columns:
    SHARED_COLS.append(H2H_GAMES_PLAYED_COL)

# build rename maps: TEAM1/TEAM2 → TEAM, {stat}_TEAM1/2 → {stat}
team1_rename = {"TEAM1": "TEAM"}
team2_rename = {"TEAM2": "TEAM"}
for b in valid_bases:
    team1_rename[f"{b}_TEAM1"] = b
    team2_rename[f"{b}_TEAM2"] = b

# slice each side — only columns that exist
cols_side1 = [c for c in SHARED_COLS + ["TEAM1"] +
              [f"{b}_TEAM1" for b in valid_bases] if c in data.columns]
cols_side2 = [c for c in SHARED_COLS + ["TEAM2"] +
              [f"{b}_TEAM2" for b in valid_bases] if c in data.columns]

side1 = data[cols_side1].rename(columns=team1_rename).copy()
side2 = data[cols_side2].rename(columns=team2_rename).copy()

# h2h win rate — TEAM1 perspective stored; TEAM2 gets the complement
if H2H_WIN_RATE_COL in data.columns:
    side1["h2h_win_rate"] = data[H2H_WIN_RATE_COL].values
    side2["h2h_win_rate"] = 1 - data[H2H_WIN_RATE_COL].values

# home / away flag
side1["is_home"] = 1
side2["is_home"] = 0

# opponent column
side1["opponent"] = data["TEAM2"].values
side2["opponent"] = data["TEAM1"].values

# win flag — derived from points if available
if "PTS" in side1.columns:
    side1["win"] = (
        data["PTS_TEAM1"].values > data["PTS_TEAM2"].values
    ).astype(int)
    side2["win"] = (
        data["PTS_TEAM2"].values > data["PTS_TEAM1"].values
    ).astype(int)
else:
    print("  PTS not found — win flag not added")

# stack and sort chronologically within each team and season
team_data = (
    pd.concat([side1, side2], ignore_index=True)
    .sort_values([SEASON_COL, DATE_COL, "TEAM"])
    .reset_index(drop=True)
)

team_data.to_csv(OUTPUT_PATH, index=False)
print(f"\nSaved to {OUTPUT_PATH}")
print(f"Columns in output: {list(team_data.columns)}")