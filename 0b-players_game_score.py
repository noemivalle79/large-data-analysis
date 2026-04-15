import pandas as pd
import time
import os
import re

# Set working directory


box_score_file= "Data/Box Score Stats.csv"
box_score_df = pd.read_csv(box_score_file)

players_file = "Data/player_data.csv"
players = pd.read_csv(players_file) 

box_score_df = pd.read_csv(box_score_file)

box_score_df = box_score_df[box_score_df["Season"] >= 2005]

Player_scores_df = box_score_df[
    [
        "Season",
        "Game_ID",
        "GAME_DATE",
        "PLAYER_NAME",
        "Team",
        "PTS",
        "AST",
        "REB",
        "STL",
        "BLK",
        "FG_PCT",
    ]
]

Player_scores_df.rename(columns={"Game_ID": "GAME_ID"}, inplace=True)


def clean_name(name):
    name = str(name).lower().strip()
    name = re.sub(r"[^\w\s]", "", name)
    return name

Player_scores_df["PLAYER_NAME"] = Player_scores_df["PLAYER_NAME"].apply(clean_name)
players["name"] = players["name"].apply(clean_name)

Player_scores_df = Player_scores_df.merge(
    players[["name", "position"]],
    left_on="PLAYER_NAME",
    right_on="name",
    how="left"
)

Player_scores_df["POSITION_MAIN"] = Player_scores_df["position"].str.split("-").str[0]

def score_guard(row):
    return (
        row["PTS"] * 1.0 +
        row["AST"] * 1.2 +
        row["STL"] * 1.0 +
        row["REB"] * 0.5 +
        row["BLK"] * 0.3 +
        row["FG_PCT"] * 10
    )

def score_forward(row):
    return (
        row["PTS"] * 1.0 +
        row["REB"] * 1.0 +
        row["AST"] * 0.8 +
        row["STL"] * 0.8 +
        row["BLK"] * 0.8 +
        row["FG_PCT"] * 10
    )
    
def score_center(row):
    return (
        row["PTS"] * 0.9 +
        row["REB"] * 1.3 +
        row["BLK"] * 1.3 +
        row["AST"] * 0.5 +
        row["STL"] * 0.5 +
        row["FG_PCT"] * 10
    )
    
def compute_score(row):
    pos = row["POSITION_MAIN"]

    if pos == "G":
        return score_guard(row)
    elif pos == "F":
        return score_forward(row)
    elif pos == "C":
        return score_center(row)
    else:
        return None

Player_scores_df["PERFORMANCE_SCORE"] = Player_scores_df.apply(compute_score, axis=1)
    
Player_scores_df["POSITION_GAME_RANK"] = (
    Player_scores_df.groupby(["GAME_ID", "POSITION_MAIN"])["PERFORMANCE_SCORE"]
    .rank(ascending=False, method="min")
)

final_df = Player_scores_df[
    [
        "GAME_ID",
        "GAME_DATE",
        "PLAYER_NAME",
        "Team",
        "POSITION_MAIN",
        "PTS", "AST", "REB", "STL", "BLK", "FG_PCT",
        "PERFORMANCE_SCORE",
        "POSITION_GAME_RANK"
    ]
]


output_file = "nba_players_game_score_with_teams.csv"
final_df.to_csv("Data/" + output_file, index=False)

print("File saved: " + output_file)
