import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from players_game_score import score_forward

Game_df= pd.read_csv('Data/nba_games.csv')
player_df= pd.read_csv('Data/nba_players_game_score_with_teams.csv')
player_df["GAME_DATE"] = pd.to_datetime(player_df["GAME_DATE"], format="%b %d, %Y")

Game_df=Game_df[(Game_df["GAME_DATE"] >= "2015-01-01") & (Game_df["SEASON"] != "2024-25")]#2015 - 2023

print(Game_df.columns)
print(player_df.columns)


metrics = ['PTS', 'REB', 'AST', 'STL', 'BLK', 'FG_PCT', 'FG3_PCT', 'FT_PCT',"player_ct", "avg_center","avg_forward","avg_guard"]

for ind, gameID in enumerate(Game_df["GAME_ID"].unique()):
    game_details=Game_df[(Game_df["GAME_ID"]==gameID)]

    if game_details["SEASON"].values[0]=="2015-16":
        #need a history to build the player details for the first season, so skipping the first season
        continue
    if gameID in (player_df["GAME_ID"].unique()):

        print(f"Processing game :{gameID}")
        team1=game_details["TEAM1"].values[0]
        team2=game_details["TEAM2"].values[0]
        print(f"Team 1: {team1}, Team 2: {team2}")
        game_date=pd.to_datetime(game_details["GAME_DATE"].values[0])

        #Assuming we know who the players are going to be 
        team1_players=player_df[(player_df["GAME_ID"]==gameID) & (player_df["Team"]==team1)]["PLAYER_NAME"].unique()
        team2_players=player_df[(player_df["GAME_ID"]==gameID) & (player_df["Team"]==team2)]["PLAYER_NAME"].unique()

        #pull players historic plays from a given game
        team1_players_details=player_df[(player_df["GAME_DATE"]<game_date) & (player_df["PLAYER_NAME"].isin(team1_players))]
        team2_players_details=player_df[(player_df["GAME_DATE"]<game_date) & (player_df["PLAYER_NAME"].isin(team2_players))]

        Game_df.loc[ind, "player_ct_TEAM1"]= team1_players_details["PLAYER_NAME"].nunique()
        Game_df.loc[ind, "player_ct_TEAM2"]=  team2_players_details["PLAYER_NAME"].nunique()
        
        Game_df.loc[ind, "avg_center_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="C"]["PERFORMANCE_SCORE"].mean()
        Game_df.loc[ind, "min_center_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="C"]["PERFORMANCE_SCORE"].min()
        Game_df.loc[ind, "max_center_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="C"]["PERFORMANCE_SCORE"].max()
        
        Game_df.loc[ind, "avg_forward_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="F"]["PERFORMANCE_SCORE"].mean()
        Game_df.loc[ind, "min_forward_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="F"]["PERFORMANCE_SCORE"].min()
        Game_df.loc[ind, "max_forward_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="F"]["PERFORMANCE_SCORE"].max()

        Game_df.loc[ind, "avg_guard_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="G"]["PERFORMANCE_SCORE"].mean()
        Game_df.loc[ind, "min_guard_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="G"]["PERFORMANCE_SCORE"].min()
        Game_df.loc[ind, "max_guard_TEAM1"]= team1_players_details[team1_players_details["POSITION_MAIN"]=="G"]["PERFORMANCE_SCORE"].max()

        Game_df.loc[ind, "avg_center_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="C"]["PERFORMANCE_SCORE"].mean()
        Game_df.loc[ind, "min_center_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="C"]["PERFORMANCE_SCORE"].min()
        Game_df.loc[ind, "max_center_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="C"]["PERFORMANCE_SCORE"].max()

        Game_df.loc[ind, "avg_forward_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="F"]["PERFORMANCE_SCORE"].mean()
        Game_df.loc[ind, "min_forward_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="F"]["PERFORMANCE_SCORE"].min()
        Game_df.loc[ind, "max_forward_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="F"]["PERFORMANCE_SCORE"].max()

        Game_df.loc[ind, "avg_guard_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="G"]["PERFORMANCE_SCORE"].mean()
        Game_df.loc[ind, "min_guard_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="G"]["PERFORMANCE_SCORE"].min()
        Game_df.loc[ind, "max_guard_TEAM2"]= team2_players_details[team2_players_details["POSITION_MAIN"]=="G"]["PERFORMANCE_SCORE"].max()

        # Historic head-to-head win rate for TEAM1 vs TEAM2 (all prior matchups, either home/away order)
        prior_h2h = Game_df[
            (pd.to_datetime(Game_df["GAME_DATE"]) < game_date) &
            (
                ((Game_df["TEAM1"] == team1) & (Game_df["TEAM2"] == team2)) |
                ((Game_df["TEAM1"] == team2) & (Game_df["TEAM2"] == team1))
            )
        ]
        if len(prior_h2h) > 0:
            team1_wins = (
                ((prior_h2h["TEAM1"] == team1) & (prior_h2h["PTS_TEAM1"] > prior_h2h["PTS_TEAM2"])) |
                ((prior_h2h["TEAM2"] == team1) & (prior_h2h["PTS_TEAM2"] > prior_h2h["PTS_TEAM1"]))
            ).sum()
            Game_df.loc[ind, "h2h_win_rate_TEAM1"] = team1_wins / len(prior_h2h)
            Game_df.loc[ind, "h2h_games_played"] = len(prior_h2h)
        else:
            Game_df.loc[ind, "h2h_win_rate_TEAM1"] = np.nan
            Game_df.loc[ind, "h2h_games_played"] = 0

        print(f"Processed game {ind+1}/{len(Game_df['GAME_ID'].unique())}")

print(Game_df.describe())
Game_df.to_csv('Data/nba_games_with_player_details.csv', index=False)