import re
import time
import json
import ast
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import lit, udf, col, monotonically_increasing_id, to_json, desc, sqrt
from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StringType, ArrayType, FloatType, StructField, IntegerType
from pyspark.sql.functions import round as pyspark_round
import requests
from typing import List, Dict

from bs4 import BeautifulSoup



"""
Classes:

    AllCompetitions
        Function X: id_compname_extractor()
        Purpose: Determines the competition name for each id (306 in total as of 12/27/2024)
        Input: None (Loops through ids to match the name of each competition to it)

        Function Y: save_id_compname()
        Purpose: Saves the results above locally (pyspark dataframe or json)
        Columns: competition_url, id, competition_name

        

    Contains the functionality for determining all the games played for any single competition. 
    Competition:
        Variables:
            competition_name: str
            year: int
            
        Function A: name_to_id_converter()
        Purpose: Takes a competition name (and year) as input and extracts the competition id via the output from id. Then saves the results to a dictionary (or pyspark dataframe or dict)
        Input: str, int: name of league or tournament, year of competition
        Output: dict: {name: id} (or pyspark data structure)

        Function B: extract_match_links()
        Purpose: Takes the competition name and year (or just a match link) as input and extracts all the match links. The results are saved to a list or pyspark data structure.
        Input: str, int: name of competition, year of competition
        Output: list [match_link_1, match_link_2, ...] (or pyspark data structure)

        XXXXX Use Function Y instead XXXXX Function C: save_locally()
        Purpose: Save the ids from function A 
        Input: None
        Output: None


        Function D: 
        Purpose: Save the match links for each id via function B.
        Input: None
        Output: None

        

    # Contains all the functionality for extracting player information for all players who receive a SPI score for every match played in a given competition. 
    Match:
        Function E: extract_single_match_players_stats()
        Purpose: Takes a game link as input and scrapes certain pieces of player information.
        Input: str: game link
        Output: json {game_link: 
                        {player_name_1: spi_score_1, player_name_2: spi_score_2, ...}, 
                        {player_name_1: spi_score_1, player_name_2: spi_score_2, ...}
                    } 
                (or pyspark data structure) 
        
        XXXXX Use Function Y instead XXXXX Function: F
        Function: save_match_players_stats()
        Purpose: Saves a json type (or pyspark ds) containing all the player information from each game in a given competition.
        Input: None
        Output: None
        Columns: match_url, (id, competition_name), year, player_name, player_number, spi_score


        
    Player:
        Funciton: Counts how many games each player where has recieved a SPI score
        Function: Computes all player's average SPI Scores
        Function: Determines the max number of games played
        Function: Determines the max average SPI received

        
        
    MVP:
        Variables:
            spi_scaler: int, scales the SPI feature exponentionally x times 
    
        Function: Scales SPI feature by user
        Function: Computes the distance between both features (number of performance indicated games and their average SPI) 
        Function: Save results (pyspark dataframe)
        Function: View Results
"""






"""
# I'm not able to find the redirected links so skip for now 

When i type an id from (competitions range from the following ids: 38-306) like https://www.fotmob.com/leagues/38 or https://www.fotmob.com/leagues/38/matches ,
it takes me to a new page. So, I should be able to see the league name in it 
like "https://www.fotmob.com/leagues/38/matches/bundesliga" but I don't. 
"""
class AllCompetitions:
    def __init__(self):
        self.fotmob_leagues_url = "https://www.fotmob.com/leagues"
        self.last_competitiion_index = 306
        self.start_competition_index = 38

    def __repr__(self):
        pass

    def all_id_compnames_extractor(self):
        for competition_id in range(self.start_competition_index, self.last_competitiion_index+1):
            url = self.single_id_compname_extractor(competition_id)
            print(url)


    def single_id_compname_extractor(self, id):
        competition_url = self.fotmob_leagues_url + f"/{id}/matches/"
        # competition_url = self.fotmob_leagues_url + f"/{id}"
        # competition_url = self.fotmob_leagues_url + f"/{id}/matches"
        response = requests.get(competition_url, allow_redirects=True)
        response.raise_for_status()
        # New link includes the competition name for each id
        redirected_link = response.url
        print(competition_url, redirected_link, response.headers, response.status_code, response.history)
        if response.history:
            print(response.history, competition_url, redirected_link, response.cookies, response.links)
        

        # return redirected_link

        html_doc = response.text
        soup = BeautifulSoup(html_doc, "html.parser")
        # print(soup.find_all("a"))
        # print(soup.text)
    # Columns: competition_name, year, match_url
    def save_all_competition_results(self)->None:
        spark_sess = SparkSession.builder.appName("AllCompetitionsSession").getOrCreate()
        temp_df = spark_sess.createDataFrame(   
                                                [
                                                    ("https://www.fotmob.com/leagues/130/matches/mls", 130, "mls"),
                                                ],
                                                schema = "competition_url string, competition_id int, competition_name string"
                                            )

        # print(temp_df2.show())
        # print("\n")
        # print(temp_df2.printSchema())

        temp_df.write.mode("overwrite").csv("all_comps_df.csv", header=True)
        spark_sess.stop()
        print("All Competition Results Saved Successfully!")


    # def workflow_extract_save_all_competition_information(self):
    #     self.all_id_compnames_extractor()
    #     self.save_results()



class Competition:
    def __init__(self):
        self.competition_name = None
        self.year = None

    def __repr__(self):
        pass 

    # Now that i saved a temp pyspark df, i need to read it back in and determine the id, based on the user_input which is the competition name.
    def choose_competition(self, competition_name: str,competition_year: int)->None:
        spark_sess = SparkSession.builder.appName("SingleCompetitionSession").getOrCreate()
        df = spark_sess.read.csv(path=f"all_comps_df.csv", header=True, inferSchema=True)
        # print(df.show())
        # print("\n")
        # print(df.printSchema())
        # competition_collection = df.select(df.competition_name, df.competition_id).collect()
        single_competition_collection = df.select(df.competition_name, df.competition_id).filter(df.competition_name == competition_name).collect()


        competition_name = single_competition_collection[0].competition_name
        competition_id =  single_competition_collection[0].competition_id
        # print(competition_name, competition_id)
        competition = {"competition_name": competition_name, "competition_id": competition_id, "competition_year": competition_year}
        all_match_links = self.contain_all_match_links(competition["competition_id"], competition["competition_name"], competition["competition_year"])
        # self.save_single_competition_results(all_match_links, competition["competition_name"], competition["competition_year"])
        data = list(zip(all_match_links, [competition_name]*len(all_match_links), [competition_year]*len(all_match_links)))
        temp_df = spark_sess.createDataFrame(data, schema="match_url string, competition_name string, competition_year int")
        temp_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}.csv", header=True)
        print(f"Data for {competition['competition_name']} in {competition['competition_year']} Downloaded Successfully!")
        spark_sess.stop()



    def contain_all_match_links(self, competition_id: int, competition_name: str, competition_year: int)->List:
        all_links = []
        # ranges from 1 to max_page + x, i.e. any id can be used but will be duplicated based off the last page (max page) with the last set of games. 
        # I'm currently running the code starting on page 37 which is the last page for MLS 2024 before the info is duplicated.
        # Therefore once eveything is completed. I need to change the page number to 1 so I can extract the player info from the entire season.
        page_number = 1
        competition_id = str(competition_id)
        duplicate = False
        while True:
            time.sleep(5)
            print(f"Page Number {page_number}")
            url = f"https://www.fotmob.com/leagues/{competition_id}/matches/{competition_name}?season={competition_year}&page={page_number}"

            links_per_page = self.extract_match_links_per_page(url)
            if all_links and all_links[-1] in links_per_page:
                print(f"Duplicate Matches Found on page {page_number}")
                duplicate = True
                break
            all_links.extend(links_per_page)
            # print(page_number, duplicate)
            page_number += 1        
        all_links_unique = list(set(all_links))
        # print(all_links, page_number+1)
        # print(page_number)
        assert len(all_links) == len(all_links_unique)
        return all_links 

    # Around 86 match links per page
    def extract_match_links_per_page(self, url: str)-> List:
        # ~ 86 match links per page * 37 pages = 3182 matches
        html_doc = requests.get(url)
        html_doc_text = html_doc.text
        soup = BeautifulSoup(html_doc_text, "html.parser")
        scripts = soup.find_all("script")
        # print("Scripts Type: ", type(scripts)) # class 'bs4.element.ResultSet'
        # Base URL for building complete match URLs
        base_url = "https://www.fotmob.com"
        # Regular expression pattern to find match links
        pattern = r'"/matches/[^"]+"'
        # Iterate over each script to find the links
        match_links = []
        for script in scripts:
            time.sleep(5)
            try:
                # Extract the string content from the script
                json_data = script.string
                if json_data:
                    # Use regex to find all match links
                    links = re.findall(pattern, json_data)
                    match_links.extend([(base_url + link).replace("\"", "") for link in links])
            except Exception as e:
                print(f"Error processing script: {e}")

        match_links = list(set(match_links))
        return match_links
    
    # # Columns: competition_name, year, match_url
    # def save_single_competition_results(self, all_match_links: list, competition_name: str, competition_year: str)->None:
    #     spark_sess = SparkSession.builder.appName("SingleCompetitionSession").getOrCreate()
    #     data = list(zip(all_match_links, [competition_name]*len(all_match_links), [competition_year]*len(all_match_links)))
    #     temp_df = spark_sess.createDataFrame(data, schema="match_url string, competition_name string, competition_year int")
    #     temp_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}.csv", header=True)
    #     spark_sess.stop()
 

       



class Match:
    def __init__(self):
        pass

    def __repr__(self):
        pass

    def choose_match(self, competition_name: str, competition_year: int):
        spark_sess = SparkSession.builder.appName("MatchSession").getOrCreate()
        temp_match_df = spark_sess.read.csv(path=f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}.csv", header=True, inferSchema=True)
        all_player_stats = []
        all_match_links = temp_match_df.select(temp_match_df.match_url).collect()
        # print(all_match_links)
        for row_match_link in all_match_links:
            # time.sleep(5)
            match_link = row_match_link.match_url
            print(match_link)

            single_match_players_stats = Row(self.extract_single_match_players_stats(match_link))
            all_player_stats.append(single_match_players_stats)

        temp_player_stats_df = spark_sess.createDataFrame(all_player_stats, schema="player_stats map<string,string>")
        player_stats_df = temp_player_stats_df.withColumn("index", monotonically_increasing_id())
        match_df = temp_match_df.withColumn("index", monotonically_increasing_id())

        url_stats_df = match_df.join(player_stats_df, on="index", how="inner").drop("index")
        url_stats_df = url_stats_df.withColumn("player_stats", to_json(col("player_stats")))
        # print(url_stats_df.show())
        url_stats_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_match_stats.csv", header=True)
        print("Player Stats have been saved successfully!")
        spark_sess.stop()




    def extract_single_match_players_stats(self, url: str)->Dict:
        # Single Matches
        # url = "https://www.fotmob.com/matches/new-york-red-bulls-vs-new-york-city-fc/1y6hxcay#4663699" # Single matches WORKS **********
        html_doc = requests.get(url)
        html_doc_text = html_doc.text
        soup = BeautifulSoup(html_doc_text, "html.parser")
        # Contains all individual stats for each lineup player
        pattern = re.compile(r"css-\w+-LineupPlayerCSS \w+")
        # # Contains the HTML for both starting players and bench players information
        playtime_roster = soup.find_all(class_=pattern)
        # Contains players' SPI ratings
        # spi_pattern = re.compile(r"css-\w+-TopRight \w+")
        # # SPI v2
        spi_pattern = re.compile(r"css-\w+-PlayerRatingStyled \w+")
        # Players' first and last name
        full_name_pattern = re.compile(r"css-\w+-LineupPlayerText \w+")
        # Players' shirt number
        shirt_number_pattern = re.compile(r"css-\w+-Shirt \w+")

        player_names = []
        spi_scores = []
        shirt_numbers = []

        for player in playtime_roster:
            spi = player.find("div", class_=spi_pattern)
            full_name = player.find("span", full_name_pattern)
            shirt_number = player.find("span", shirt_number_pattern)
            # time.sleep(5)
            if spi:
                # print(full_name["title"], spi.text, shirt_number.text, "\n")
                player_names.append(full_name["title"])
                spi_scores.append(spi.text)
                shirt_numbers.append(shirt_number.text)

        return {"Player_Names": player_names, "SPI_Scores": spi_scores, "Shirt_Numbers": shirt_numbers}
                

       

    # def save_all_matches_players_stats(self, competition_name: str, competition_year: int):
    #    spark_sess = SparkSession.builder.appName("AllMatchesPlayersStatsSession").getOrCreate()
    #    spark_sess.overwrite("True").csv(f"{competition_name}_{competition_year}_players_stats.csv", header=True)


class Player:
    def __init__(self):
        pass

    def __repr__(self):
        pass

    def choose_player_stats(self, competition_name: str, competition_year: int):
        spark_sess = SparkSession.builder.appName("PlayerStatsSession").getOrCreate()
        df = spark_sess.read.csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_match_stats.csv", header=True, inferSchema=True)
        # df = spark_sess.read.csv(f"/Users/druestaples/projects/soccer_mvp_project/MlsMvp/{competition_name}_{competition_year}_player_stats.csv", header=True, inferSchema=True)


        match_stats = df.select(df.player_stats)
        match_rows = match_stats.collect()
        all_player_names = []
        all_shirt_numbers = []
        all_spi_scores = []


        for row in match_rows:
            # print(i, "\n") # Row()
            player_info = json.loads(row.player_stats)
            # print(ast.literal_eval(player_info["Player_Names"]))

            raw_data = player_info["Player_Names"]
            corrected_data = "[" + ", ".join(f'"{name.strip()}"' for name in raw_data.strip("[]").split(", ") if name) + "]"
            player_names = ast.literal_eval(corrected_data)
            # print(player_names)
            all_player_names.extend(player_names)


            match_shirt_numbers = json.loads(player_info["Shirt_Numbers"])
            all_shirt_numbers.extend(match_shirt_numbers) 

            match_spi_scores = json.loads(player_info["SPI_Scores"])
            all_spi_scores.extend(match_spi_scores)

        # print(all_player_names)
        # print(all_spi_scores)
        # print(len(all_shirt_numbers),  len(all_spi_scores), len(all_player_names))


        data = list(zip(all_player_names, all_shirt_numbers, all_spi_scores))

        new_schema = StructType()
        new_schema.add(StructField(name="player_name", dataType=StringType()))
        new_schema.add(StructField(name="player_number", dataType=IntegerType()))
        new_schema.add(StructField(name="spi_score", dataType=FloatType()))
        new_df = spark_sess.createDataFrame(data, schema=new_schema)
        print(new_df.show())
        new_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_player_stats.csv", header=True)
        print("All players' basic stats have been saved successfully!")
        spark_sess.stop()
  


    def competition_analysis(self, competition_name: str, competition_year: int):
        spark = SparkSession.builder.appName("CompetitionAnalysis").getOrCreate()
        df = spark.read.csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_player_stats.csv", header=True, inferSchema=True)
        def count_matches_played():
            matches_played_per_player = df.groupBy(["player_name", "player_number"]).count()
            # matches_played_per_player.show()
            return matches_played_per_player

        def compute_average_spi():
            avg_spi_per_player = df.groupBy(["player_name", "player_number"]).agg({"spi_score": "avg"})
            # avg_spi_per_player.show()
            return avg_spi_per_player
        
        matches_played_per_player = count_matches_played()
        avg_spi_per_player = compute_average_spi()


        all_player_stats_df = matches_played_per_player.join(avg_spi_per_player, how="inner", on=["player_name", "player_number"]).sort(desc("avg(spi_score)"))
        print(all_player_stats_df.show)

        all_player_stats_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_player_stats_analysis.csv", header=True)
        print("All players' intermediate stats have been saved successfully!")
        spark.stop()




class MVP:
    def __init__(self):
        pass

    def __repr__(self):
        pass


    def compute_mvp(self, competition_name: str, competition_year: int, scalar: int):
        assert scalar <= 10
        spark_sess = SparkSession.builder.appName("MVPSession").getOrCreate()
        all_player_stats_df = spark_sess.read.csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_player_stats_analysis.csv", header=True, inferSchema=True)

        def find_max_matches_played():
            max_games_played = all_player_stats_df.agg({"count": "max"})
            max_games_played_var = max_games_played.collect()[0]["max(count)"]
            # max_games_played.show()
            return max_games_played_var
        
        def find_max_spi():
            max_avg_spi = all_player_stats_df.filter(col("count") >= min_required_matches).agg({"avg(spi_score)": "max"})
            max_avg_spi_var = round(max_avg_spi.collect()[0]["max(avg(spi_score))"], 1)
            # max_avg_spi.show()
            return max_avg_spi_var
        
        max_games_played_var = find_max_matches_played()
        min_required_matches = max_games_played_var // 2
        max_avg_spi_var = find_max_spi()
        print("Max Games Played: ", max_games_played_var, "\n")
        print("Max Average SPI: ", max_avg_spi_var)



        df_with_scaled_spi = all_player_stats_df.withColumn("spi_scaled", pyspark_round((col("avg(spi_score)")**scalar) / (float(max_avg_spi_var)**scalar),3))
        # df_with_scaled_spi.show()
        df_with_scaled_matches = all_player_stats_df.withColumn("matches_scaled", col("count") / max_games_played_var)
        
        # spi_scaled = x^n / max_games_played, where n will be included in other function that will allow me to scale up by n times (exponentionally) then scaled back down by the max scaled-up spi.
        
        mvp_df = df_with_scaled_matches.join(df_with_scaled_spi, on=["player_name", "player_number", "count", "avg(spi_score)"]).filter(col("count") >= min_required_matches).sort(desc("spi_scaled"))
        # mvp_distance_df = mvp_df.withColumn("euclidean_distance", sqrt(((1-col("spi_scaled"))**2 + (1-col("matches_scaled"))**2))).sort(desc("avg(spi_score)"))
        mvp_distance_df = mvp_df.withColumn("euclidean_distance", sqrt(((1-col("spi_scaled"))**2 + (1-col("matches_scaled"))**2))).sort("euclidean_distance")

        print(mvp_distance_df.show())
        mvp_distance_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_mvp_results.csv", header=True)
        print(f"MVP Results have been saved successfully with a scalar of {scalar}!")
        spark_sess.stop()






if __name__ == "__main__":
    # MLS 2024 = League + Playoffs
    # Add Euclidean Distance Column
    # Allow exponential scaling to SPI Scores
    # Turn into an API

    # all_comps = AllCompetitions()
    # redirected_url = all_comps.single_id_compname_extractor(130)
    # print(redirected_url)
    # print(all_comps.get_redirected_url(306))
    # print(all_comps.save_results())



    comps = Competition()
    print(comps.choose_competition("mls", 2024))


    match = Match()
    print(match.choose_match("mls", 2024))


    player = Player()
    print(player.choose_player_stats("mls", 2024))
    print(player.competition_analysis("mls", 2024))


    mvp = MVP()
    print(mvp.compute_mvp("mls", 2024, 10))



