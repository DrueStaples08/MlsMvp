import re
import time
import json
import ast
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, monotonically_increasing_id, to_json, desc
from pyspark.sql.types import StringType
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, FloatType, StructField, IntegerType
from pyspark.sql.functions import round as pyspark_round
import requests
from typing import List, Dict
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import re
import matplotlib.pyplot as plt
import pandas as pd
from pandas.plotting import table



class AllCompetitions:
    """
    This class is responsible for gathering competition data from Fotmob.
    
    Attributes:
        fotmob_leagues_url (str): URL for accessing Fotmob leagues.
        last_competition_index (int): Index for the last competition.
        start_competition_index (int): Index for the starting competition.
        
    Methods:
        gather_all_competition_ids(url: str) -> Dict: 
            Fetches all competition IDs from Fotmob, processes the data, and returns a dictionary of competition names and their prefixes.

        format_competition_names(tournament_prefixes_dict: Dict) -> Dict:
            Processes and formats the competition names to a more usable format (e.g., lowercase, replacing spaces with dashes).
    
    Example:
        comp = AllCompetitions()
        competition_data = comp.gather_all_competition_ids(comp.fotmob_leagues_url)
    """
    def __init__(self):
        self.fotmob_leagues_url = "https://www.fotmob.com/leagues"
        self.last_competitiion_index = 306
        self.start_competition_index = 38

    def __repr__(self):
        pass

    def gather_all_competition_ids(self, url: str)->Dict:
        """
        Fetches and processes competition IDs from Fotmob.
        
        Args:
            url (str): URL to fetch the competition data.
            
        Returns:
            Dict: A dictionary with formatted competition names as keys and their respective competition ID as values.
        """
        # Single Matches
        html_doc = requests.get(url)
        html_doc_text = html_doc.text
        soup = BeautifulSoup(html_doc_text, "html.parser")
        scripts = soup.find("script", {"id": "__NEXT_DATA__", "type": "application/json"})
        scripts_str = scripts.string
        scripts_json = json.loads(scripts_str)
        tournament_prefixes_dict = scripts_json["props"]["pageProps"]["fallback"]["/api/translationmapping?locale=en"]["TournamentPrefixes"]
        new_comp_dict = self.format_competition_names(tournament_prefixes_dict)
        return new_comp_dict

    def format_competition_names(self, tournament_prefixes_dict: Dict)->Dict: 
        """
        Formats the competition names to be more usable.
        
        Args:
            tournament_prefixes_dict (Dict): A dictionary with competition names.
            
        Returns:
            Dict: A formatted dictionary with cleaned-up competition names.
        """
        new_comp_dict = {i[0]: i[1].lower().replace(" ", "-").replace("(w)", "qualification").replace("(women)", "women").replace(".", "") for i in tournament_prefixes_dict.items()}
        return new_comp_dict





class Competition:
    """
    This class is used to choose a specific competition, fetch its match links, and save the data.
    
    Attributes:
        competition_name (str): Name of the competition.
        year (int): Year of the competition.
        
    Methods:
        choose_competition(competition_name: str, competition_year: int) -> None:
            Loads the selected competition's match data, processes it, and saves it to a CSV.
        
        contain_all_match_links(competition_id: int, competition_name: str, competition_year: int) -> List:
            Extracts all match links for a given competition.

        extract_match_links_per_page(url: str) -> List:
            Extracts match links from a single page of Fotmob's website.
    
    Example:
        comp = Competition()
        comp.choose_competition("premier-league", 2023)
    """
    def __init__(self):
        self.competition_name = None
        self.year = None

    def __repr__(self):
        pass 

    # Now that i saved a temp pyspark df, i need to read it back in and determine the id, based on the user_input which is the competition name.
    def choose_competition(self, competition_name: str,competition_year: int)->None:
        """
        Loads competition data and saves it to a CSV file.
        
        Args:
            competition_name (str): Name of the competition.
            competition_year (int): Year of the competition.
        """
        spark_sess = SparkSession.builder.appName("SingleCompetitionSession").getOrCreate()
        df = spark_sess.read.csv(path=f"all_comps_df.csv", header=True, inferSchema=True)
        single_competition_collection = df.select(df.competition_name, df.competition_id).filter(df.competition_name == competition_name).collect()
        competition_name = single_competition_collection[0].competition_name
        competition_id =  single_competition_collection[0].competition_id
        competition = {"competition_name": competition_name, "competition_id": competition_id, "competition_year": competition_year}
        all_match_links = self.contain_all_match_links(competition["competition_id"], competition["competition_name"], competition["competition_year"])
        data = list(zip(all_match_links, [competition_name]*len(all_match_links), [competition_year]*len(all_match_links)))
        temp_df = spark_sess.createDataFrame(data, schema="match_url string, competition_name string, competition_year int")
        temp_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}.csv", header=True)
        print(f"Data for {competition['competition_name']} in {competition['competition_year']} Downloaded Successfully!")
        spark_sess.stop()

    def contain_all_match_links(self, competition_id: int, competition_name: str, competition_year: int)->List:
        """
        Retrieves all match links for a specific competition, handling pagination.
        
        Args:
            competition_id (int): The ID of the competition.
            competition_name (str): Name of the competition.
            competition_year (int): The year of the competition.
            
        Returns:
            List: A list of unique match URLs for the given competition.
        """
        all_links = []
        # ranges from 1 to max_page + x, i.e. any id can be used but will be duplicated based off the last page (max page) with the last set of games. 
        page_number = 1
        competition_id = str(competition_id)
        while True:
            time.sleep(5)
            print(f"Page Number {page_number}")
            url = f"https://www.fotmob.com/leagues/{competition_id}/matches/{competition_name}?season={competition_year}&page={page_number}"
            links_per_page = self.extract_match_links_per_page(url)
            if all_links and all_links[-1] in links_per_page:
                print(f"Duplicate Matches Found on page {page_number}")
                break
            all_links.extend(links_per_page)
            page_number += 1        
        all_links_unique = list(set(all_links))
        assert len(all_links) == len(all_links_unique)
        return all_links 

    # Around 86 match links per page
    def extract_match_links_per_page(self, url: str)-> List:
        """
        Extracts match links from a single page.
        
        Args:
            url (str): URL for the page to extract match links from.
            
        Returns:
            List: A list of extracted match URLs from the page.
        """
        # ~ 86 match links per page * 37 pages = 3182 matches
        html_doc = requests.get(url)
        html_doc_text = html_doc.text
        soup = BeautifulSoup(html_doc_text, "html.parser")
        scripts = soup.find_all("script")
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
    



class Match:
    """
    This class handles individual match data by extracting match player stats.
    
    Methods:
        choose_match(competition_name: str, competition_year: int) -> None:
            Processes match data and player statistics, saving it to a CSV file.

        extract_single_match_players_stats(url: str) -> Dict:
            Extracts player statistics for a given match URL.
    
    Example:
        match = Match()
        match.choose_match("premier-league", 2023)
    """
    def __init__(self):
        pass

    def __repr__(self):
        pass

    def choose_match(self, competition_name: str, competition_year: int):
        """
        Process match and player stats and saves to a CSV file.
        
        Args:
            competition_name (str): Name of the competition.
            competition_year (int): Year of the competition.
        """
        spark_sess = SparkSession.builder.appName("MatchSession").getOrCreate()
        temp_match_df = spark_sess.read.csv(path=f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}.csv", header=True, inferSchema=True)
        all_player_stats = []
        all_match_links = temp_match_df.select(temp_match_df.match_url).collect()
        for row_match_link in all_match_links:
            time.sleep(5)
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
                
    def extract_single_match_players_stats(self, url: str) -> Dict:
        """
        Extracts player statistics for a given match URL.
        
        Args:
            url (str): URL of the match to extract player stats for.
            
        Returns:
            Dict: A dictionary of player statistics for the match.
        """
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)
        driver.get(url)
        # Wait for JavaScript to load the content
        time.sleep(5)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        all_players_info = soup.find("div", class_=re.compile(r"css-[\w\d]+-LineupCSS"))
        player_stats = {"Player_Names": [], "Shirt_Numbers": [], "SPI_Scores": []}
        # Starting Lineup
        if all_players_info:
            player_spans = all_players_info.find_all("span", class_=re.compile(r"css-[\w\d]+-LineupPlayerText"))

            for player_span in player_spans:
                # Player name
                player_name = player_span.get("title", "").strip()

                # Go up to the player container
                player_container = player_span.find_parent("div", class_=re.compile(r"css-[\w\d]+-PlayerDiv"))
                if not player_container:
                    continue  # Skip if structure is broken

                # Shirt number (optional)
                shirt_number_span = player_container.find("span", class_=re.compile(r"css-[\w\d]+-Shirt"))
                shirt_number = shirt_number_span.text.strip() if shirt_number_span else "?"

                # Player rating
                rating_div = player_container.find("div", class_=re.compile(r"css-[\w\d]+-PlayerRatingStyled"))
                if not rating_div:
                    continue  # Skip if no rating

                rating = rating_div.text.strip()
                # print(player_name, shirt_number, rating)
                player_stats["Player_Names"].append(player_name)
                player_stats["Shirt_Numbers"].append(shirt_number)
                player_stats["SPI_Scores"].append(rating)
        # All bench entries
        for li in soup.select("li.css-gu28li-BenchItem"):
            name = li.select_one(".css-p11ffl-PlayerName")
            rating = li.select_one(".css-v1kdpc-PlayerRatingStyled span")
            shirt = li.select_one(".css-a7ajuu-Shirt")
            if name and rating and shirt:
                sub_name = name.text.strip()
                sub_rating = rating.text.strip()
                sub_shirt_number = shirt.text.strip() if shirt else None
                # print(sub_name, sub_shirt_number, sub_rating)
                player_stats["Player_Names"].append(sub_name)
                player_stats["Shirt_Numbers"].append(sub_shirt_number)
                player_stats["SPI_Scores"].append(sub_rating)

        driver.quit()
        # print([len(i) for h, i in player_stats.items()])
        return player_stats
    
    
    

class Player:
    def __init__(self):
        pass

    def __repr__(self):
        pass

    def choose_player_stats(self, competition_name: str, competition_year: int):
        """
        Selects and processes player statistics for a specific competition and year. 
        Extracts relevant player names, shirt numbers, and SPI scores, 
        and saves them as a CSV file.
        
        Args:
            competition_name (str): The name of the competition.
            competition_year (int): The year of the competition.
        """
        spark_sess = SparkSession.builder.appName("PlayerStatsSession").getOrCreate()
        df = spark_sess.read.csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_match_stats.csv", header=True, inferSchema=True)
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
        """
        Analyzes player statistics by computing the number of matches played 
        and the average SPI score for each player. 
        The results are saved in a CSV file.
        
        Args:
            competition_name (str): The name of the competition.
            competition_year (int): The year of the competition.
        """
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

    def compute_mvp(self, competition_name: str, competition_year: int, scalar: int=4):
        """
        Computes the MVP of a competition based on player performance, using a 
        scaling factor to adjust SPI and match statistics. 
        The results are saved in a CSV file.
        
        Args:
            competition_name (str): The name of the competition.
            competition_year (int): The year of the competition.
            scalar (int, optional): The scaling factor for SPI score. Defaults to 4.
        """
        assert scalar <= 10
        spark_sess = SparkSession.builder.appName("MVPSession").getOrCreate()
        all_player_stats_df = spark_sess.read.csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_player_stats_analysis.csv", header=True, inferSchema=True)

        def find_max_matches_played():
            # Games will only be counted for players that receive an SPI rating for that game. 
            # I.e if a player gets injured within 10 minutes of playtime and needs to subbed out, they most likely won't receive a score. 
            # Therefore this game wouldn't be accounted for (unless the player has earned a rating) that individual. 
            max_games_played = all_player_stats_df.agg({"count": "max"})
            max_games_played_var = max_games_played.collect()[0]["max(count)"]
            # max_games_played.show()
            return max_games_played_var
        
        def find_max_spi():
            max_avg_spi = all_player_stats_df.filter(col("count") >= min_required_matches).agg({"avg(spi_score)": "max"})
            # max_avg_spi_var = round(max_avg_spi.collect()[0]["max(avg(spi_score))"], 1)
            max_avg_spi_var = max_avg_spi.collect()[0]["max(avg(spi_score))"]
            # max_avg_spi.show()
            return max_avg_spi_var
        
        max_games_played_var = find_max_matches_played()
        # min_required_matches = max_games_played_var // 2
        min_required_matches = max_games_played_var / 2
        max_avg_spi_var = find_max_spi()
        print("Max Games Played: ", max_games_played_var, "\n")
        print("Max Average SPI: ", max_avg_spi_var)
        max_avg_spi_scaled_var = float(max_avg_spi_var)**scalar
        df_with_scaled_spi = all_player_stats_df.withColumn("spi_scaled", pyspark_round((col("avg(spi_score)")**scalar) / (max_avg_spi_scaled_var),5))
        # df_with_scaled_spi.show()
        df_with_scaled_matches = all_player_stats_df.withColumn("matches_scaled", col("count") / max_games_played_var)
        # spi_scaled = x^n / max_games_played, where n will be included in other function that will allow me to scale up by n times (exponentionally) then scaled back down by the max scaled-up spi.
        mvp_df = df_with_scaled_matches.join(df_with_scaled_spi, on=["player_name", "player_number", "count", "avg(spi_score)"]).filter(col("count") >= min_required_matches).sort(desc("spi_scaled"))
        # mvp_distance_df = mvp_df.withColumn("euclidean_distance", sqrt(((1-col("spi_scaled"))**2 + (1-col("matches_scaled"))**2))).sort("euclidean_distance")
        mvp_scaled_df = mvp_df.withColumn("mvp_scaled", col("spi_scaled") * col("matches_scaled")).sort(col("mvp_scaled").desc())
        self.save_dataframe_as_image(mvp_scaled_df.toPandas(), competition_name, competition_year)
        print(mvp_scaled_df.show())
        mvp_scaled_df.write.mode("overwrite").csv(f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_mvp_results.csv", header=True)
        print(f"MVP Results have been saved successfully with a scalar of {scalar}!")
        spark_sess.stop()


    def save_dataframe_as_image(self, df: pd.DataFrame, competition_name: str, competition_year: int):
        """
        Saves a Pandas DataFrame as an image.
        
        Args:
            df (pd.DataFrame): The DataFrame to save as an image.
            competition_name (str): The competition's name.
            competition_year (int): The competition's year.
        """
        # Generate a plot to place the dataframe table
        fig, ax = plt.subplots(figsize=(8, 4))  # Adjust the size as needed
        ax.axis("off")  # Hides the axes
        tbl = table(ax, df.head(20), loc='center', colWidths=[0.5]*len(df.columns))  # Only show the first 20 rows
        tbl.auto_set_font_size(False)  # Disable automatic font size
        tbl.set_fontsize(10)  # Set font size for better readability
        tbl.scale(1.2, 1.2)  # Scale the table if necessary

        # Save the figure as an image
        image_path = f"{competition_name}_{competition_year}_dir/{competition_name}_{competition_year}_mvp_results_image.png"
        plt.savefig(image_path, bbox_inches='tight', dpi=300)  # Save as a high-res PNG image
        print(f"Image of the MVP results saved as {image_path}")
        plt.close()  # Close the figure to free up memory



if __name__ == "__main__":
    # MLS 2024 = League + Playoffs
    # Add Euclidean Distance Column
    # Allow exponential scaling to SPI Scores
    # Turn into an API

    # all_comps = AllCompetitions()
    # print(all_comps.gather_all_competition_ids("https://www.fotmob.com/")) # run



    # comps = Competition()
    # print(comps.choose_competition("mls", 2024)) # run


    # match = Match()
    # print(match.extract_single_match_players_stats("https://www.fotmob.com/matches/new-york-red-bulls-vs-new-york-city-fc/1y6hxcay#4663699")) # shows all player stats
    # print(match.choose_match("mls", 2024)) # run


    # player = Player()
    # print(player.choose_player_stats("mls", 2024)) # run both 
    # print(player.competition_analysis("mls", 2024)) # run both


    mvp = MVP()
    print(mvp.compute_mvp("mls", 2024, 4)) # run this



