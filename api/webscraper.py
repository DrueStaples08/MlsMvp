import re
import time
import pyspark
import requests
from bs4 import BeautifulSoup

# Access URL
# Q. Does each league/tournament have a unique numerical identifier e.g. below shows MLS as ID 130
# html_doc = requests.get(url="https://www.fotmob.com/leagues/130/matches/mls?group=by-date&page=1")
# print(html_doc)



# Single Matches
# There are 306 total events as of 11/28/24
url_all_matches = "https://www.fotmob.com/leagues/130/matches/mls?season=2024" # Matches per week
# https://www.fotmob.com/leagues/130/matches/mls?season=2024&page=37
url = "https://www.fotmob.com/130/matches/new-york-red-bulls-vs-new-york-city-fc/1y6hxcay#4663699" # Single matches WORKS **********
# url = "https://www.fotmob.com/matches/la-galaxy-vs-colorado-rapids/1ujvja#4662942:tab=lineup"
# url = "https://www.fotmob.com/matches/la-galaxy-vs-colorado-rapids/1ujvja#4662942"
# url = "https://www.fotmob.com/130/matches/new-york-red-bulls-vs-new-york-city-fc/1y6hxcay#4663699:tab=lineup" # Players' performances for a single match


html_doc = requests.get(url)
# Extract HTML
html_doc_text = html_doc.text


soup = BeautifulSoup(html_doc_text, "html.parser")

"""
Only players who receive an SPI score will count as having played that match. 
I'm not forsure certain of the exact time, but a starting player only needs to play a few min in a match for them to receive an SPI for that said match. 
However, a substitute needs to play for around 10 min first before they get one. I know, weird, right?
Therefore a substitute player who plays less than the minimum time or a player that gets injured quickly into the game will not receive a score. 
Originally I was going to give the starting score (I'm pretty sure the scores start at 7.0) to any individual if they don't receive one, but have played in the match. 
However, a player who scores and one who lets an own-goal are treated the same and would receive the same scores, which doesn't make sense.
Therefore, this MVP Program will ONLY count a matched as played if a player has a SPI rating. 

"""

"""
Note: A player needs to play at least 10 min to recieve a rating.
Therefore, a player will be removed if they have less than 10 minutes of playing time
Still need to do:
- For starting players, 
    - Determine if a player has a None value or greater than 9 min of playing time. Remove them from output otherwise.
- For substitutes, 
    - Each match type will first be classified as "Full time" (90 minutes), "After extra time" (120 minutes), or "Canceled" (treated as 0 minutes - match is ignored). 
    - Afterwards, determine if a player has at least 10 minutes of playing time by checking the the time they entered the game vs the match type (above). 
    - Also every neighboring pair of SPI's are duplicated e.g. [7.8,7.8,9.1,9.1,8.5,8.5,7.8,7.8] so remove the one of the elements from each neigboring pair
"""








# url = "https://www.fotmob.com/130/matches/new-york-red-bulls-vs-new-york-city-fc/1y6hxcay#4663699" # Single matches WORKS **********

html_doc = requests.get(url)
html_doc_text = html_doc.text


soup = BeautifulSoup(html_doc_text, "html.parser")

# Contains all individual stats for each lineup player
# <div class="css-jopvbj-LineupPlayerCSS e174d5a70"
pattern = re.compile(r"css-\w+-LineupPlayerCSS \w+")


# # Contains the HTML for both starting players and bench players information
# pattern_all = re.compile(r"css-\w+-LineupCSS \w+")
playtime_roster = soup.find_all(class_=pattern)


# Contains players' SPI ratings
spi_pattern = re.compile(r"css-\w+-TopRight \w+")
# SPI v2
spi_pattern_v2 = re.compile(r"css-\w+-PlayerRatingStyled \w+")
# Players' first and last name
full_name_pattern = re.compile(r"css-\w+-LineupPlayerText \w+")
# Players' shirt number
shirt_number_pattern = re.compile(r"css-\w+-Shirt \w+")





for player in playtime_roster:
    spi = player.find("div", class_=spi_pattern_v2)
    full_name = player.find("span", full_name_pattern)
    shirt_number = player.find("span", shirt_number_pattern)
    if spi:
        print(full_name["title"], spi.text, shirt_number.text, "\n")



"""
I am currently able to scrape full names, player namubers and their spi rating for a specific link. 

Next Steps:

- Structure with OOP because everything is already become cluttered 
- Expand current functionality to account for all games in a specified league (or tournament) and year
- Save results to a pyspark dataframe (beta version will not include cloud services)



"""

