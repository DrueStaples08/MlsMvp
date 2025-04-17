# MlsMvp
Analysis to determine the most valuable player and a API to run it all 

# Soccer MVP Analyzer

This project calculates the **Most Valuable Player (MVP)** for soccer leagues like Major League Soccer (MLS) using player performance statistics.

You can think of it as a custom-built analytics engine that scrapes, cleans, processes, and visualizes soccer player data to answer the question:  
**"Who’s really been the MVP this season?"**

---

## Getting Started

### 1. Clone the project

```
git clone https://github.com/your-username/soccer-mvp-analyzer.git
cd soccer-mvp-analyzer
```


Make sure you're using Python 3.12 and then install the required packages:

```pip install -r requirements.txt```

Or, if you're using pyenv:
```
pyenv install 3.12.4
pyenv virtualenv 3.12.4 soccer-mvp-env
pyenv activate soccer-mvp-env
pip install -r requirements.txt
```
If you hit a ModuleNotFoundError: No module named 'distutils', run:

```pip install setuptools```

3. Run the MVP calculator
In the terminal, run:

python path/to/webscraper_v2.py
It will compute the MVPs and save the results both as a CSV file and as an image.

What This Project Does
Scrapes and compiles soccer player performance data

Uses Spark for large-scale data processing

Calculates MVP based on a weighted combination of stats like SPI scores, goals, assists, and match appearances

Ranks players and outputs a leaderboard

Saves the results as both a .csv and a shareable .png image

Outputs
Once run, the project will generate:

mls_2024_mvp_results.csv: Tabular MVP data

mls_2024_mvp_results.png: Visual snapshot of the top players

Tech Stack
Python 3.12

Apache Spark (PySpark): Scalable data processing

Pandas: Data manipulation

Matplotlib: Save tables as images

Setuptools: Needed for compatibility with PySpark on Python 3.12+




As of 12/28/24, there are 306 event options to choose from. 

e.g.

*/https://www.fotmob.com/leagues/306/overview/world-cup-u17/*


## How a Player Receives an SPI Score:

Only players who receive an SPI score are considered to have officially played in a match. 
While the exact time required for a score isn't fully confirmed, starting players typically 
need to play just a few minutes to earn an SPI for that match. On the other hand, substitutes 
must play for approximately 10 minutes to qualify for a score.

Substitute players who play less than the required time or those who are injured shortly after 
entering the game will not receive an SPI score. Initially, I considered assigning a default 
starting score (likely around 6.0 or 7.0, which is what I think a player starts off anyways) to any competitor who participated in the match but didn’t earn 
a score. However, this approach raised concerns: for instance, a player who scores a goal and 
one who concedes an own-goal would both receive the same starting score, which seems inappropriate.

Unless the system is entirely condition-based (e.g., a player must achieve something impactful 
like scoring, assisting, or completing a pass to initiate their score), the SPI scoring could 
instead follow a dual approach. This would mean a player earns a score either by making an 
impactful play or by playing a minimum amount of time.

To maintain consistency, this MVP program will only consider a match as "played" if a player has been assigned an SPI rating. 





Resource: (Install Pyspark) - */https://medium.com/@jpurrutia95/install-and-set-up-pyspark-in-5-minutes-m1-mac-eb415fe623f3/*
