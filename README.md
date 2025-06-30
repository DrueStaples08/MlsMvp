# ⚽ FootballMVPAPI

Analysis to determine the most valuable player and an API to run it all

---

## 🔍 Soccer MVP Analyzer

This project calculates the **Most Valuable Player (MVP)** for professional football leagues and tournaments using player performance statistics.

You can think of it as a custom-built analytics engine that scrapes, cleans, processes, and visualizes soccer player data to answer the question:
**"Who’s really been the MVP this season?"**

---

## 🚀 Getting Started

### 1. Clone the project

**Repo Access Methods:**

* **Workflow Type A - Clone (Engineer):**

  ```bash
  git clone https://github.com/YOUR_USERNAME/FootballMVPAPI.git
  ```

* **Workflow Type B - Fork (Contributor):**

  ```bash
  git fork https://github.com/YOUR_USERNAME/FootballMVPAPI.git
  ```

* **Workflow Type C - Pip Install (Developer):**

  ```bash
  pip install FootballMVPAPI
  pip install -r requirements.txt
  python mvp_comp_dir/mvp_comp_compute.py
  ```

---

### 2. Environment Setup

**Python 3.12 Required**

Install dependencies:

```bash
pip install -r requirements.txt
```

**Using pyenv (Recommended):**

```bash
pyenv install 3.12.4
pyenv virtualenv 3.12.4 soccer-mvp-env
pyenv activate soccer-mvp-env
pip install -r requirements.txt
```

**Troubleshooting:**
If you hit a `ModuleNotFoundError: No module named 'distutils'`, run:

```bash
pip install setuptools
```

---

### 3. Input Parameters (Function Call)

```python
def compute_mvp(
    competition_name: str,              # Required
    competition_year: str,             # Required
    scalar: int = 4,                   # Optional
    open_close_league: str = "",       # Optional
    overide: bool = True,              # Optional
    title: str = "",                   # Optional
    percentile_threshold: float = .98, # Optional
    manual_competition_id: str = ""    # Optional
) -> str:
```

---

## ⚙️ Run Options

### Option A: Docker Container

```bash
git clone https://github.com/YOUR_USERNAME/FootballMVPAPI.git
cd FootballMVPAPI
docker build -t mvp-api:latest .
docker run -p 5000:5000 mvp-api:latest
```

### Option B: Flask API

```bash 
git clone https://github.com/YOUR_USERNAME/FootballMVPAPI.git
cd FootballMVPAPI
- Inside terminal 
    1. View comps inside url below:
        flask run 
        # open up in web browser
        - http://127.0.0.1:5000/


    2. Add comp to list (open up second terminal)
    curl -X POST http://127.0.0.1:5000/add_competition \
  -H "Content-Type: application/json" \
  -d '{"competition_name": "open-cup"}'
    
    3. Run MVP Analysi on Competition (also run in second terminal)
    curl -X POST http://127.0.0.1:5000/run_analysis \
  -H "Content-Type: application/json" \
  -d '{
    "competition_name": "open-cup",
    "competition_year": "2022",
    "scalar": 4,
    "overide": True,
    "title": "United States Open Cup: 2022"}'

    4. If you have already ran the command above at least once, then you will have those player stats already saved. Therefore as the season is still ongoing, you can save yourself some time by turning off the overide. By skipping the matches with previously saved player stats, you can just scrape the player stats from match links that havenn't been updated by your last run. 
    # Note: Most competitons should be ran with a scalar of 4 as proven in my analysis
    curl -X POST http://127.0.0.1:5000/run_analysis \
  -H "Content-Type: application/json" \
  -d '{
    "competition_name": "open-cup",
    "competition_year": "2022",
    "scalar": 4,
    "overide": true,
    "title": "United States Open Cup: 2022"}'


    - Another Example (Note: Furthermore, for competitions with few total games like FIFA World Club Cup from 2020-2023 and the Intercontinental Cup in 2024 should be scaled to 14)
    curl -X POST http://127.0.0.1:5000/run_analysis \
  -H "Content-Type: application/json" \
  -d '{
    "competition_name": "fifa-club-world-cup",
    "competition_year": "2022",
    "scalar": 14,
    "overide": true,
    "title": "FIFA Club World Cup 2022",
    "manual_competition_id": "78"}'


    - Avoid running on port 8000 with Gunicorn as their servers expect fast responses
```

Open another terminal and curl into the server to compute MVP results.

### Option C: Local Execution with Python

```bash
git clone https://github.com/YOUR_USERNAME/FootballMVPAPI.git
cd FootballMVPAPI
python mvp_comp_dir/mvp_comp_compute.py
```

---

## 🧪 Examples

### View Competitions

```python
all_comps = AllCompetitions()
all_comp_info = all_comps.gather_all_competition_ids("https://www.fotmob.com/")
print(all_comp_info)
```

### Add Competition to Watchlist

* If already in list:

```python
print(all_comps.add_competition_to_my_watchlist(competition_name="open-cup", gather_all_competition_ids=all_comp_info))
```

* If NOT in list (custom URL):

```python
print(all_comps.add_competition_to_my_watchlist(
    competition_name="", 
    gather_all_competition_ids=all_comp_info, 
    defined_url="https://www.fotmob.com/leagues/55/matches/serie"
))
```

### Run the Program

#### Only Required Params:

```python
print(workflow_compute_mvp(competition_name="mls", competition_year="2023"))
print(workflow_compute_mvp(competition_name="serie", competition_year="2024-2025"))
```

#### Custom Scalar

```python
print(mvp.compute_mvp(competition_name="fifa-intercontinental-cup", competition_year="2024", scalar=14))
```

#### Two Leagues Per Year

```python
print(workflow_compute_mvp(competition_name="liga-mx", competition_year="2024-2025", open_close_league="Apertura"))
print(workflow_compute_mvp(competition_name="liga-mx", competition_year="2024-2025", open_close_league="Clausura"))
```

#### Memoization Without Override

```python
print(workflow_compute_mvp(competition_name="canadian-championship", competition_year="2025", overide=False))
```

#### Custom Table Title

```python
print(workflow_compute_mvp(competition_name="concacaf-champions-cup", competition_year="2025", title="Top Performing Players for North American Football Clubs"))
```

#### Custom Percentile Threshold

```python
mvp = MVP()
print(mvp.compute_mvp(competition_name="serie", competition_year="2024-2025", percentile_threshold=.97, title="Top 3% MVPs for Serie-A in 2024-2025"))
```

#### Use Manual Competition ID

```python
print(all_comps.add_competition_to_my_watchlist(competition_name="", gather_all_competition_ids=all_comp_info, defined_url="https://www.fotmob.com/leagues/47/matches/premier-league"))
print(workflow_compute_mvp(competition_name="premier-league", competition_year="2016-2017", manual_competition_id="47"))

print(all_comps.add_competition_to_my_watchlist(competition_name="", gather_all_competition_ids=all_comp_info, defined_url="https://www.fotmob.com/leagues/9986/matches/premier-league"))
print(workflow_compute_mvp(competition_name="premier-league", competition_year="2019", manual_competition_id="9986"))
```

#### Run Functions Individually

```python
comps = Competition()
print(comps.choose_competition(competition_name="mls", competition_year="2025"))

match = Match()
print(match.choose_match(competition_name="mls", competition_year="2025"))

player = Player()
print(player.choose_player_stats(competition_name="mls", competition_year="2025"))
print(player.competition_analysis(competition_name="mls", competition_year="2025"))

mvp = MVP()
print(mvp.compute_mvp(competition_name="mls", competition_year="2025", scalar=4))
```
### Sample Code:
    - International - Countries
        - print(workflow_compute_mvp(competition_name="concacaf-gold-cup", competition_year="2025"))
        - print(workflow_compute_mvp(competition_name="euro", competition_year="2024"))
        - print(workflow_compute_mvp(competition_name="concacaf-nations-league", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="uefa-nations-league-a", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="uefa-nations-league-b", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="uefa-nations-league-c", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="uefa-nations-league-d",competition_year= "2024-2025"))
        - print(workflow_compute_mvp(competition_name="copa-america", competition_year="2024"))
        - print(workflow_compute_mvp(competition_name="summer-olympics", competition_year="2024"))
        - print(workflow_compute_mvp(competition_name="world-cup", competition_year="2022"))
    - International - Clubs
        - print(workflow_compute_mvp(competition_name="fifa-club-world-cup", competition_year="2025"))
        - print(workflow_compute_mvp(competition_name="europa-league", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="afc-champions-league-elite", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="champions-league", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="concacaf-champions-cup", competition_year="2025"))
    - England
        - print(workflow_compute_mvp(competition_name="premier-league", competition_year="2024-2025", manual_competition_id="47"))
        - print(workflow_compute_mvp(competition_name="fa-cup", competition_year="2024-2025"))
    - France
        - print(workflow_compute_mvp(competition_name="ligue-1", competition_year="2024-2025", manual_competition_id="53"))
        - print(workflow_compute_mvp(competition_name="coupe-de-france", competition_year="2024-2025"))
    - Germany
        - print(workflow_compute_mvp(competition_name="bundesliga", competition_year="2024-2025", manual_competition_id="54"))
        - print(workflow_compute_mvp(competition_name="dfb-pokal", competition_year="2024-2025"))
    - Italy
        - print(mvp.compute_mvp(competition_name="serie", competition_year="2024-2025", manual_competition_id="55")) 
        - print(workflow_compute_mvp(competition_name="coppa-italia", competition_year="2024-2025"))
    - Mexico
        - print(workflow_compute_mvp(competition_name="liga-mx", competition_year="2023-2024", open_close_league="Apertura")) 
        - print(workflow_compute_mvp(competition_name="liga-mx", competition_year="2023-2024", open_close_league="Clausura")) 
    - USA
        - print(workflow_compute_mvp(competition_name="open-cup", competition_year="2025"))
        - print(workflow_compute_mvp(competition_name="mls", competition_year="2025"))        
    - Austria
        - print(workflow_compute_mvp(competition_name="eredivisie", competition_year="2024-2025"))        
    - Saudi Arabia
        - print(workflow_compute_mvp(competition_name="saudi-pro-league", competition_year="2024-2025"))        
    - Netherlands
        - print(workflow_compute_mvp(competition_name="eredivisie", competition_year="2024-2025", manual_competition_id="57"))        
    - Spain
        - print(workflow_compute_mvp(competition_name="laliga", competition_year="2024-2025"))
        - print(workflow_compute_mvp(competition_name="copa-del-rey", competition_year="2024-2025"))        
    - Scotland
        - print(workflow_compute_mvp(competition_name="premiership", competition_year="2024-2025"))        
    - Canada
        - print(workflow_compute_mvp(competition_name="canadian-championship", competition_year="2025"))
        - print(workflow_compute_mvp(competition_name="premier-league", competition_year="2025", manual_competition_id="9986"))

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

To maintain consistency, this MVP program will only consider a match as "played" if a player has been assigned an SPI rating. 

The FotMob player rating is calculated based on more than 100 individual stats per player per match

---

All Competition names and ids (i.e. all_comp_info):

{'38': 'bundesliga', '40': 'first-division-a', '42': 'champions-league', '43': 'confederation-cup', '44': 'copa-america', '45': 'copa-libertadores', '46': 'superligaen', '48': 'championship', '50': 'euro', '51': 'veikkausliiga', '57': 'eredivisie', '58': 'eredivisie', '64': 'premiership', '65': 'summer-olympics-women', '66': 'summer-olympics', '68': 'allsvenskan', '69': 'super-league', '73': 'europa-league', '85': '1-division', '86': 'serie-b', '108': 'league-one', '109': 'league-two', '112': 'liga-profesional', '113': 'a-league', '114': 'friendlies', '116': 'cymru-premier', '117': 'national-league', '119': '2-liga', '120': 'super-league', '121': 'primera-division', '122': '1-liga', '123': 'championship', '124': 'league-one', '125': 'league-two', '127': 'ligat-haal', '128': 'leumit-league', '129': 'premiership', '130': 'mls', '131': 'liga-1', '132': 'fa-cup', '135': 'super-league-1', '136': '1-division', '137': 'scottish-cup', '140': 'laliga2', '141': 'coppa-italia', '142': 'efl-trophy', '143': 'suomen-cup', '144': 'primera-division', '145': 'greece-cup', '147': 'serie-c', '150': 'coupe-de-la-ligue', '151': 'turkish-cup', '161': 'primera-division', '163': 'challenge-league', '169': 'ettan', '171': 'svenska-cupen', '176': '1-liga', '179': 'challenge-cup', '180': 'league-cup', '181': 'premiership-playoff', '182': 'super-liga', '187': 'league-cup', '189': 'liga-i', '190': 'cupa-româniei', '193': 'russian-cup', '196': 'ekstraklasa', '199': 'division-profesional', '204': 'postnord-ligaen', '205': 'norsk-tipping-ligaen', '207': 'trophée-des-champions', '209': 'dfb-pokal', '215': 'besta-deildin', '217': 'icelandic-cup', '218': 'first-division', '224': 'j-league-cup', '225': 'premier-league', '230': 'liga-mx', '231': 'national-division', '239': '2-division', '240': '3-division', '241': 'danmarksserien', '246': 'serie-a', '251': 'ykkosliiga', '256': 'kvindeligaen', '263': 'premier-league', '264': 'first-division-b', '267': 'premier-league', '270': 'first-professional-league', '273': 'primera-division', '274': 'categoría-primera-a', '287': 'euro-u19', '288': 'euro-u21', '290': 'asian-cup', '292': "women's-euro", '293': "women's-friendlies", '297': 'concacaf-champions-cup', '298': 'concacaf-gold-cup', '299': 'copa-sudamericana', '300': 'east-asian-championship', '301': 'european-championship-u-17', '305': 'toulon-tournament', '329': 'gulf-cup', '331': 'toppserien', '332': '1-division-kvinner', '335': 'primera-division', '336': 'liga-nacional', '337': 'liga-nacional', '338': '1-division', '339': 'primera-division', '342': 'finland-cup', '441': 'premier-league', '489': 'club-friendlies', '512': 'regionalliga', '519': 'premier-league', '524': 'stars-league', '525': 'afc-champions-league-elite', '526': 'caf-champions-league', '529': 'premier-league', '533': 'professional-football-league', '537': 'premier-soccer-league', '544': 'ligue-i', '8815': 'super-league-2', '8870': 'championship', '8944': 'national-north-&-south', '8947': 'premier-division', '8965': 'primera-b-nacional', '8968': 'primera-federación', '8969': 'ykkonen', '8971': 'serie-c', '8973': '2-liga', '8974': 'j-league-2', '8976': 'liga-de-expansión-mx', '8980': 'brazil-state-championship', '9015': 'liga-primera', '9039': 'lpf', '9080': 'k-league-1', '9081': 'bundesliga', '9100': '2-division', '9112': 'liga-3', '9113': 'liga-ii', '9116': 'k-league-2', '9122': 'segunda-division', '9123': 'second-league', '9125': 'primera-b', '9126': 'primera-b', '9134': 'nwsl', '9137': 'china-league-one', '9138': 'segunda-federación', '9141': 'germany-5', '9143': 'singapore-cup', '9210': 'france-4', '9213': 'primera-b-metropolitana-&-torneo-federal-a', '9253': 'fa-trophy', '9265': 'asean-championship', '9294': "women's-championship", '9296': 'usl-league-one', '9305': 'copa-argentina', '9306': 'cecafa-cup', '9345': 'copa-mx', '9375': "women's-champions-league", '9382': 'toppserien-qualification', '9390': 'west-asian-championship', '9391': 'copa-mx-clausura', '9408': 'international-champions-cup', '9422': 'k-league', '9428': 'african-nations-championship', '9429': 'copa-do-nordeste', '9441': 'open-cup', '9468': 'caf-confed-cup', '9469': 'afc-champions-league-two', '9470': 'afc-challenge-league', '9474': 'mtn8', '9478': 'indian-super-league', '9494': 'usl-league-two', '9495': 'a-league-women', '9500': 'we-league-women', '9514': 'the-atlantic-cup', '9537': 'k-league-3', '9545': 'highland-/-lowland', '9579': 'algarve-cup-qualification', '9656': 'concacaf-championship-u20', '9682': 'concacaf-central-american-cup', '9690': 'southeast-asian-games', '9717': "women's-league-cup", '9741': 'uefa-youth-league', '9754': 'obos-ligaen', '9806': 'uefa-nations-league-a', '9807': 'uefa-nations-league-b', '9808': 'uefa-nations-league-c', '9809': 'uefa-nations-league-d', '9821': 'concacaf-nations-league', '9833': 'asian-games', '9837': 'canadian-championship', '9841': 'afc-u20-asian-cup', '9876': 'saff-championship', '9906': 'liga-mx-femenil', '9907': 'liga-f', '9921': 'shebelieves-cup-qualification', '9986': 'premier-league', '10007': 'copa-de-la-liga-profesional', '10022': 'regionalliga', '10043': 'leagues-cup', '10046': 'copa-ecuador', '10056': 'liga-2', '10075': 'torneo-de-verano', '10076': 'league-cup', '10082': 'fa-cup-women', '10084': 'nisa', '10145': 'footballs-staying-home-cup-(esports)', '10167': 'nwsl-challenge-cup', '10176': 'premier-league-2-div-2', '10178': 'serie-a-femminile', '10188': 'k-league-3', '10207': 'nisa-legends-cup', '10216': 'conference-league', '10242': 'fifa-arab-cup', '10244': 'paulista-a1', '10269': 'womens-asian-cup', '10270': 'league-cup', '10272': 'carioca', '10273': 'mineiro', '10274': 'gaúcho', '10290': 'baiano', '10291': 'goiano', '10304': 'finalissima', '10309': 'durand-cup', '10310': 'stars-league-relegation-playoff', '10325': 'preseason', '10366': 'super-cup', '10368': 'copa-america-femenina', '10437': 'euro-u-21', '10449': 'nacional-feminino', '10457': "uefa-women's-nations-league-a", '10458': 'uefa-nations-league-b-women', '10459': 'uefa-nations-league-c-women', '10474': 'arab-club-champions-cup', '10498': 'summer-olympics-concacaf-qualification', '10508': 'african-football-league', '10511': 'afc-summer-olympics-women', '10584': 'south-africa-league', '10603': 'concacaf-gold-cup-women', '10607': 'euro', '10609': 'asian-cup--playoff', '10610': 'concafaf-gold-cup', '10611': 'champions-league', '10612': "women's-champions-league", '10613': 'europa-league', '10615': 'conference-league', '10616': 'euro-u19', '10617': 'euro-u17', '10618': 'copa-libertadores', '10619': 'caf-champions-league', '10621': 'concacaf-championship-u20', '10622': 'afc-champions-league-elite', '10623': 'copa-sudamericana', '10640': "women's-euro-league-a", '10641': "women's-euro-league-b", '10642': "women's-euro-league-c", '10649': 'nwsl-x-liga-mx', '10651': 'copa-de-la-reina', '10654': 'usl-jägermeister-cup', '10699': 'usl-super-league-women', '10703': 'fifa-intercontinental-cup', '10705': 'national-league-cup', '10708': 'knockout-cup', '10717': 'uefa-nations-league-a', '10718': 'uefa-nations-league-b', '10719': 'uefa-nations-league-c', '10791': 'swpl-1', '10840': 'baller-league', '10844': 'baller-league', '10872': 'northern-super-league'}

---

Todo:
    - Revamp README
    - Create an API
    - Containerize with Docker
    - CI/CD pipeline 
    - Add unit Tests
    - Level Analogy Comparison 
    - Levels of Soccer Comps 
    - Add functionality to save and read data via GCP Bucket 
    - Allow users to save results to Cloud Storage Service and/or Database

---

Resource: 
- (Fotmob Homepage) - */https://www.fotmob.com/*

- (Install Pyspark) - */https://medium.com/@jpurrutia95/install-and-set-up-pyspark-in-5-minutes-m1-mac-eb415fe623f3/*

---

Made by Drue Tomas Staples