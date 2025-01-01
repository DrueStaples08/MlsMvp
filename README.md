# MlsMvp
Analysis to determine the most valuable player and a API to run it all 



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
