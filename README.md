# Football Predictions
---
**Introduction**

This is my most recent iteration of football prediction systems. This system automates both scraping from websites and the predictions. The results and player ratings stored in the database are always up to date. The system is run on the _Google Cloud Platform_ with a low-power e2-micro VM instance; the more intense computation is offloaded to Google Cloud _Run_ which enables Docker containers to be triggered with HTTP requests. 

This system is able to make informed decisions across all major football leagues in Europe. At the time of writing, the database holds 41k matches, 65k players and 25k historical match odds. After implementing the previous [Deep Learning model](https://github.com/liamhbyrne/Premier-League-Predictions-with-Deep-Learning) the model can predict the outcome of 56% of matches correctly (_random would be 33%_). 

This project also carries out an investigation into whether this model can see a consistent return on investment when applied to betting markets. The model provides a confidence of the outcome it predicts, this can inform variable sized bets using Kelly Criterion. 


**Previous iterations**: 
1. [Football Predictions using Twitter sentiment analysis](https://github.com/liamhbyrne/twitter-football-prediction)
2. [Premier League Predictions with Deep Learning](https://github.com/liamhbyrne/Premier-League-Predictions-with-Deep-Learning)

---
