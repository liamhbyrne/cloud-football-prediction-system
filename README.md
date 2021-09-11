# Football Predictions
*my third iteration of football prediction models*    
[**Working Progress**] This will be a football prediction application hosted on the Google Cloud Platform.
Predictions will be easy to generate and more accurate.  

The backend is hosted on the low spec e2-micro Compute Engine (VM). The VM hosts a PostgreSQL database and a Flask 
server to trigger different events. Due to the low power of the e2-micro, I decided to offload large processing jobs to
triggered containers on Google Cloud Run. The database will be able to refresh itself with new data meaning predictions
are always well-informed.

[11 Sept 2021] - Database complete; *41k matches, 65k players and 25k historical match odds*.