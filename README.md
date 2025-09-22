# Strava activities analytics
This is an end-to-end data analysis & ML project built on top of activities data gathered via Strava API. 
It is a dashboard showcasing temporal and performance metrics on different sports activities captured using Strava.

# Motivation:
I wanted to have control over the data I am working with and built a data analytics project showcasing different aspects of a 
data project ranging from data sourcing, data pipelines, clearning, analysis, ML and deployment. It is meant to be an iterative process
that evolves over time. Below is a rough scope and roadmap for the project:
- **Data Access:** Access activities data using `Strava API`.
- **Database Design:** Develop Database schema on `Azure SQL`/`MySQL` to store and utilize pulled activies data for analysis.
- **Data Orchestration:** Automate data refresh by pulling latest activities data based on event/time based triggers using `Prefect`/`Azure Data Facory`.
- **Dashboard build and Deployment:** Build a live webapp/dashboard using `Streamlit` to showcase perforamce, temporal metrics and patterns found in sports activities.
- **CI/CD Pipelines:** Automate webapp deployment via Github Actions based on new commits.
- **ML Deployment & Serving:** Build ML capabilities into the app and serve it via API or webapp. (`FastAPI`, `MLFlow`)
- **ML Evals:** Built infra to monitor and access ML evaluation metrics and potentially automate retraining.

# Tech Stack:

#Usage:
https://predict-my-run.azurewebsites.net/

