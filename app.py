import polars as pl
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy import text
from datetime import datetime
import plotly.express as px
from typing import Optional
import pydeck as pdk
import pyodbc
from sqlalchemy.engine import URL
import pandas as pd




# Connect to DB 
@st.cache_resource
def get_db_engine():
    connection_string =(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=strava-azure-server.database.windows.net,1433;"
    "DATABASE=strava_azure_db;"
    "Authentication=ActiveDirectoryMsi;"
    )
    
    connection_url = URL.create('mssql+pyodbc',
                                query={'odbc_connect':connection_string}) 
    return create_engine(connection_url)

activities_pl = pl.DataFrame()
try:
    engine = get_db_engine()
    
    with engine.connect() as conn:
        result = conn.execute("SELECT TOP 5 * FROM activities")
        for row in result:
            st.write(row)

    activities_pd = conn.query("SELECT TOP 5 * FROM activities", ttl=600) 
    activities_pl = pl.from_pandas(activities_pd)
    st.write("✅ Successfully connected to Azure SQL with Managed Identity!")
    st.dataframe(activities_pl)

except Exception as e:
    st.error(f"❌ Failed to connect. Error: {e}")
    
# Add local testing via SQl auth (username/password)





# conn = st.connection('sql_azure',
#                        query={
#                             "driver": "ODBC Driver 17 for SQL Server",
#                             "authentication": "ActiveDirectoryPassword",
#                             "encrypt": "yes",
#                             })
# conn = st.connection("sql_new", 
#                      query={
#                             "driver": "ODBC Driver 17 for SQL Server",
#                             "authentication": "ActiveDirectoryPassword",                          
#                             "encrypt": "yes",                            })
# activities = conn.query('SELECT * FROM activities')

activities = activities_pl
    
def remove_outliers_z_score(
    df: pl.DataFrame,
    column: str,
    grouping: Optional[str] = None,
    z_threshold: int = 3
) -> pl.DataFrame:
    
    if grouping is None:
        # global mean & std
        column_mean = df[column].mean()
        column_std = df[column].std()
        return df.filter(((pl.col(column) - column_mean).abs() / column_std) < z_threshold)
    
    else:
        # mean & std per group using window functions
        return df.filter(
            (
                (pl.col(column) - pl.col(column).mean().over(grouping)).abs()
                / pl.col(column).std().over(grouping)
            ) < z_threshold
        )

activities.head()

# Data prep   
activities = activities.with_columns(
    pl.col('start_date_local').dt.year().alias('activity_year'),
    pl.col('start_date_local').dt.month().alias('activity_month'),
    pl.col('start_date_local').dt.weekday().alias('activity_weekday'),
    pl.col('start_date_local').dt.hour().alias('activity_hour'),
)
    
# Metrics:
first_use_date = datetime.now() - activities.select('start_date_local').min().item()
first_use_delta = first_use_date.days

last_run_delta = datetime.now() - activities.filter(pl.col('sport_type') == 'Run').select('start_date_local').max().item()
last_walk_delta = datetime.now() - activities.filter(pl.col('sport_type') == 'Walk').select('start_date_local').max().item()
last_hike_delta = datetime.now() - activities.filter(pl.col('sport_type') == 'Hike').select('start_date_local').max().item()
last_ride_delta = datetime.now() - activities.filter(pl.col('sport_type') == 'Ride').select('start_date_local').max().item()


# Activities overview:
activities_by_sport_type = activities.group_by('sport_type').len().sort('len', descending=True).filter(pl.col('len') > 1)

px.bar(x= 'sport_type',
       y= 'len',
       color='sport_type',
       data_frame=activities_by_sport_type,
       title='Activities done across Strava',
       labels={
           'sport_type': '',
           'len': 'Count of activities'
       },
        template="simple_white",
        color_discrete_sequence=px.colors.sequential.Aggrnyl).update_layout(
            hovermode='x'
        ).update_traces(
            hovertemplate=None
        )
        


moving_time_over_years = activities.filter(pl.col('sport_type').is_in(['Run', 'Walk', 'Ride'])
                                    ).group_by('sport_type', 'activity_year'
                                        ).agg(pl.col('moving_time_hr').mean()
                                            ).sort('activity_year', descending=True)
                                        
# Remove outliers
moving_time_over_years = remove_outliers_z_score(moving_time_over_years, column='moving_time_hr')      


# Weekly Snapshot

# Metrics to show:
# Activities this week
# Distance moved this week
# Time active this week

weekly_snapshot = activities.with_columns(
    pl.col('start_date_local').dt.week().alias('week')
).group_by('activity_year','week').agg(
    pl.col('activity_id').len().alias('activities'),
    pl.col('distance_km').sum().round(2).alias('distance'),
    pl.col('moving_time_hr').sum().round(2).alias('time')
).top_k(2, by = ['activity_year', 'week']).with_columns( # Add previous week metrics
    pl.col('activities').shift(-1).alias('previous_week_activities'),
    pl.col('distance').shift(-1).alias('previous_week_distance'),
    pl.col('time').shift(-1).alias('previous_week_time')
    ).top_k(1, by=['activity_year', 'week']) # Keep only this week row



# Streamlit App
st.write('Strava dashboard: A deep dive into activities')     
weekly_activities, weekly_distance, weekly_time = st.columns(3)

weekly_activities.metric(label='Activities this week',
          value=weekly_snapshot['activities'].item(),
          delta=weekly_snapshot['previous_week_activities'].item()
          )       

weekly_distance.metric(label='Distance (km)',
          value=weekly_snapshot['distance'].item(),
          delta=weekly_snapshot['previous_week_distance'].item()
          )

weekly_time.metric(label='Time moved (hours)',
          value=weekly_snapshot['time'].item(),
          delta=weekly_snapshot['previous_week_time'].item()
          )       

map_data = activities.select('start_latitude', 'start_longitude').drop_nulls().with_columns(pl.col('start_latitude').alias('lat'),
                                                                                        pl.col('start_longitude').alias('lon')).to_pandas()
# Default mapping service
#st.map(map_data) 

# Selectbox for selecting type of sport


# Customizing initial view via pydeck_chart
st.pydeck_chart(
    pdk.Deck(
        map_style= None,
        initial_view_state=pdk.ViewState(
            latitude=43.6532,
            longitude=-79.3832,
            zoom=11,
            pitch=15
        ),
        layers=[
            pdk.Layer(
                'ScatterplotLayer',
                data=map_data,
                get_position='[lon, lat]',
                get_color = '[200,30,0,160]',
                get_radius=100
                )]
    )
)


