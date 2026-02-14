# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in 04-analytics-engineering/taxi_rides_ny/ to transform NYC taxi data and answer questions by querying the models.

Question 1. dbt Lineage and Execution
Given a dbt project with the following structure:

    models/
    ├── staging/
    │   ├── stg_green_tripdata.sql
    │   └── stg_yellow_tripdata.sql
    └── intermediate/
        └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)

If you run dbt run --select int_trips_unioned, what models will be built?

    Answer: 
        int_trips_unioned only
    
    