# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in 04-analytics-engineering/taxi_rides_ny/ to transform NYC taxi data and answer questions by querying the models.

<b> Question 1. </b> dbt Lineage and Execution

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

<b> Question 2. </b> dbt Tests

You've configured a generic test like this in your schema.yml:

    columns:
      - name: payment_type
        data_tests:
          - accepted_values:
              arguments:
                values: [1, 2, 3, 4, 5]
                quote: false

Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data. What happens when you run dbt test --select fct_trips?

    Answers: dbt will fail the test, returning a non-zero exit code

    Generic test used: 

    models: 
      - name: fact_trips
        description: "Fact table containing enriched trip data for analysis."
        columns:
          - name: payment_type
            description: "The method of payment used for the trip."
            data_tests:
              - accepted_values:
                  arguments:
                    values: [1, 2, 3, 4, 5]
                    quote: false
    
    When we created an generic test like the previous one, dbt creates a model with the name 'accepted_values_fact_trips_payment_type__False__1__2__3__4__5.sql' within the compiled folder in our project; Then when we build the model that contains the test for a specified column, it executes the test model that is stored in the compiled folder and if the condition is true (condition = 1), the test execution will fail with an non-zero exit code:

    with all_values as (

    select
        payment_type as value_field,
        count(*) as n_records

    from `project-0c3c5223-416f-4242-b0f`.`zoomcamp_bq_dataset`.`fact_trips`
    group by payment_type

    )

    select *
    from all_values
    where value_field not in (     -- Condition must to be false else test will not succed.
        1,2,3,4,5
    )

<b> Question 3. </b> Counting Records in fct_monthly_zone_revenue. 

After running your dbt project, query the fct_monthly_zone_revenue model.

What is the count of records in the fct_monthly_zone_revenue model?

    Answer: 12,184

    SELECT 
        COUNT(*) AS TotalRecords
    FROM `project-0c3c5223-416f-4242-b0f.dez_homework_4.fact_monthly_zone_revenue`
    ;

<b> Question 4. </b> Best Performing Zone for Green Taxis (2020)

Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.

Which zone had the highest revenue?

    Answer: East Harlem North

    SELECT
        pickup_zone, 
        DATE_TRUNC(Revenue_Month, YEAR) AS revenue_year,
        ROUND(SUM(total_revenue_per_month), 2) AS revenue_zone
        
    FROM `project-0c3c5223-416f-4242-b0f.dez_homework_4.fact_monthly_zone_revenue`
    WHERE service_type = 'Green'
    AND Revenue_Month BETWEEN '2020-01-01' AND '2020-12-01'
    GROUP BY pickup_zone, revenue_year
    ORDER BY revenue_zone DESC
    ;

<b> Question 5. </b> Green Taxi Trip Counts (October 2019)

Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?

    Answer: 384,624

    SELECT
        SUM(total_trips) AS TotalTrips201910 
    
    FROM `project-0c3c5223-416f-4242-b0f.dez_homework_4.fact_monthly_zone_revenue`
    WHERE service_type = 'Green'
    AND Revenue_Month = '2019-10-01'
    ;

<b> Question 6. </b> Build a Staging Model for FHV Data

Create a staging model for the For-Hire Vehicle (FHV) trip data for 2019.

1. Load the FHV trip data for 2019 into your data warehouse
2. Create a staging model stg_fhv_tripdata with these requirements:
- Filter out records where dispatching_base_num IS NULL
- Rename fields to match your project's naming conventions (e.g., PUlocationID → pickup_location_id)

What is the count of records in stg_fhv_tripdata?

    Answer: 43,244,693

    SELECT 
        COUNT(*) AS total_records

    FROM `project-0c3c5223-416f-4242-b0f.dez_homework_4.stg_fhv_tripdata`
    ;
    
    
    