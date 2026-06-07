-- models/marts/agg_daily_zone_summary.sql
--
-- Gold aggregate: daily trip summary by pickup zone
--
-- Pre-aggregated for BI tools and dashboards.
-- Grain: one row per (pickup_date, pickup_zone).
-- Incremental merge on the composite unique key.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['pickup_date', 'pickup_zone_fk'],
    tags=['gold', 'aggregate']
) }}

with fact as (

    select * from {{ ref('fact_trips') }}

    {% if is_incremental() %}
    where pickup_date >= '{{ var("start_date") }}'
      and pickup_date <= '{{ var("end_date") }}'
    {% endif %}

),

dim_zone as (
    select zone_sk, zone_id, zone_name, borough, service_zone
    from {{ ref('dim_zone') }}
    where is_current = true
),

aggregated as (

    select
        fact.pickup_date,
        fact.pickup_zone_fk,
        zone.zone_name                              as pickup_zone_name,
        zone.borough                                as pickup_borough,
        zone.service_zone,

        -- Volume metrics
        count(*)                                    as total_trips,
        sum(fact.passenger_count)                   as total_passengers,

        -- Distance metrics
        round(avg(fact.trip_distance), 2)           as avg_trip_distance_miles,
        round(sum(fact.trip_distance), 2)           as total_trip_distance_miles,

        -- Duration metrics
        round(avg(fact.trip_duration_minutes), 1)   as avg_trip_duration_minutes,

        -- Revenue metrics
        round(sum(fact.fare_amount), 2)             as total_fare_amount,
        round(sum(fact.tip_amount), 2)              as total_tip_amount,
        round(sum(fact.total_amount), 2)            as total_revenue,
        round(avg(fact.fare_amount), 2)             as avg_fare_per_trip,
        round(avg(fact.tip_pct), 2)                 as avg_tip_pct,

        -- Time-of-day breakdown
        count(case when fact.pickup_hour between 7 and 9 then 1 end)   as am_peak_trips,
        count(case when fact.pickup_hour between 17 and 19 then 1 end) as pm_peak_trips,
        count(case when fact.is_weekend = true then 1 end)             as weekend_trips,

        -- Metadata
        current_timestamp()                         as _dbt_loaded_at

    from fact

    left join dim_zone as zone
        on fact.pickup_zone_fk = zone.zone_sk

    group by 1, 2, 3, 4, 5

)

select * from aggregated
