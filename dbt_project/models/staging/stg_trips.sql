-- models/staging/stg_trips.sql
--
-- Staging model: silver.trips → staging layer
--
-- Purpose: 1:1 with the Silver trips table. Light casting only.
-- No business logic here — that lives in intermediate and marts.
--
-- Filters:
--   - Only current-valid records (no DQ flags on key fields)
--   - Excludes rows with NEGATIVE_FARE or FUTURE_PICKUP flags
--   - Incremental: only process rows for the current run window

{{ config(
    materialized='view',
    tags=['staging', 'trips']
) }}

with source as (

    select * from {{ source('silver', 'trips') }}

    {% if is_incremental() %}
    where pickup_date >= '{{ var("start_date") }}'
      and pickup_date <= '{{ var("end_date") }}'
    {% endif %}

),

cleaned as (

    select
        -- Keys
        vendor_id,
        pickup_location_id,
        dropoff_location_id,

        -- Timestamps and derived date parts
        pickup_datetime,
        dropoff_datetime,
        pickup_date,
        pickup_hour,
        pickup_day_of_week,
        is_weekend,

        -- Trip metrics
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        speed_mph,

        -- Financials
        fare_amount,
        tip_amount,
        total_amount,
        payment_type,

        -- Quality flags
        _dq_flags,
        _batch_id,
        _ingestion_ts

    from source

    -- Exclude rows with critical DQ issues
    where _dq_flags not like '%NEGATIVE_FARE%'
      and _dq_flags not like '%FUTURE_PICKUP%'

)

select * from cleaned
