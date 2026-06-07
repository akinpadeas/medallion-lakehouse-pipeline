-- models/marts/fact_trips.sql
--
-- Gold mart: fact_trips (star schema fact table)
--
-- Grain: one row per taxi trip.
-- Joins enriched trip data to dimension surrogate keys.
-- Incremental: merges on trip_sk to support idempotent re-runs.

{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_sk',
    tags=['gold', 'fact'],
    post_hook=[
        "{{ optimize_delta(this) }}",
    ]
) }}

with trips as (

    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
    where pickup_date >= '{{ var("start_date") }}'
      and pickup_date <= '{{ var("end_date") }}'
    {% endif %}

),

dim_date as (
    select date_sk, full_date from {{ ref('dim_date') }}
),

dim_zone as (
    -- Use current zone dimension values (SCD2: is_current = true)
    select zone_sk, zone_id from {{ ref('dim_zone') }}
    where is_current = true
),

dim_vendor as (
    select vendor_sk, vendor_id from {{ ref('dim_vendor') }}
),

final as (

    select
        -- Surrogate key: hash of business keys ensures idempotency
        {{ dbt_utils.generate_surrogate_key([
            'trips.vendor_id',
            'trips.pickup_datetime',
            'trips.pickup_location_id',
            'trips.dropoff_location_id'
        ]) }} as trip_sk,

        -- Foreign keys to dimensions
        date_dim.date_sk                    as date_fk,
        pickup_zone.zone_sk                 as pickup_zone_fk,
        dropoff_zone.zone_sk                as dropoff_zone_fk,
        vendor_dim.vendor_sk                as vendor_fk,

        -- Degenerate dimensions (no separate dim table needed)
        trips.payment_type,

        -- Trip measures
        trips.passenger_count,
        trips.trip_distance,
        trips.trip_duration_minutes,
        trips.speed_mph,

        -- Financial measures
        trips.fare_amount,
        trips.tip_amount,
        trips.total_amount,
        round(trips.tip_amount / nullif(trips.fare_amount, 0) * 100, 2) as tip_pct,

        -- Date parts (convenience — avoids join to dim_date for simple queries)
        trips.pickup_date,
        trips.pickup_hour,
        trips.pickup_day_of_week,
        trips.is_weekend,

        -- Lineage
        trips._batch_id,
        trips._ingestion_ts,
        current_timestamp()                 as _dbt_loaded_at

    from trips

    left join dim_date as date_dim
        on trips.pickup_date = date_dim.full_date

    left join dim_zone as pickup_zone
        on trips.pickup_location_id = pickup_zone.zone_id

    left join dim_zone as dropoff_zone
        on trips.dropoff_location_id = dropoff_zone.zone_id

    left join dim_vendor as vendor_dim
        on trips.vendor_id = vendor_dim.vendor_id

)

select * from final
