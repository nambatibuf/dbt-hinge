{{ config(
    materialized = 'incremental',
    unique_key = 'user_id',
    incremental_strategy = 'merge',
    contract = { "enforced": true },
    on_schema_change = 'fail'
) }}

with base as (

    select
        user_id,
        status,
        country_code,
        timezone,
        is_test_user,
        marketing_source,

        -- created timestamp (contract: timestamp)
        cast(created_at as timestamp) as user_created_at,

        -- profile
        profile.first_name                  as first_name,
        profile.city                        as city,
        cast(profile.birthdate as date)     as birthdate,
        profile.gender                      as gender,
        profile.orientation                 as orientation,
        cast(profile.height_cm as int)      as height_cm,
        profile.education_level             as education_level,
        profile.employer                    as employer,
        profile.job_title                   as job_title,

        -- location
        profile.approx_location.country_code as approx_country_code,
        profile.geo.lat                      as approx_lat,
        profile.geo.lon                      as approx_lon,

        -- preferences (contract: int)
        preferences.gender_pref                  as pref_gender,
        cast(preferences.min_age as int)         as pref_min_age,
        cast(preferences.max_age as int)         as pref_max_age,
        cast(preferences.max_distance_km as int) as pref_max_distance_km,

        -- unified change timestamp (contract: timestamp)
        cast(
          coalesce(
              profile.updated_at,
              preferences.updated_at,
              created_at
          ) as timestamp
        ) as event_ts

    from {{ source('raw_app', 'users') }}

)

select *
from base

{% if is_incremental() %}
  where event_ts >
    (
      select coalesce(
          max(event_ts),
          to_timestamp('1900-01-01')
      )
      from {{ this }}
    )
{% endif %}