{{ config(
    materialized         = 'incremental',
    unique_key           = 'user_sk',
    incremental_strategy = 'merge'
) }}

with src as (

    select
        -- SCD2 surrogate key from snapshot
        dbt_scd_id as user_sk,

        -- business key
        user_id,

        -- core attrs (renamed)
        status       as user_status,
        country_code as user_country_code,
        timezone,
        cast(is_test_user as boolean) as is_test_user_flag,
        marketing_source,

        -- profile
        first_name,
        city,
        birthdate,
        gender,
        orientation,
        height_cm,
        education_level,
        employer,
        job_title,

        -- preferences
        pref_gender,
        pref_min_age,
        pref_max_age,
        pref_max_distance_km,

        -- location
        approx_country_code,
        approx_lat,
        approx_lon,

        -- timestamps from staging/snapshot
        user_created_at,
        event_ts,
        dbt_valid_from,
        dbt_valid_to,
        dbt_updated_at,

        -- date keys (YYYYMMDD int) for joining to dim_date
        cast(date_format(birthdate, 'yyyyMMdd') as int)      as birthdate_key,
        cast(date_format(user_created_at, 'yyyyMMdd') as int) as user_created_date_key,
        cast(date_format(dbt_valid_from, 'yyyyMMdd') as int) as valid_from_date_key,
        cast(
            date_format(
                coalesce(dbt_valid_to, to_timestamp('9999-12-31')),
                'yyyyMMdd'
            ) as int
        )                                                    as valid_to_date_key,

        -- current-flag derived from valid_to
        case when dbt_valid_to is null then 1 else 0 end     as is_current_record

    from {{ ref('snapshot_users_scd2') }}
)

select *
from src

{% if is_incremental() %}
  -- only upsert new SCD2 versions into dim_users
  where dbt_valid_from >
    (select coalesce(max(dbt_valid_from), to_timestamp('1900-01-01')) from {{ this }})
{% endif %}