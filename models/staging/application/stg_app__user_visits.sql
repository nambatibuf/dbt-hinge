{{ config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'event_id',
    on_schema_change = 'append_new_columns'
) }}

with bronze as (

    select
        topic,
        cast(partition as int)      as kafka_partition,
        cast(offset as bigint)      as kafka_offset,
        cast(kafka_timestamp as timestamp) as kafka_timestamp,
        key_str,
        value_str,
        cast(ingest_ts as timestamp) as ingest_ts
    from {{ source('raw_app', 'user_visits') }}

    {% if is_incremental() %}
      where ingest_ts >
        (select coalesce(max(ingest_ts), timestamp('1900-01-01')) from {{ this }})
    {% endif %}

),

parsed as (

    select
        *,
        from_json(
          value_str,
          'struct<
            event_id:string,
            event_type:string,
            viewer_user_id:bigint,
            viewed_user_id:bigint,
            impression_time_utc:string,
            impression_date:string,
            impression_hour_utc:string,
            source:string,
            platform:string,
            viewer_membership_tier:string,
            rank_in_feed:int,
            is_boosted_profile:boolean,
            viewer_has_likes_left:boolean,
            viewer_country:string,
            viewer_city:string
          >',
          map('mode','PERMISSIVE')
        ) as v
    from bronze

),

flattened as (

    select
        -- business/event fields
        v.event_id                                   as event_id,
        v.event_type                                 as event_type,
        cast(v.viewer_user_id as bigint)              as viewer_user_id,
        cast(v.viewed_user_id as bigint)              as viewed_user_id,

        -- timestamps/dates (safe casting)
        to_timestamp(v.impression_time_utc)           as impression_time_utc,
        to_date(v.impression_date)                    as impression_date,
        cast(v.impression_hour_utc as int)            as impression_hour_utc,

        v.source                                     as source,
        v.platform                                   as platform,
        v.viewer_membership_tier                      as viewer_membership_tier,
        cast(v.rank_in_feed as int)                   as rank_in_feed,
        cast(v.is_boosted_profile as boolean)         as is_boosted_profile,
        cast(v.viewer_has_likes_left as boolean)      as viewer_has_likes_left,
        v.viewer_country                              as viewer_country,
        v.viewer_city                                 as viewer_city,

        -- kafka lineage
        topic,
        kafka_partition,
        kafka_offset,
        kafka_timestamp,
        key_str,
        ingest_ts
    from parsed
    where v.event_id is not null

),

deduped as (
    select * except(rn)
    from (
        select
            f.*,
            row_number() over (
                partition by event_id
                order by ingest_ts desc, kafka_offset desc
            ) as rn
        from flattened f
    )
    where rn = 1

)

select * from deduped
