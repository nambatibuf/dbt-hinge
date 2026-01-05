{{ config(
    materialized = 'incremental',
    unique_key = 'like_id',
    incremental_strategy = 'append'
) }}

with source as (
    select * from {{ source('raw_app', 'user_likes') }}
),

incremental_filtered as (
    select
        like_id,
        liker_user_id,
        liked_user_id,
        like_type,
        source_surface,
        liked_at,
        comment_text
    from source
    {% if is_incremental() %}
      where liked_at > (
        select coalesce(max(liked_at), '1970-01-01')
        from {{ this }}
      )
    {% endif %}
)

select * from incremental_filtered
