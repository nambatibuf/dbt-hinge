{{ config(
    materialized = 'table'
) }}

with source as (

    select * from {{ ref('stg_app__subscription') }}
)

select * from source
