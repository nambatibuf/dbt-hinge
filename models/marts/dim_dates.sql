{{ config(
    materialized = 'table'
) }}

with source as (

    select * from {{ ref('stg_default__dates') }}
)

select * from source