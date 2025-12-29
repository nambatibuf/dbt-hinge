with 
    source as (

        select
            *
        from {{ source('raw_app', 'users') }}
    )
select * from source
