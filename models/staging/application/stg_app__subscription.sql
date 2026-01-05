with source as (

    select * from {{ source('raw_app', 'subscription_plans') }}
),

renaming as (

    select
        plan_id as product_id,
        plan_name as product_name,
        billing_period as product_type,
        tier as product_category,
        perks.discount as product_discount,
        perks.likes_unlimited as product_likes_unlimited,
        perks.priority_likes as product_priority_likes,
        perks.standouts_boost as product_standouts_boost,
        regexp_extract(plan_name, '\\d+(?=\\s*Pack)', 0) AS product_roses_count
    from source
)

select * from renaming