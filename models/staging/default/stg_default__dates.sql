with date_range as (

    -- one row per day from 2015-01-01 to 2035-12-31
    select
      explode(
        sequence(
          to_date('2015-01-01'),
          to_date('2035-12-31'),
          interval 1 day
        )
      ) as date

),

date_dim as (
    select
        -- keys & base
        cast(date_format(date, 'yyyyMMdd') as int)          as date_key,
        date,
        date_format(date, 'EEEE, MMMM dd, yyyy')            as date_description,

        -- year / quarter
        year(date)                                          as year,
        quarter(date)                                       as quarter,
        concat('Q', cast(quarter(date) as string))          as quarter_name,
        case quarter(date)
            when 1 then 'First Quarter'
            when 2 then 'Second Quarter'
            when 3 then 'Third Quarter'
            else      'Fourth Quarter'
        end                                                 as quarter_name_full,

        cast(date_trunc('quarter', date) as date) as start_of_quarter,
        date_add(add_months(date_trunc('quarter', date), 3), -1)
                                                            as end_of_quarter,
        concat(cast(year(date) as string),
               ' Q',
               cast(quarter(date) as string))              as year_quarter_name,

        -- month
        month(date)                                         as month,
        date_format(date, 'MMMM')                           as month_name,
        date_format(date, 'MMM')                            as month_name_short,
        try_cast(date_trunc('month',   date) as date) as start_of_month,
        last_day(date)                                      as end_of_month,
        day(last_day(date))                                 as days_in_month,
        date_format(date, 'yyyy-MM')                        as year_month,
        month(date)                                         as month_number_in_year,

        -- day of month / week
        day(date)                                           as day,

        -- make Monday = 1 ... Sunday = 7
        ((dayofweek(date) + 5) % 7) + 1                     as day_of_week,
        date_format(date, 'EEEE')                           as day_of_week_name,
        date_format(date, 'EEE')                            as day_of_week_name_short,

        -- week (Mon–Sun)
        date_add(date,
                 -(((dayofweek(date) + 5) % 7)))            as start_of_week,
        date_add(date,
                 6 - (((dayofweek(date) + 5) % 7)))         as end_of_week,
        dayofyear(date)                                     as day_number_in_year,
        weekofyear(date)                                    as week_number_in_year,

        -- year
        try_cast(date_trunc('year',    date) as date) as start_of_year,
        date_add(add_months(date_trunc('year', date), 12), -1)
                                                            as end_of_year,

        -- simple fiscal = calendar
        year(date)                                          as fiscal_year,
        quarter(date)                                       as fiscal_quarter,
        month(date)                                         as fiscal_month,

        -- month-end indicator
        case when date = last_day(date)
             then 'Last Day In Month'
             else 'Not Last Day In Month'
        end                                                 as last_day_in_month_indicator,

        -- holiday indicator (US federal holidays seed)
        case
            when date in (select date from {{ ref('us_federal_holidays') }})
                then 'Holiday'
            else 'Not Holiday'
        end                                                 as holiday_indicator,

        -- weekend indicator (Spark: 1=Sun, 7=Sat)
        case
            when dayofweek(date) in (1, 7)
                then 'Weekend'
            else 'Not Weekend'
        end                                                 as weekend_indicator

    from date_range
),

special_date as (
    select
        -1          as date_key,
        null        as date,
        'Unknown'   as date_description,
        null        as year,
        null        as quarter,
        'Unknown'   as quarter_name,
        'Unknown'   as quarter_name_full,
        null        as start_of_quarter,
        null        as end_of_quarter,
        'Unknown'   as year_quarter_name,
        null        as month,
        'Unknown'   as month_name,
        'Unknown'   as month_name_short,
        null        as start_of_month,
        null        as end_of_month,
        null        as days_in_month,
        'Unknown'   as year_month,
        null        as month_number_in_year,
        null        as day,
        null        as day_of_week,
        'Unknown'   as day_of_week_name,
        'Unknown'   as day_of_week_name_short,
        null        as start_of_week,
        null        as end_of_week,
        null        as day_number_in_year,
        null        as week_number_in_year,
        null        as start_of_year,
        null        as end_of_year,
        null        as fiscal_year,
        null        as fiscal_quarter,
        null        as fiscal_month,
        'Unknown'   as last_day_in_month_indicator,
        'Unknown'   as holiday_indicator,
        'Unknown'   as weekend_indicator
)

select *
from date_dim
union all
select *
from special_date;
