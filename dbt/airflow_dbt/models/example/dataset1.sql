/*
A dataset containing the location, date and temperature of the highest 
temperatures reported by location and month.
*/

with hourly_source as (

    SELECT  
        DATE_PART('day',to_timestamp(DT, 'YYYY-MM-DD hh24:mi:ss')::timestamp) as MONTH,
        cast(TEMP as numeric) as TEMP,                  
        CITY,
        STATE,
        COUNTRY
    FROM hourly

)
    
    SELECT 
        city,
        state,
        country, 
        month, 
        MAX(TEMP)  as temp_max
    FROM hourly_source
    GROUP BY city, state, country, month


