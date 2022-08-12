/*
A dataset containing the average temperature, min temperature,
 location of min temperature, and location of max temperature per day.
*/

with hourly_source as (

    SELECT  
        DATE_PART('day',to_timestamp(DT, 'YYYY-MM-DD hh24:mi:ss')::timestamp) as DAY,
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
        day, 
        MAX(TEMP)  as temp_max,
        MIN(TEMP)  as temp_min,
        ROUND(AVG(TEMP),2)  as temp_avg
    FROM hourly_source
    GROUP BY city, state, country, day


