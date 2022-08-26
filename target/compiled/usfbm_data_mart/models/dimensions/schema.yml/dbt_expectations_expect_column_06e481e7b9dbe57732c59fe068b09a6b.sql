



with validation_errors as (

    select
        DIM_DATE_SK
    from CDW_DEV.SANDBOX_W846026.DIM_DATE
    where
        1=1
        and 
        (
            DIM_DATE_SK is not null
            
        )
    
    group by
        DIM_DATE_SK
    having count(*) > 1

)
select * from validation_errors

