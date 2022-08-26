




    with grouped_expression as (
    select
        
        
    
  ACTUAL_SECONDS is not null as expression


    from CDW_DEV.SANDBOX_W846026.FCT_ASSIGMENT
    

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression = true)

)

select *
from validation_errors



