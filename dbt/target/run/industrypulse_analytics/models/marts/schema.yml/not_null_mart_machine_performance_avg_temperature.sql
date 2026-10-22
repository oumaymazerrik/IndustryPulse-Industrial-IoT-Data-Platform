
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_temperature
from "industrypulse"."analytics"."mart_machine_performance"
where avg_temperature is null



  
  
      
    ) dbt_internal_test