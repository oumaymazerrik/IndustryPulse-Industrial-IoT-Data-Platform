
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select machine_id
from "industrypulse"."analytics"."mart_machine_performance"
where machine_id is null



  
  
      
    ) dbt_internal_test