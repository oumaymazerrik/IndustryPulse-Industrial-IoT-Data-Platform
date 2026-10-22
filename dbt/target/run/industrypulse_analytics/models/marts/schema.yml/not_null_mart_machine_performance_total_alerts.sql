
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_alerts
from "industrypulse"."analytics"."mart_machine_performance"
where total_alerts is null



  
  
      
    ) dbt_internal_test