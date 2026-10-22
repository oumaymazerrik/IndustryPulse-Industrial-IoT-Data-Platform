
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    machine_id as unique_field,
    count(*) as n_records

from "industrypulse"."analytics"."mart_machine_performance"
where machine_id is not null
group by machine_id
having count(*) > 1



  
  
      
    ) dbt_internal_test