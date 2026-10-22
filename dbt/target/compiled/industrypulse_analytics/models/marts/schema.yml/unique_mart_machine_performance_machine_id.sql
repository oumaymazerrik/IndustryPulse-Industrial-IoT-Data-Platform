
    
    

select
    machine_id as unique_field,
    count(*) as n_records

from "industrypulse"."analytics"."mart_machine_performance"
where machine_id is not null
group by machine_id
having count(*) > 1


