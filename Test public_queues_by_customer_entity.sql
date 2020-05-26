select * from public.queues_by_customer_entity q
Join nvrpt.finance_deck_cp f on f.task_queue_id = q.task_queue_id
where f.task_queue_id = '4e1222ce-9425-46d9-b13c-8c7a0d2ffc95'

--------------------------------------------------------------------------------------
select count(distinct task_queue_id) 
from (select task_queue_id,count(*) from public.queues_by_customer_entity group by 1
having count(*)>1)

select task_queue_id,count(customer) from public.queues_by_customer_entity group by 1
having count(*)>1

select count(distinct task_queue_id)
from
(SELECT task_queue_id, count(customer_grp_value)
FROM 
(SELECT DISTINCT customer_grp_value,
             customer_grp_value AS vertical_or_sub_initiative,
             customer_grp_value AS device_or_initiative,
             transformation,
             task_queue_name,
             task_queue_id,
             'RAMP' AS queue_type,
             'RAMP' AS priority,
             NULL AS service_order_id,
             NULL AS aim_work_type,
             NULL AS order_name,
             NULL AS customer_grp_key
      FROM nvads.ramp_edx)
group by 1
having count(customer_grp_value)>1
)

SELECT task_queue_id, count(customer_grp_value)
FROM 
(SELECT DISTINCT customer_grp_value,
             customer_grp_value AS vertical_or_sub_initiative,
             customer_grp_value AS device_or_initiative,
             transformation,
             task_queue_name,
             task_queue_id,
             'RAMP' AS queue_type,
             'RAMP' AS priority,
             NULL AS service_order_id,
             NULL AS aim_work_type,
             NULL AS order_name,
             NULL AS customer_grp_key
      FROM nvads.ramp_edx)
group by 1
having count(customer_grp_value)>1
-------------------------------------------------------------------------------------------------------

select distinct q.program from public.queues_by_customer_entity q
Join nvrpt.finance_deck_cp f on f.task_queue_id = q.task_queue_id
where f.program is not null and f.program <> 'null'

select top 1 * from public.queues_by_customer_entity q
Join nvrpt.finance_deck_cp f on f.task_queue_id = q.task_queue_id
where f.program is not null and f.program <> 'null'
