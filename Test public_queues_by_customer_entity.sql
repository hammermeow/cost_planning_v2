select * from public.queues_by_customer_entity q
Join nvrpt.finance_deck_cp f on f.task_queue_id = q.task_queue_id
where f.task_queue_id = '4e1222ce-9425-46d9-b13c-8c7a0d2ffc95'
