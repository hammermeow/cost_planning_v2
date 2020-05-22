DROP VIEW public.queues_by_customer_entity;

CREATE OR REPLACE VIEW public.queues_by_customer_entity 
AS
SELECT CASE
         WHEN (lower(task_queue_name) LIKE '%certify%' OR lower(task_queue_name) LIKE '%gsr-verify-verify%' OR lower(task_queue_name) LIKE '%verify-verify-verify%') THEN 'ADS-Quality'
         WHEN (LOWER(queue_type) LIKE '%training%' OR LOWER(queue_type) LIKE '%accuracy%' OR LOWER(queue_type) LIKE '%error categorization%') THEN 'ADS-WFM'
         WHEN LOWER([priority]) LIKE '%accuracy%' THEN 'ADS-Quality'
         WHEN LOWER([priority]) LIKE '%training%' THEN 'ADS-WFM'
         WHEN (lower(task_queue_name) LIKE '%transcription audit%' OR LOWER(queue_type) = 'quality') THEN 'ADS-Quality'
         WHEN LOWER(queue_type) LIKE '%training%' THEN 'ADS-WFM'
         WHEN lower(task_queue_name) LIKE '%ads-science%' THEN 'ADS-Science'
         WHEN lower(task_queue_name) LIKE '%ads-wfm%' THEN 'ADS-WFM'
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative = 'Other' AND (lower(queue_type) LIKE '%eeep%' OR lower(task_queue_name) LIKE '%fx-clue%' OR task_queue_name LIKE '%FX-CLUE%' OR lower(task_queue_name) LIKE '%ramp%' OR task_queue_name LIKE '% FX-CLUE%' OR task_queue_name LIKE '%FX-CLUE %' OR lower(task_queue_name) LIKE '%clue%')) THEN 'ADS-RAMP'
         WHEN (customer_grp_value IS NULL AND LOWER(transformation) LIKE '%wake word%') THEN 'WakeWord'
         WHEN (lower(task_queue_name) LIKE '%privacy_enhancements%' AND (lower(vertical_or_sub_initiative) LIKE '%other%' OR lower(vertical_or_sub_initiative) LIKE '%ads-wfm%' OR lower(vertical_or_sub_initiative) LIKE '%ads-science%')) THEN 'ADSC Tech'
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative = 'Echo Devices') THEN '1P Devices'
         WHEN (customer_grp_value IS NULL AND lower(vertical_or_sub_initiative) LIKE '%1p%') THEN '1P Devices'
         WHEN (customer_grp_value IS NULL AND lower(vertical_or_sub_initiative) LIKE '%2p%') THEN '2P Devices'
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative = 'Alexa Enabled Amazon Devices') THEN '2P Devices'
         WHEN (customer_grp_value = 'Alexa Enabled Amazon Devices' AND vertical_or_sub_initiative = 'Alexa Enabled Amazon Devices') THEN '2P Devices'
         WHEN (customer_grp_value = 'Auto/Navigation' OR customer_grp_value = 'Alexa Automotive' OR customer_grp_value = 'Automotive' OR customer_grp_value = 'Auto' OR (customer_grp_value IS NULL AND (vertical_or_sub_initiative = 'Other' AND device_or_initiative = 'Auto'))) THEN 'Alexa Automotive'
         WHEN (customer_grp_value = 'Alexa Experience and Devices (EAD)' OR vertical_or_sub_initiative = 'Alexa Experience and Devices (EAD)' OR customer_grp_value = 'Alexa Experience & Devices (EAD)' OR vertical_or_sub_initiative = 'Alexa Experience & Devices (EAD)' OR vertical_or_sub_initiative LIKE 'Alexa Experience%') THEN 'Alexa Experience and Personality'
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative IN ('Alexa Brain')) THEN 'Alexa Intelligent Decisions'
         WHEN (customer_grp_value IS NULL AND device_or_initiative IN ('Alexa Brain')) THEN 'Alexa Intelligent Decisions'
         WHEN (customer_grp_value = 'ID') THEN 'Alexa Intelligent Decisions'
         WHEN (customer_grp_value = 'Alexa Communication') THEN 'Communication'
         WHEN (customer_grp_value IS NULL AND device_or_initiative = 'Other' AND vertical_or_sub_initiative = 'Dylan VBM') THEN 'Dylan (VBM)'
         WHEN (customer_grp_value = 'HHO' OR vertical_or_sub_initiative = 'HHO') THEN 'Household Organization (HHO)'
         WHEN (vertical_or_sub_initiative = 'Information Non-QA') THEN 'Information'
         WHEN (customer_grp_value = 'SmartHome') THEN 'Smart Home'
         WHEN (customer_grp_value IS NULL AND customer_grp_value = 'Ask') THEN 'ASK'
         WHEN (customer_grp_value = 'Health & Wellness') THEN 'Health and Wellness'
         WHEN (customer_grp_value IS NULL AND device_or_initiative = 'Hybrid') THEN 'Alexa Hybrid'
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative = 'Other' AND device_or_initiative = 'Speaker ID') THEN 'SpeakerID'
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative <> 'Other') THEN vertical_or_sub_initiative
         WHEN (customer_grp_value IS NULL AND vertical_or_sub_initiative = 'Other') THEN device_or_initiative
         ELSE customer_grp_value
       END AS customer,
       task_queue_id,
       service_order_id,
       aim_work_type,
       order_name,
       customer_grp_key
FROM (SELECT DISTINCT customer_grp_value,
             wtqd.vertical_or_sub_initiative,
             wtqd.device_or_initiative,
             wtqd.atat_transformation AS transformation,
             wtqd.task_queue_name,
             wtqd.task_queue_id_key AS task_queue_id,
             wtqd.queue_type,
             priority,
             dtq.service_order_id,
             fso.aim_work_type,
             fso.order_name,
             customer_grp_key::VARCHAR(1000) AS customer_grp_key
      FROM nvdev.wbr_task_queue_dim wtqd
        LEFT JOIN nvads.dim_task_queue_view dtq ON wtqd.task_queue_id_key = dtq.task_queue_id
        LEFT JOIN (SELECT order_id,
                          cust_grp_id,
                          aim_work_type,
                          order_name
                   FROM (SELECT DISTINCT order_id,
                                cust_grp_id,
                                aim_work_type,
                                order_name,
                                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_update_dt DESC) AS RNK
                         FROM nvads.fact_service_order)
                   WHERE RNK = 1) fso ON dtq.service_order_id = fso.order_id
        LEFT JOIN (SELECT customer_grp_key,
                          customer_grp_value,
                          cust_grp_id
                   FROM (SELECT DISTINCT customer_grp_key,
                                customer_grp_value,
                                cust_grp_id,
                                ROW_NUMBER() OVER (PARTITION BY CUST_GRP_ID ORDER BY insert_date DESC) AS RNK
                         FROM nvads.dim_customer_group)
                   WHERE RNK = 1) cust ON fso.cust_grp_id = cust.cust_grp_id
        LEFT JOIN (SELECT DISTINCT order_id,
                          order_create_dt,
                          order_update_dt,
                          order_name,
                          aim_work_type AS ADS_Service
                   FROM (SELECT DISTINCT order_id,
                                order_create_dt,
                                order_update_dt,
                                order_name,
                                aim_work_type,
                                ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_update_dt DESC) AS RNK
                         FROM nvads.fact_service_order
                         WHERE (LOWER(order_name) NOT LIKE '%%[closed]%%' OR LOWER(order_name) NOT LIKE '%%[closded]%%'))
                   WHERE RNK = 1) fso2 ON dtq.service_order_id = fso2.order_id
      UNION ALL
      SELECT DISTINCT customer_grp_value,
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
      FROM nvads.ramp_edx
      UNION ALL
      SELECT DISTINCT cycle_customer AS customer_grp_value,
             cycle_customer AS vertical_or_sub_initiative,
             cycle_type || ' ~ work state = ' || cycle_state AS transformation,
             cycle_customer AS device_or_initiative,
             cycle_name AS task_queue_name,
             cycle_id::TEXT AS task_queue_id,
             'RVD' AS queue_type,
             'RVD' AS priority,
             NULL AS service_order_id,
             NULL AS aim_work_type,
             NULL AS order_name,
             NULL AS customer_grp_key
      FROM nvrvd.tap_cycle) WITH NO SCHEMA BINDING;

GRANT SELECT ON public.queues_by_customer_entity TO nvort;

