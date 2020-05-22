SELECT transformation, parent_work_type, work_type, workflow_app, base_verify_workflow_app, core_non_core_workflow_app FROM nvads.RAMP_edx r
left join 
(select distinct parent_work_type, work_type, workflow_app, base_verify_workflow_app, core_non_core_workflow_app
from nvads.ads_workflows_categorical_mappings) d
on r.transformation=d.workflow_app
Where 1=1
and cast(actual_year as int) = 2020
and cast(actual_month as int) = 4
