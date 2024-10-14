select 
	creditor_id
	,amount_gbp as fds_exposure_current

	/******** ACTION FIELDS ********/
	,'Test_Oct14' as process_name
	,true as query_log
	,true as run_actions
	,false as create_tickets
	,true as log_results
	,true as savelog

from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
LIMIT 2

