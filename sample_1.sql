select 
	creditor_id
	,amount_gbp as fds_exposure_current

	/******** ACTION FIELDS ********/
	-- ,'Test_Oct14' as process_name
	,false as create_ticket


from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
LIMIT 2

