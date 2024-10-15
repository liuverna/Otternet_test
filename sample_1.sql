select 
	creditor_id
	,amount_gbp as fds_exposure_current

	/******** ACTION FIELDS ********/
	,'Test_Oct15' as process_name
	,TO_JSON_STRING(STRUCT(
        'Credit - TEST_TEST_TEST ' || creditor_id AS subject,
        'Normal' AS priority,
        3285009 AS brand_id,
        360005611314 AS group_id,
        9724439852828 AS requester_id,
        5636997079964 AS ticket_form_id,
        'This is a test\nPlease Ignore' AS commentBody,
        false AS commentPublic,
        'credit__inactive_company' AS category
    )) AS ActionField_ZendeskCreateTicket


from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
LIMIT 2


