select 
	creditor_id
	,amount_gbp as fds_exposure_current
 ,3402624 as ticket_id

	/******** ACTION FIELDS ********/
	,'Test_Oct15' as process_name
	, TO_JSON_STRING(STRUCT(
        12443238293916 AS ticket_form_id,
        ARRAY<STRUCT<
            id INT64, 
            value INT64
        >>[
            -- Custom field entries
            STRUCT(15542500163356, 55555),  -- First custom field (category)
            STRUCT(15545615128732, 123)     -- Second custom field (fraud score)
        ] AS custom_fields,
        
        -- Adding comment body and comment public fields
        'This is a test\nPlease Ignore\n Editing comment' AS commentBody,
        false AS commentPublic
    )) AS ActionField_ZendeskUpdateTicket


from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
LIMIT 1
