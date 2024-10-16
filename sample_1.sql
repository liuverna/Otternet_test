select 
	creditor_id
	,amount_gbp as fds_exposure_current

	/******** ACTION FIELDS ********/
	,'Test_Oct16' as process_name
	,true as createriskalertonadmin
	,12345 as closedriskalertid


	
  	,TO_JSON_STRING(STRUCT(
        STRUCT(
            "normal" AS priority, 
            "brand_id": 3285009, -- Uncomment if you want to include
            "group_id": 12439677670684, -- Uncomment if needed
            "requester_id": 9724439852828, -- Uncomment if needed
            12443238293916 AS ticket_form_id,
            ARRAY<STRUCT<
                id INT64, 
                value INT64
            >>[
                -- Custom field entries
                -- Uncomment if needed
                STRUCT(28480929, 'fraud__alerts_low'),  -- Category
                STRUCT(15542500163356, 12345)  -- Exposure
                STRUCT(15545615128732, 123)  -- Fraud score (uncomment if needed)
            ] AS custom_fields,

            -- Comment object
            STRUCT(
                'This is s test do not Panic!!!' AS body,
                false AS public
            ) AS comment,

            -- Subject
            'This is s test do not Panic!!! 1' AS subject
        ) AS ticket
    )) AS ActionField_ZendeskCreateTicket

from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
LIMIT 1
