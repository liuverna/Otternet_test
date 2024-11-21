with payload as (
select
	a.id as creditor_id
	,a.organisation_id
	,a.name as merchant_name
	,a.geo
	,a.merchant_category_code
	,a.merchant_category_code_description
	,a.creditor_risk_label_parent as merchant_risk_label
	,a.creditor_risk_label_detail as merchant_risk_label_description
	,a.most_recent_risk_label_created_at
	,case when a.creditor_risk_label_detail in ("in_administration","insolvency","restructuring","dissolved","liquidation","inactivity") then true else false end as insolvency_flag
	,a.creditor_created_date 
	,a.is_account_closed
	,a.is_payment_provider
  ,a.organisation_with_multiple_creditors
	,b.current_revenue_account_type as account_type
  ,b.current_state
  ,b.parent_account_id
  ,b.parent_account_name
	,1 as var1
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as a
left join dbt_core_model.d_organisation as b
on a.organisation_id = b.organisation_id
where creditor_id = 'CR0000Q4R4BP5D')


select * 

			,'test_nov21' as process_name

			,TO_JSON_STRING(STRUCT(
        STRUCT(
            "high" AS priority, 
            5636997079964 AS ticket_form_id,

            -- Comment object
	 STRUCT(
				'**Merchant Name:**' || merchant_name || 'This is a test'
				|| '\n\n\n' || 'Created by OtterNet'
		      AS body,
        false AS public
            ) AS comment,

            -- Subject
            'TEST TEST TETSe - 'AS subject


        ) AS ticket
				)) AS ActionField_ZendeskCreateTicket


from payload

