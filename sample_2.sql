select organisation_id
,name
,creditor_id
,geo
,merchant_type
,current_state
,current_revenue_account_type
,organisation_created_date
,true as ActionField_ZendeskCreateTicket
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_organisation` 
LIMIT 1
