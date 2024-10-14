select 
	creditor_id
	,amount_gbp as fds_exposure_current
from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1
LIMIT 1
