with

/******************************************************************************************************/
/******************************************Creditor Details********************************************/
/******************************************************************************************************/
creditor_details as (
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
  ,a.is_creditor_frozen
  ,a.payout_enabled
  ,a.organisation_with_multiple_creditors
	,b.current_revenue_account_type as account_type
  ,b.current_state
  ,b.account_id
  ,b.parent_account_id
  ,b.parent_account_name
	,b.is_cs_managed
  ,b.csm_owner_name
  ,date(b.created_at) as organisation_signup_date
	,1 as var1
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor`  as a
left join dbt_core_model.d_organisation as b
on a.id = b.creditor_id
where not a.is_payment_provider)


/******************************************************************************************************/
/******************************************  FDS Exposure  ********************************************/
/******************************************************************************************************/
-- ,exposure as (
-- select 
-- 	creditor_id
-- 	,amount_gbp as fds_exposure_current
-- from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
-- qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1)

,exposure AS (
    SELECT 
        creditor_id,
        amount_gbp AS fds_exposure_current,
        calculated_at_date
    FROM 
        `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY creditor_id ORDER BY calculated_at_date DESC) = 1
),
previous_exposure AS (
    SELECT 
        creditor_id,
        amount_gbp AS fds_exposure_previous_year,
        calculated_at_date
    FROM 
        `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
    WHERE 
        calculated_at_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY creditor_id ORDER BY calculated_at_date DESC) = 1
)

, final_exposure as (SELECT 
    e.creditor_id,
    e.fds_exposure_current,
    p.fds_exposure_previous_year
FROM 
    exposure e
LEFT JOIN 
    previous_exposure p ON e.creditor_id = p.creditor_id)


/******************************************************************************************************/
/******************************************   D&B Scores   ********************************************/
/******************************************************************************************************/
,db_failure as (
select 
    creditor_id
    ,dnb_assessment.failure_score.national_percentile as db_failure_score_current
    ,latest_financials.overview.cash_and_liquid_assets AS db_cash_and_liquid_assets
    ,latest_financials.overview.tangible_net_worth AS db_tangible_net_worth
    ,latest_financials.overview.current_ratio AS db_current_ratio
    ,latest_financial_statement_date AS db_latest_financial_statement_date
    ,latest_financials.financial_statement_to_date as db_financial_statement_to_date
    ,date(retrieved_at) as db_failure_score_current_date
    ,row_number() over (partition by creditor_id order by retrieved_at desc) as rowno

from  `gc-prd-risk-prod-gdia.dun_bradstreet_reports.dun_bradstreet_report__4` 
where dnb_assessment.failure_score.national_percentile is not null
qualify rowno = 1)


/******************************************************************************************************/
/******************************************   PD  Scores   ********************************************/
/******************************************************************************************************/
,PD_score as (
select 
  creditor_id
  ,prediction_date
  ,date(concat(substr(prediction_date,1,4),"-",substr(prediction_date,5,2),"-",substr(prediction_date,7,2))) as prediction_calendar_date
  ,probability as PD_score_latest
from `gc-prd-credit-risk-dev-81b5.pd_model.probability_of_default_model_predictions_historic`
qualify row_number() over (partition by creditor_id order by prediction_date desc)= 1 )


/******************************************************************************************************/
/******************************************  NB Balances   ********************************************/
/******************************************************************************************************/
,creditor_balances as (
select
	 owner_id as creditor_id 
	,calendar_date 
	,sum(balance_amount_sum_gbp) as balance_amount_sum_gbp
from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.scd_abacus_available_merchant_funds_daily`
where name = 'available_merchant_funds'
and calendar_date >= current_date()-31 and calendar_date <=current_date()
group by 1,2)

,creditor_balances_2 as (
   SELECT 
    creditor_id,
    calendar_date,
    balance_amount_sum_gbp,
    CASE 
      WHEN balance_amount_sum_gbp <= -10000 and calendar_date >= current_date()- 8  and calendar_date <= current_date() THEN '>10k'
      WHEN balance_amount_sum_gbp >= -10000 AND balance_amount_sum_gbp <= -5000 and calendar_date >= current_date()- 31 and calendar_date <= current_date() THEN '5k to 10k'
      WHEN balance_amount_sum_gbp >= -5000  and balance_amount_sum_gbp <0 THEN '<5k'
      ELSE 'no_nb'
    END AS nb_category
  FROM creditor_balances
)

,creditor_balances_3 as (
  SELECT 
  creditor_id,
  nb_category,
  count(*) as days_nb
from creditor_balances_2
group by 1,2
)

,creditor_balances_4 as (
  SELECT 
  *
 from creditor_balances_3
where (nb_category = '>10k' and days_nb = 8) or (nb_category = '5k to 10k' and days_nb = 31)
)
--just checking if the merchant has NB limit, regardless the currency
,nb_limit as (select
	organisation_id
	,creditor_id
	--,currency as nb_limit_currency
	,balance_limit as nb_limit_amount
	,date(updated_at) as nb_limit_updated_at
--from `gc-prd-bi-pdata-prod-94e7.dbt_gc_paysvc_live.negative_balance_limits`
from `gc-prd-data-sources-prod-aa9d.dbt_payments_service.negative_balance_limits_v1`
qualify row_number() over (partition by creditor_id order by updated_at DESC) =1
order by 1,2,3)

/******************************************************************************************************/
/******************************************  SDS ************************************************/
/******************************************************************************************************/
,sds as (select 
    d.organisation_id
   ,d.name as organisation_name
   ,a.creditor_id
   ,c.name as creditor_name
   ,a.scheme
   ,b.to_state
   ,date(b.created_at) as created_date
   ,a.id as sds_id
from `gc-prd-raw-pdata-prod-7806.gc_paysvc_live.same_day_settlement_creditors` as a 
left join `gc-prd-raw-pdata-prod-7806.gc_paysvc_live.same_day_settlement_creditor_transitions` as b 
   on a.id=b.same_day_settlement_creditor_id
left join `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_creditor` as c   
   on a.creditor_id=c.id
left join `gc-prd-bi-pdata-prod-94e7.dbt_core_model.d_organisation` as d
   on c.organisation_id=d.organisation_id
where b.most_recent
--and b.to_state = "activated"
order by a.created_at desc)

/******************************************************************************************************/
/******************************************    Payments    ********************************************/
/******************************************************************************************************/
,creditor_payments_temp as (select
	creditor_id
    ,sum(case when date(last_charge_date) between current_date() and current_date()+7 then amount_gbp else 0 end) as future_payments_7days

    ,sum(case when is_paid and date(last_charge_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_30d
    ,sum(case when is_charged_back  and date(last_charged_back_at) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_30d

    ,sum(case when is_paid and date(last_charge_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_90d
    ,sum(case when is_paid and date(last_charge_date) between current_date()-90 and current_date()-1 then amount_gbp else 0 end) as merchant_payment_value_last_90d

    ,sum(case when is_charged_back  and date(last_charged_back_at) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_90d
    ,sum(case when is_charged_back  and date(last_charged_back_at) between current_date()-90 and current_date()-1 then amount_gbp else 0 end) as merchant_chargeback_value_last_90d

    ,sum(case when is_failed_or_late_failed and date(last_failed_at) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_failure_vol_last_90d
    ,sum(case when is_failed_or_late_failed and date(last_late_failure_at) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_late_failure_vol_last_90d
    ,sum(case when date(last_refund_created_at) between current_date()-90 and current_date()-1 then 1  else 0 end) as merchant_refund_vol_last_90d

    ,sum(case when is_paid and date(last_charge_date)  between current_date()-365   and current_date()-1    then amount_gbp  else 0 end) as merchant_payment_amt_gbp_last_365d
    ,SUM(CASE WHEN is_paid AND DATE(last_charge_date) BETWEEN CURRENT_DATE() - 730 AND CURRENT_DATE() - 366 THEN amount_gbp ELSE 0 END) AS merchant_payment_amt_gbp_previous_year

from `gc-prd-payment-proc-prod-6639.dbt_payment_processing_data_products.report_payments_v1`
where 
date(last_charge_date) between current_date()-365 and current_date()-1
or date(last_charged_back_at) between current_date()-90 and current_date()-1
or date(last_failed_or_late_failure_at) between current_date()-90 and current_date()-1
or date(last_refund_created_at) between current_date()-90 and current_date()-1

group by 1)


,creditor_payments as (
select
	creditor_id

	,future_payments_7days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_30d,merchant_payment_vol_last_30d) as cb_rate_30days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_90d,merchant_payment_vol_last_90d) as cb_vol_rate_90days
	,SAFE_DIVIDE(merchant_chargeback_value_last_90d,merchant_payment_value_last_90d) as cb_value_rate_90days

	,SAFE_DIVIDE(merchant_failure_vol_last_90d,merchant_payment_vol_last_90d) as failure_rate_90days
	,SAFE_DIVIDE(merchant_late_failure_vol_last_90d,merchant_payment_vol_last_90d) as late_failure_rate_90days
	,SAFE_DIVIDE(merchant_refund_vol_last_90d,merchant_payment_vol_last_90d) as refund_rate_90days

	,merchant_payment_amt_gbp_last_365d
  ,merchant_payment_amt_gbp_previous_year

	from creditor_payments_temp)


/******************************************************************************************************/
/******************************************    Existing Tickets    ********************************************/
/******************************************************************************************************/

,tickets as (SELECT
dbt_zendesk_groups.name AS zendesk_group_name,
dbt_zendesk_tickets.subject AS tickets_subject,
dbt_zendesk_tickets.id AS ticket_id,
dbt_zendesk_tickets.ticket_category,
(DATE(CASE WHEN dbt_zendesk_tickets.status = 'closed'
            THEN CAST(dbt_zendesk_tickets.updated_at AS TIMESTAMP)
            ELSE NULL END)) AS tickets_closed_at_date,
dbt_zendesk_tickets.updated_at,
coalesce(REGEXP_EXTRACT(dbt_zendesk_ticket_comments.body , r'\b(OR[A-Z0-9]{12})\b'),
         REGEXP_EXTRACT(dbt_zendesk_tickets.description, r'\b(OR[A-Z0-9]{12})\b'),
         REGEXP_EXTRACT(dbt_zendesk_tickets.subject, r'\b(OR[A-Z0-9]{12})\b'),
         dbt_zendesk_organizations.gc_organization_id  ) as organisation_id,
(DATE(CAST(dbt_zendesk_ticket_metrics.created_at AS TIMESTAMP))) AS ticket_created_at,
--ROW_NUMBER() OVER (PARTITION BY organisation_id ORDER BY dbt_zendesk_tickets.updated_at DESC) AS rowno

FROM 
        `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_tickets` AS dbt_zendesk_tickets
    LEFT JOIN 
        `gc-prd-data-sources-prod-aa9d.dbt_zendesk.zendesk_ticket_metrics_v1` AS dbt_zendesk_ticket_metrics 
        ON dbt_zendesk_tickets.id = dbt_zendesk_ticket_metrics.ticket_id
    LEFT JOIN 
        `gc-prd-data-sources-prod-aa9d.dbt_zendesk.zendesk_groups_v1` AS dbt_zendesk_groups 
        ON dbt_zendesk_tickets.group_id = dbt_zendesk_groups.id
    LEFT JOIN 
        `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_organizations` AS dbt_zendesk_organizations 
        ON dbt_zendesk_tickets.organization_id = dbt_zendesk_organizations.id
    LEFT JOIN `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_ticket_comments` AS dbt_zendesk_ticket_comments ON dbt_zendesk_tickets.id = dbt_zendesk_ticket_comments.ticket_id
    WHERE 
         dbt_zendesk_groups.name = 'Credit' and dbt_zendesk_tickets.ticket_category in ('credit__acr_credit_check', 'credit__hrob__credit_review_not_required_' ,'credit__monitoring_cb',
'credit__monitoring_fs', 'credit__monitoring_nb' ,'credit__monitoring_rr' ,'risk__hrob','credit__monitoring_','credit__monitoring__no_response'))


, tickets_2 as( SELECT * 
FROM(
     SELECT *,
     ROW_NUMBER() OVER (PARTITION BY organisation_id ORDER BY updated_at DESC) AS rowno
FROM tickets) AS subquery
WHERE rowno = 1
)

,tickets_decision as (select 
ticket_id
,ticket_field_title
,ticket_field_value 
FROM `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_ticket_fields` 
where ticket_field_title in ("Credit decision","Credit Condition(s) status"))

, tickets_decision_2 as (select 
ticket_id
,max(case when ticket_field_title = "Credit decision" then ticket_field_value end) as credit_decision
,max(case when ticket_field_title = "Credit Condition(s) status" then ticket_field_value end) as credit_condition_status
from tickets_decision
group by 1)

,zendesk_tickets as (select
  a.*
  ,b.credit_decision
  ,b.credit_condition_status
  from tickets_2 as a left join tickets_decision_2 as b
  on a.ticket_id = b.ticket_id
)


/******************************************************************************************************/
/******************************************   Company Number    ********************************************/
/******************************************************************************************************/

,company_number as (select
creditor_id
,company_number
from `gc-prd-raw-pdata-prod-7806.gc_paysvc_live.company_details`)


/******************************************************************************************************/
/******************************************   Opportunity    ********************************************/
/******************************************************************************************************/

, RankedOpportunities AS (
  SELECT
    account_id,
    opportunity_id,
    opportunity_name,
    previous_opportunity_id,
    contract_end_date,
    opportunity_created_at,
    opportunity_type,
    commission_model,
    opportunity_stage_name,
    total_acv_gbp,
    total_tcv_gbp,
    ROW_NUMBER() OVER (
      PARTITION BY account_id, previous_opportunity_id
      ORDER BY TIMESTAMP(contract_end_date) DESC, opportunity_created_at DESC
    ) AS rank
  FROM
    `gc-prd-sales-prod-ae5e.dbt_sales_data_products.report_salesforce_opportunities_v1`
    WHERE opportunity_stage_name = '7. Closed Won' and DATE(contract_end_date) > current_date()
),
FilteredOpportunities AS (
  SELECT
    account_id,
    opportunity_id,
    opportunity_name,
    previous_opportunity_id,
    contract_end_date,
    opportunity_created_at,
    opportunity_type,
    commission_model,
    opportunity_stage_name,
    total_acv_gbp,
    total_tcv_gbp,
    rank
  FROM
    RankedOpportunities
  WHERE
    rank = 1 OR previous_opportunity_id IS NULL
)
,FilteredOpportunities2 as (SELECT
   account_id,
    opportunity_id,
    opportunity_name,
    previous_opportunity_id,
    contract_end_date,
    opportunity_created_at,
    opportunity_type,
    commission_model,
    opportunity_stage_name,
    total_acv_gbp,
    total_tcv_gbp
FROM
  FilteredOpportunities
WHERE
  NOT EXISTS (
    SELECT 1
    FROM FilteredOpportunities AS sub
    WHERE sub.account_id = FilteredOpportunities.account_id
      AND sub.previous_opportunity_id = FilteredOpportunities.opportunity_id
  )
)


/******************************************************************************************************/
/******************************************   Risk Org Check Form    ********************************************/
/******************************************************************************************************/
, risk_org_form as (
  select *
from `gc-prd-data-sources-prod-aa9d.dbt_salesforce.salesforce_risk_check_forms_v1`
qualify row_number() over (partition by opportunity_id order by created_date desc)=1 )

, risk_org_form_2 as (
select
  id as risk_check_form_id
  --,risk_org_form
  ,name as ROC_ID
  ,opportunity_id
  ,created_date as risk_form_created_date
  ,refunds as refunds_request
  ,refunds_approval
  ,limit_raise as limit_raise_request
  ,limit_raise_approval
  ,year_1_to_3_ratio
  ,acr_corporate_currency as ACR_gbp
  ,high_risk_vertical
  ,high_risk_approval
  ,credit_risk
  ,credit_risk_outcome
  ,conditions_met_risk
  ,approval_status
  ,escalated_deal
  ,form_url
from risk_org_form)

----------------------------------------------------------------------------
--Data Merge
----------------------------------------------------------------------------
,data_merge_0 as (select 
   FilteredOpportunities2.* 
  ,risk_org_form_2.risk_check_form_id
  --,risk_org_form_2.risk_org_form
  ,risk_org_form_2.ROC_ID
  ,risk_org_form_2.risk_form_created_date
  ,risk_org_form_2.refunds_request
  ,risk_org_form_2.refunds_approval
  ,risk_org_form_2.limit_raise_request
  ,risk_org_form_2.limit_raise_approval
  ,risk_org_form_2.year_1_to_3_ratio
  ,risk_org_form_2.ACR_gbp
  ,risk_org_form_2.high_risk_vertical
  ,risk_org_form_2.high_risk_approval
  ,risk_org_form_2.credit_risk
  ,risk_org_form_2.credit_risk_outcome
  ,risk_org_form_2.conditions_met_risk
  ,risk_org_form_2.approval_status
  ,risk_org_form_2.escalated_deal
  ,risk_org_form_2.form_url
  ,creditor_details.creditor_id
  ,creditor_details.organisation_id
  ,creditor_details.merchant_name
  ,creditor_details.parent_account_id
  ,creditor_details.parent_account_name
  ,creditor_details.geo
  ,creditor_details.merchant_category_code
  ,creditor_details.merchant_category_code_description
  ,creditor_details.current_state
  ,creditor_details.merchant_risk_label
  ,creditor_details.merchant_risk_label_description
  ,date(creditor_details.most_recent_risk_label_created_at) as most_recent_risk_label_created_at
  ,creditor_details.organisation_signup_date
  ,creditor_details.is_payment_provider
  ,creditor_details.account_type
  ,creditor_details.is_cs_managed
  ,creditor_details.csm_owner_name
  ,creditor_details.insolvency_flag
  ,creditor_details.is_account_closed
  ,creditor_details.is_creditor_frozen
  ,creditor_details.payout_enabled
  from FilteredOpportunities2 
  left join risk_org_form_2 on FilteredOpportunities2.opportunity_id = risk_org_form_2.opportunity_id
  left join creditor_details on FilteredOpportunities2.account_id = creditor_details.account_id)


,data_merge as (
                  select 
                    a.*

                    ,round(b.fds_exposure_current,1) as fds_exposure_current
                    ,round(b.fds_exposure_previous_year,1) as fds_exposure_previous_year

                    ,c.db_failure_score_current
                    ,c.db_failure_score_current_date
                    ,c.db_cash_and_liquid_assets
                    ,c.db_tangible_net_worth
                    ,c.db_current_ratio
                    ,c.db_latest_financial_statement_date
                    ,c.db_financial_statement_to_date

                    ,d.PD_score_latest
                    ,d.prediction_calendar_date

                    --,case when e.balance_amount_sum_gbp <0 then e.balance_amount_sum_gbp else 0 end as nb_balance_current
                    ,e.nb_category
                    ,e.days_nb

                    ,round(f.merchant_payment_amt_gbp_last_365d,1) as merchant_payment_amt_gbp_last_365d
                    ,round(f.merchant_payment_amt_gbp_previous_year,1) as merchant_payment_amt_gbp_previous_year
                    ,f.cb_vol_rate_90days
                    ,f.cb_value_rate_90days
                    ,f.failure_rate_90days
                    ,f.late_failure_rate_90days
                    ,f.refund_rate_90days


                    ,g.ticket_id
                    ,g.ticket_created_at
                    ,g.zendesk_group_name
                    ,g.tickets_closed_at_date
                    ,g.tickets_subject
                    ,g.ticket_category
                    ,g.credit_decision
                    ,g.credit_condition_status

                    ,h.company_number

                    --,i.nb_limit_currency
                    ,i.nb_limit_amount

                    ,j.sds_id

        


                  from data_merge_0 as a
                  left join final_exposure   			      as b on a.creditor_id = b.creditor_id
                  left join db_failure            as c on a.creditor_id = c.creditor_id
                  left join PD_score	            as d on a.creditor_id = d.creditor_id
                  left join creditor_balances_4     as e on a.creditor_id = e.creditor_id
                  left join creditor_payments     as f on a.creditor_id = f.creditor_id
                  left join company_number        as h on a.creditor_id = h.creditor_id
                  left join zendesk_tickets               as g on a.organisation_id = g.organisation_id   
                  left join nb_limit              as i on a.creditor_id = i.creditor_id   
                  left join sds                   as j on a.creditor_id = j.creditor_id 

)


, policy_filters as (select *,
case when db_failure_score_current >50 and db_failure_score_current_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH) then TRUE else FALSE end as flag_failure_score,
case when credit_decision = 'Approved (no conditions)' and ticket_category in ('credit__acr_credit_check' ,'credit__hrob__credit_review_not_required_', 'credit__monitoring_cb',
'credit__monitoring_fs', 'credit__monitoring_nb', 'credit__monitoring_rr', 'risk__hrob','credit__monitoring_','credit__monitoring__no_response') and tickets_closed_at_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH) then TRUE else FALSE end as flag_last_credit_approval,
--case when approval_status = 'Approved' then TRUE else FALSE end as flag_rocf_approval,
CASE 
        WHEN nb_category = '>10k' 
             AND days_nb = 8 
             AND (nb_limit_amount IS NULL OR nb_limit_amount = 0) 
             AND sds_id IS NULL THEN FALSE
        ELSE TRUE 
    END AS flag_nb_10k,
CASE 
        WHEN nb_category = '5k to 10k' 
             AND days_nb = 31 
             AND (nb_limit_amount IS NULL OR nb_limit_amount = 0) 
             AND sds_id IS NULL THEN FALSE
        ELSE TRUE 
    END AS flag_nb_5k_10k,
    
case when cb_vol_rate_90days <0.35 and cb_value_rate_90days < 0.35 then TRUE else FALSE end as flag_cb_rate,
case when (fds_exposure_current > 0.25 * fds_exposure_previous_year) and fds_exposure_current >= 250000 then FALSE else TRUE end as flag_exposure_increase,
CASE 
        WHEN merchant_payment_amt_gbp_previous_year > 0 THEN 
            CASE 
                WHEN (merchant_payment_amt_gbp_last_365d / merchant_payment_amt_gbp_previous_year) < 0.95 THEN FALSE
                ELSE TRUE
            END
        ELSE TRUE
    END AS flag_gc_yoy_growth,
case when DATE(db_financial_statement_to_date) >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 21 MONTH), MONTH) AND DATE(db_financial_statement_to_date) <= CURRENT_DATE()then TRUE else FALSE end as flag_db_financial_statement,
case when db_cash_and_liquid_assets >= 0.5 * fds_exposure_current then TRUE else FALSE end as flag_cash_fds,
case when db_tangible_net_worth >0 then TRUE else FALSE end as flag_net_worth,
case when db_current_ratio > 1 then TRUE else FALSE end as flag_current_ratio,
case when is_account_closed is true then FALSE else TRUE end as flag_account_closed,
case when is_creditor_frozen is true then FALSE else TRUE end as flag_account_frozen,
case when current_state = 'active' then TRUE else FALSE end as flag_current_state,
case when ACR_gbp <= 60000 then TRUE else FALSE end as flag_acr,
case when payout_enabled is true then TRUE else FALSE end as flag_payout_enabled,
case when merchant_risk_label_description is null then TRUE else FALSE end as flag_risk_label

from data_merge
)

, policy_rules as (SELECT 
    *,
    CASE 
        WHEN flag_risk_label = TRUE --and flag_acr = TRUE
         AND ( 
            (CASE WHEN flag_failure_score = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_last_credit_approval = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_nb_10k = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_nb_5k_10k = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_cb_rate = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_exposure_increase = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_gc_yoy_growth = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_db_financial_statement = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_cash_fds = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_net_worth = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_current_ratio = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_account_closed = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_account_frozen = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_current_state = TRUE THEN 1 ELSE 0 END) +
            (CASE WHEN flag_payout_enabled = TRUE THEN 1 ELSE 0 END) 
        ) >= 11 
        THEN TRUE
        ELSE FALSE 
    END AS auto_approve
FROM 
    policy_filters)

  select * from  policy_rules where date(contract_end_date) = (current_date() + 105) and ACR_gbp >= 60000
