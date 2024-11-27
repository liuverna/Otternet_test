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
,exposure as (
select 
	creditor_id
	,amount_gbp as fds_exposure_current
from `gc-prd-risk-prod-gdia.dbt_risk.d_fds_exposure`
qualify row_number() over (partition by creditor_id order by calculated_at_date desc) =1)


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
and calendar_date = current_date()-1
group by 1,2)


/******************************************************************************************************/
/******************************************    Payments    ********************************************/
/******************************************************************************************************/
,creditor_payments_temp as (select
	creditor_id
    ,sum(case when date(charge_date) between current_date() and current_date()+7 then amount_gbp else 0 end) as future_payments_7days

    ,sum(case when is_paid and date(charge_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_30d
    ,sum(case when is_charged_back  and date(charged_back_date) between current_date()-30 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_30d

    ,sum(case when is_paid and date(charge_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_payment_vol_last_90d
    ,sum(case when is_charged_back  and date(charged_back_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_chargeback_vol_last_90d
    ,sum(case when is_failed and date(failed_or_late_failure_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_failure_vol_last_90d
    ,sum(case when is_late_failure and date(failed_or_late_failure_date) between current_date()-90 and current_date()-1 then 1 else 0 end) as merchant_late_failure_vol_last_90d
    ,sum(case when is_refunded  and date(refund_created_at) between current_date()-90 and current_date()-1 then 1  else 0 end) as merchant_refund_vol_last_90d

    ,sum(case when is_paid and date(charge_date)  between current_date()-365   and current_date()-1    then amount_gbp  else 0 end) as merchant_payment_amt_gbp_last_365d

from `gc-prd-bi-pdata-prod-94e7.dbt_core_model.x_payments` 
where 
date(charge_date) between current_date()-365 and current_date()-1
or date(charged_back_date) between current_date()-90 and current_date()-1
or date(failed_or_late_failure_date) between current_date()-90 and current_date()-1
or date(refund_created_at) between current_date()-90 and current_date()-1

group by 1)


,creditor_payments as (
select
	creditor_id

	,future_payments_7days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_30d,merchant_payment_vol_last_30d) as cb_rate_30days

	,SAFE_DIVIDE(merchant_chargeback_vol_last_90d,merchant_payment_vol_last_90d) as cb_rate_90days
	,SAFE_DIVIDE(merchant_failure_vol_last_90d,merchant_payment_vol_last_90d) as failure_rate_90days
	,SAFE_DIVIDE(merchant_late_failure_vol_last_90d,merchant_payment_vol_last_90d) as late_failure_rate_90days
	,SAFE_DIVIDE(merchant_refund_vol_last_90d,merchant_payment_vol_last_90d) as refund_rate_90days

	,merchant_payment_amt_gbp_last_365d

	from creditor_payments_temp)

/******************************************************************************************************/
/******************************************    Existing Tickets    ********************************************/
/******************************************************************************************************/

,tickets as (SELECT *
FROM (
    SELECT
        dbt_zendesk_groups.name AS zendesk_group_name,
        dbt_zendesk_tickets.subject AS tickets_subject,
        dbt_zendesk_tickets.id AS ticket_id,
        (DATE(CASE WHEN dbt_zendesk_tickets.status = 'closed'
            THEN CAST(dbt_zendesk_tickets.updated_at AS TIMESTAMP)
            ELSE NULL
        END)) AS tickets_closed_at_date,
        dbt_zendesk_organizations.gc_organization_id AS organisation_id,
        (DATE(CAST(dbt_zendesk_ticket_metrics.created_at AS TIMESTAMP))) AS ticket_created_at,
        ROW_NUMBER() OVER (PARTITION BY dbt_zendesk_organizations.gc_organization_id ORDER BY dbt_zendesk_tickets.updated_at DESC) AS rowno
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
    WHERE 
        (dbt_zendesk_groups.name) = 'Credit'
) AS subquery 
WHERE rowno = 1)


-- ,tickets as (SELECT
--     dbt_zendesk_groups.name  AS dbt_zendesk_groups_group_name,
--     dbt_zendesk_tickets.subject  AS dbt_zendesk_tickets_subject,
--     dbt_zendesk_tickets.id  AS dbt_zendesk_tickets_id,
--         (DATE(CASE WHEN dbt_zendesk_tickets.status='closed'
--     THEN  cast(dbt_zendesk_tickets.updated_at as timestamp)
--     ELSE null
--     END)) AS dbt_zendesk_tickets_closed_at_date,
--     dbt_zendesk_organizations.gc_organization_id  AS dbt_zendesk_organizations_gc_organization_id,
--         (DATE(cast(dbt_zendesk_ticket_metrics.created_at as timestamp) )) AS dbt_zendesk_ticket_metrics_ticket_created_at_date,
--     ROW_NUMBER() OVER (PARTITION BY dbt_zendesk_organizations.gc_organization_id ORDER BY dbt_zendesk_tickets.updated_at DESC) AS rowno
-- FROM `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_tickets` AS dbt_zendesk_tickets
-- LEFT JOIN `gc-prd-data-sources-prod-aa9d.dbt_zendesk.zendesk_ticket_metrics_v1` AS dbt_zendesk_ticket_metrics ON dbt_zendesk_tickets.id = dbt_zendesk_ticket_metrics.ticket_id
-- LEFT JOIN `gc-prd-data-sources-prod-aa9d.dbt_zendesk.zendesk_groups_v1` AS dbt_zendesk_groups ON dbt_zendesk_tickets.group_id = dbt_zendesk_groups.id
-- LEFT JOIN `gc-prd-bi-pdata-prod-94e7.dbt_zendesk.zendesk_organizations` AS dbt_zendesk_organizations ON dbt_zendesk_tickets.organization_id = dbt_zendesk_organizations.id
-- WHERE (dbt_zendesk_groups.name ) = 'Credit' and rowno=1)

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

,opportunities as (SELECT *
FROM (
    SELECT
        opportunity_id,
        opportunity_name,
        opportunity_created_date,
        account_id,
        opportunity_type,
        commission_model,
        total_acv_gbp,
        total_tcv_gbp,
        contract_end_date,
        contract_start_date,
        SAFE_DIVIDE(DATE_DIFF(contract_end_date, contract_start_date, DAY), 365) AS contract_term,
        cast(concat(substr(cast(contract_end_date as string),1,4),'' ,substr(cast(contract_end_date as string),6,2)) as string) as end_date_yymm,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY opportunity_created_date DESC) AS rowno
    FROM
        `gc-prd-sales-prod-ae5e.dbt_sales_data_products.denorm_salesforce_opportunities_v1`
) AS opportunities
WHERE rowno = 1 )


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
  ,risk_org_form
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
   opportunities.* 
  ,risk_org_form_2.risk_check_form_id
  ,risk_org_form_2.risk_org_form
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
  from opportunities 
  left join risk_org_form_2 on opportunities.opportunity_id = risk_org_form_2.opportunity_id
  left join creditor_details on opportunities.account_id = creditor_details.account_id)

,data_merge as (
                  select 
                    a.*

                    ,round(b.fds_exposure_current,1) as fds_exposure_current

                    ,c.db_failure_score_current
                    ,c.db_failure_score_current_date
                    ,c.db_cash_and_liquid_assets
                    ,c.db_tangible_net_worth
                    ,c.db_current_ratio
                    ,c.db_latest_financial_statement_date

                    ,d.PD_score_latest
                    ,d.prediction_calendar_date

                    ,case when e.balance_amount_sum_gbp <0 then e.balance_amount_sum_gbp else 0 end as nb_balance_current
                    
                    ,round(f.merchant_payment_amt_gbp_last_365d,1) as merchant_payment_amt_gbp_last_365d
                    ,f.cb_rate_90days
                    ,f.failure_rate_90days
                    ,f.late_failure_rate_90days
                    ,f.refund_rate_90days


                    ,g.ticket_id
                    ,g.ticket_created_at
                    ,g.zendesk_group_name
                    ,g.tickets_closed_at_date
                    ,g.tickets_subject

                    ,h.company_number

        


                  from data_merge_0 as a
                  left join exposure   			      as b on a.creditor_id = b.creditor_id
                  left join db_failure            as c on a.creditor_id = c.creditor_id
                  left join PD_score	            as d on a.creditor_id = d.creditor_id
                  left join creditor_balances     as e on a.creditor_id = e.creditor_id
                  left join creditor_payments     as f on a.creditor_id = f.creditor_id
                  left join company_number        as h on a.creditor_id = h.creditor_id
                  left join tickets               as g on a.organisation_id = g.organisation_id      

)

/******************************************************************************************************/
/******************************************  Payload & Logic  *****************************************/
/******************************************************************************************************/
,payload as (
select * from data_merge
where DATE(contract_end_date) > current_date () and ACR_gbp >=60000  --DATE_ADD(CURRENT_DATE(), INTERVAL 105 DAY) and ACR_gbp >=60000
)
 

--  select count(account_id)
--  ,count (distinct account_id)
-- from creditor_details

/******************************************************************************************************/
/**************************************  Action Fields   **********************************************/
/******************************************************************************************************/
select * 

			,'credit_cra_contract_renewal' as process_name

			,TO_JSON_STRING(STRUCT(
        STRUCT(
            "high" AS priority, 
            3285009 as brand_id, 
            360005611314 as group_id, 
            9724439852828 as requester_id, 
            5636997079964 AS ticket_form_id,
						4451452073116 as assignee_id,

            ARRAY<STRUCT<
                id INT64, 
                value STRING
            >>[
                -- Custom field entries
                STRUCT(28480929, 'credit__acr_credit_check')  -- Category
                -- STRUCT(15542500163356, '12345')  -- Exposure
                -- STRUCT(15545615128732, '123')  -- Fraud score (uncomment if needed)

            ] AS custom_fields,

            -- Comment object
	 STRUCT(
				'**Merchant Details:**'
				|| '\n' || '**Creditor ID:** [' || COALESCE(creditor_id, 'N/A') || '](https://manage.gocardless.com/admin/creditors/' || COALESCE(creditor_id, 'N/A') || ')'
				|| '\n' || '**Organisation ID:** ' || COALESCE(organisation_id, 'N/A')
				|| '\n' || '**Merchant name:** ' || COALESCE(merchant_name, 'N/A')
				|| '\n' || '**Geo:** ' || COALESCE(geo, 'N/A')
				|| '\n' || '**MCC:** ' || COALESCE(merchant_category_code_description, 'N/A')
				|| '\n' || '**Payment provider:** ' || COALESCE(cast(is_payment_provider as string), 'N/A')
				|| '\n' || '**Account Type:** ' || COALESCE(account_type, 'N/A')
				|| '\n' || '**CS Managed:** ' || COALESCE(cast(is_cs_managed as string), 'N/A')
				|| '\n' || '**CS Manager Name:** ' || COALESCE(csm_owner_name, 'N/A')
        || '\n' || '**Company number:** ' || COALESCE(company_number, 'N/A')

				|| '\n\n' || '**Parent Information:**'
				|| '\n' || '**Parent ID:** ' || COALESCE(parent_account_id, 'N/A')
				|| '\n' || '**Parent Name:** ' || COALESCE(parent_account_name, 'N/A')

				|| '\n\n' || '**Risk Labels:**'
				|| '\n' || '**Risk Label:** ' || COALESCE(merchant_risk_label_description, 'N/A')
				|| '\n' || '**Risk Label Date:** ' || COALESCE(cast(date(most_recent_risk_label_created_at) as string), 'N/A')

				|| '\n\n' || '**Failure Score:**'
				|| '\n' || '**D&B Score:** ' || COALESCE(cast(db_failure_score_current as string), 'N/A')
				|| '\n' || '**D&B Score date:** ' || COALESCE(cast(db_failure_score_current_date as string), 'N/A')
				|| '\n' || '**Internal PD Score:** ' || COALESCE(cast(round(PD_score_latest,2) as string), 'N/A')
				|| '\n' || '**Internal PD Score date:** ' || COALESCE(CAST(prediction_calendar_date AS STRING), 'N/A')
        
        || '\n\n' || '**D&B Financials:**'
				|| '\n' || '**D&B Cash at bank:** ' || COALESCE(cast(db_cash_and_liquid_assets as string), 'N/A')
				|| '\n' || '**D&B Tangible net worth:** ' || COALESCE(cast(db_tangible_net_worth as string), 'N/A')
				|| '\n' || '**D&B current ratio:** ' || COALESCE(cast(round(db_current_ratio,2) as string), 'N/A')
				|| '\n' || '**D&B latest financial statements date:** ' || COALESCE(CAST(db_latest_financial_statement_date AS STRING), 'N/A')
         

        || '\n\n' || '**Negative Balance:**'
				|| '\n' || '**Current Negative Balance:** £' || COALESCE(CAST(nb_balance_current AS STRING FORMAT '999,999,999.0'), 'N/A')

				|| '\n\n' || '**Payment Information:**'
				|| '\n' || '**FDS Exposure:** £' || COALESCE(CAST(fds_exposure_current AS STRING FORMAT '999,999,999.0'), 'N/A')
				|| '\n' || '**Payments last 12m:** £' || COALESCE(CAST(merchant_payment_amt_gbp_last_365d AS STRING FORMAT '999,999,999.0'), 'N/A')
				|| '\n' || '**Chargeback rate (90days):** ' || COALESCE(CAST(cb_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Failure rate (90days):** ' || COALESCE(CAST(failure_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Late Failure rate (90days):** ' || COALESCE(CAST(late_failure_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'
				|| '\n' || '**Refund rate (90days):** ' || COALESCE(CAST(refund_rate_90days * 100 AS STRING FORMAT '999,999,999.00'), 'N/A') || '%'

        || '\n\n' || '**Opportunity Details:**'
				|| '\n' || '**Opportunity ID:** ' || COALESCE(cast(opportunity_id as string), 'N/A')
				|| '\n' || '**Contract end date:** ' || COALESCE(cast(contract_end_date as string), 'N/A')
				|| '\n' || '**Contract term:** ' || COALESCE(cast(round(contract_term,2) as string), 'N/A')
				|| '\n' || '**ACR:** ' || COALESCE(CAST(ACR_gbp AS STRING), 'N/A')			
        || '\n' || '**TCV:** ' || COALESCE(CAST(total_tcv_gbp AS STRING), 'N/A')			



		    || '\n\n' || '**Latest ticket created at:** ' || date(ticket_created_at)
		    || '\n' || '**Ticket link here:** [' || COALESCE(ticket_id, 0)  || '](https://gocardless.zendesk.com/agent/tickets/' || COALESCE(ticket_id, 0)  || ')'
        || '\n' || '**Last ticket closed date:** ' || date(tickets_closed_at_date) 
        --|| '\n' || '**Last credit review date:** ' || date(last_credit_review_date) 

				|| '\n\n' || '**Link to underwriter’s dashboard:** [Underwriter Dashboard](https://looker.gocardless.io/dashboards/3505?Organisation+ID=' || COALESCE(organisation_id, 'N/A') || '&Creditor+ID=&Company+Number=)'
				|| '\n\n\n' || 'Created by OtterNet'
		AS body,
              false AS public
            ) AS comment,

            -- Subject
            'Sales-led Contract Renewal - ' || merchant_name || ' - ' || creditor_id AS subject


        ) AS ticket
				)) AS ActionField_ZendeskCreateTicket


from payload limit 1
