-- Hier wird die Tabelle visit_h aus dem Clickstream in die Tabelle visit_h in der Sandbox kopiert

Create or replace table brain-soso-dev.sandbox.visit_h as  SELECT visit_type, visit_h_sk, ot_bot
FROM `brain-clickstream-prd.datalake_v1_ov_lhotse_clickstream_datavault_optin.CS_VISIT_S_OPTIN`
WHERE event_timestamp_utc>='2024-03-04'
--LIMIT 1000