USE ROLE ACCOUNTADMIN;
ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

CREATE DATABASE IF NOT EXISTS CLAIMS_EXTRACTION_LAB;
CREATE SCHEMA IF NOT EXISTS CLAIMS_EXTRACTION_LAB.HOL;

CREATE OR REPLACE FILE FORMAT parquet_format TYPE = PARQUET;
CREATE OR REPLACE STAGE claims_data_stage FILE_FORMAT = parquet_format;

create or replace api integration RETRIEVAL_LAB_INTEGRATION
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/dschuler-phdata/claims-extraction-and-eval-hol.git')
    enabled = true
    allowed_authentication_secrets = all
;

CREATE OR REPLACE GIT REPOSITORY claims_extraction_repo
    API_INTEGRATION = RETRIEVAL_LAB_INTEGRATION
    ORIGIN = 'https://github.com/dschuler-phdata/claims-extraction-and-eval-hol.git'
;

ALTER GIT REPOSITORY claims_extraction_repo FETCH;

COPY FILES
    INTO @claims_data_stage
    FROM @claims_extraction_repo/branches/main/FimaNfipClaimsV2.parquet
;

CREATE OR REPLACE TABLE ADJUSTER_NOTES AS
SELECT
    $1:claim_id::INTEGER AS claim_id,
    $1:date_of_loss::DATE AS date_of_loss,
    $1:year_of_loss::INTEGER AS year_of_loss,
    $1:state::STRING AS state,
    $1:county_code::STRING AS county_code,
    $1:flood_zone::STRING AS flood_zone,
    $1:flood_event::STRING AS flood_event,
    $1:building_damage_amount::FLOAT AS building_damage_amount,
    $1:contents_damage_amount::FLOAT AS contents_damage_amount,
    $1:amount_paid_building::FLOAT AS amount_paid_building,
    $1:amount_paid_contents::FLOAT AS amount_paid_contents,
    $1:building_property_value::FLOAT AS building_property_value,
    $1:contents_property_value::FLOAT AS contents_property_value,
    $1:building_replacement_cost::FLOAT AS building_replacement_cost,
    $1:contents_replacement_cost::FLOAT AS contents_replacement_cost,
    $1:water_depth::STRING AS water_depth,
    $1:elevation_difference::STRING AS elevation_difference,
    $1:basement_type::STRING AS basement_type,
    $1:flood_characteristics::STRING AS flood_characteristics,
    $1:flood_water_duration::STRING AS flood_water_duration,
    $1:floodproofed::STRING AS floodproofed,
    $1:elevation_certificate::STRING AS elevation_certificate,
    $1:building_description_code::STRING AS building_description_code,
    $1:building_deductible_code::STRING AS building_deductible_code,
    $1:contents_deductible_code::STRING AS contents_deductible_code,
    $1:state_owned::STRING AS state_owned,
    $1:policy_count::STRING AS policy_count,
    $1:adjuster_notes::STRING AS adjuster_notes
FROM @claims_data_stage/claims_with_notes.snappy.parquet
    (FILE_FORMAT => 'parquet_format');

SELECT
    CASE
        WHEN COUNT(*) > 0 THEN 'Setup complete: ' || COUNT(*) || ' rows loaded into ADJUSTER_NOTES'
        ELSE 'ERROR: ADJUSTER_NOTES table is empty. Please check the data load.'
    END AS setup_status
FROM ADJUSTER_NOTES;