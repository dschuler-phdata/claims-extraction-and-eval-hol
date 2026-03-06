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

CREATE OR REPLACE TABLE FIMA_CLAIMS AS
SELECT
    $1:CLAIM_ID::STRING AS claim_id,
    $1:DATE_OF_LOSS::DATE AS date_of_loss,
    $1:YEAR_OF_LOSS::NUMBER(4,0) AS year_of_loss,
    $1:STATE::STRING AS state,
    $1:COUNTY_CODE::STRING AS county_code,
    $1:FLOOD_ZONE::STRING AS flood_zone,
    $1:FLOOD_EVENT::STRING AS flood_event,
    $1:BUILDING_DAMAGE_AMOUNT::FLOAT AS building_damage_amount,
    $1:CONTENTS_DAMAGE_AMOUNT::FLOAT AS contents_damage_amount,
    $1:AMOUNT_PAID_BUILDING::FLOAT AS amount_paid_building,
    $1:AMOUNT_PAID_CONTENTS::FLOAT AS amount_paid_contents,
    $1:BUILDING_PROPERTY_VALUE::FLOAT AS building_property_value,
    $1:CONTENTS_PROPERTY_VALUE::FLOAT AS contents_property_value,
    $1:BUILDING_REPLACEMENT_COST::FLOAT AS building_replacement_cost,
    $1:CONTENTS_REPLACEMENT_COST::FLOAT AS contents_replacement_cost,
    $1:WATER_DEPTH::STRING AS water_depth,
    $1:ELEVATION_DIFFERENCE::STRING AS elevation_difference,
    $1:BASEMENT_TYPE::STRING AS basement_type,
    $1:FLOOD_CHARACTERISTICS::STRING AS flood_characteristics,
    $1:FLOOD_WATER_DURATION::STRING AS flood_water_duration,
    $1:FLOODPROOFED::STRING AS floodproofed,
    $1:ELEVATION_CERTIFICATE::STRING AS elevation_certificate,
    $1:BUILDING_DESCRIPTION_CODE::STRING AS building_description_code,
    $1:BUILDING_DEDUCTIBLE_CODE::STRING AS building_deductible_code,
    $1:CONTENTS_DEDUCTIBLE_CODE::STRING AS contents_deductible_code,
    $1:STATE_OWNED::STRING AS state_owned,
    $1:POLICY_COUNT::STRING AS policy_count,
    $1:ADJUSTER_NOTES::STRING AS adjuster_notes
FROM @claims_data_stage/FimaNfipClaimsV2.parquet
    (FILE_FORMAT => 'parquet_format');

SELECT
    CASE
        WHEN COUNT(*) > 0 THEN 'Setup complete: ' || COUNT(*) || ' rows loaded into FIMA_CLAIMS'
        ELSE 'ERROR: FIMA_CLAIMS table is empty. Please check the data load.'
    END AS setup_status
FROM FIMA_CLAIMS;