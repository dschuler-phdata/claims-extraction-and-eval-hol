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

-- SPROC creation for extraction lab

CREATE OR REPLACE PROCEDURE CLAIMS_EXTRACTION_LAB.HOL.EXTRACT_CLAIMS_PARALLEL(
    BATCH_SIZE    INT,
    MAX_WORKERS   INT,
    MAX_RETRIES   INT,
    STALE_MINUTES INT
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pydantic')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import json
import time
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel, Field, ValidationError, field_validator


# ── Schema ────────────────────────────────────────────────────────────────────

class ClaimExtractionValidated(BaseModel):
    claim_id:                   Optional[str]   = Field(None, description="The claim identifier or number")
    date_of_loss:               Optional[str]   = Field(None, description="Date when the flood damage occurred (YYYY-MM-DD format)")
    state:                      Optional[str]   = Field(None, description="Two-letter US state code")
    county_code:                Optional[str]   = Field(None, description="County FIPS code")
    flood_zone:                 Optional[str]   = Field(None, description="FEMA flood zone designation (e.g., AE, V, X, A)")
    flood_event:                Optional[str]   = Field(None, description="Name of the flood event or disaster")
    water_depth_inches:         Optional[int]   = Field(None, description="Depth of flood water in inches")
    flood_duration_hours:       Optional[int]   = Field(None, description="How long flood water remained, in hours")
    building_damage_amount:     Optional[float] = Field(None, description="Dollar amount of building/structural damage")
    contents_damage_amount:     Optional[float] = Field(None, description="Dollar amount of contents/personal property damage")
    building_property_value:    Optional[float] = Field(None, description="Assessed or estimated value of the building")
    contents_property_value:    Optional[float] = Field(None, description="Assessed or estimated value of contents")
    structural_damage_severity: Optional[str]   = Field(None, description="Overall severity: minor, moderate, severe, or catastrophic")
    amount_paid_building:       Optional[float] = Field(None, description="Insurance payout amount for building damage")
    amount_paid_contents:       Optional[float] = Field(None, description="Insurance payout amount for contents damage")
    basement_type:              Optional[int]   = Field(None, description="Basement/enclosure/crawlspace type code (0-4)")
    floodproofed:               Optional[bool]  = Field(None, description="Whether the building has floodproofing measures")
    mold_present:               Optional[bool]  = Field(None, description="Whether mold was observed during inspection")
    number_of_stories:          Optional[int]   = Field(None, description="Number of stories in the building")
    elevation_certificate:      Optional[bool]  = Field(None, description="Whether an elevation certificate exists for the property")
    building_description_code:  Optional[str]   = Field(None, description="Building description type code")

    @field_validator("state")
    @classmethod
    def validate_state(cls, v):
        if v is not None and (len(v) != 2 or not v.isalpha()):
            raise ValueError(f"State must be 2 letters, got: {v}")
        return v.upper() if v else v

    @field_validator("water_depth_inches")
    @classmethod
    def validate_water_depth(cls, v):
        if v is not None and v < 0:
            raise ValueError(f"Water depth cannot be negative, got: {v}")
        return v

    @field_validator("building_damage_amount", "contents_damage_amount")
    @classmethod
    def validate_damage_amount(cls, v):
        if v is not None and v < 0:
            raise ValueError(f"Damage amount cannot be negative, got: {v}")
        return v

    @field_validator("structural_damage_severity")
    @classmethod
    def validate_severity(cls, v):
        if v is not None:
            allowed = {"minor", "moderate", "severe", "catastrophic"}
            if v.lower() not in allowed:
                raise ValueError(f"Severity must be one of {allowed}, got: {v}")
            return v.lower()
        return v

    @field_validator("number_of_stories")
    @classmethod
    def validate_stories(cls, v):
        if v is not None and (v < 1 or v > 5):
            raise ValueError(f"Stories must be 1-5, got: {v}")
        return v


FIELD_GROUPS = {
    "claim_location": {
        "label": "Claim & Location",
        "fields": ["claim_id", "date_of_loss", "state", "county_code", "flood_zone"],
    },
    "flood_damage": {
        "label": "Flood Event & Damage",
        "fields": [
            "flood_event", "water_depth_inches", "flood_duration_hours",
            "building_damage_amount", "contents_damage_amount",
            "building_property_value", "contents_property_value",
            "structural_damage_severity", "amount_paid_building", "amount_paid_contents",
        ],
    },
    "property_details": {
        "label": "Property Details",
        "fields": [
            "basement_type", "floodproofed", "mold_present",
            "number_of_stories", "elevation_certificate", "building_description_code",
        ],
    },
}


# ── Schema helpers ────────────────────────────────────────────────────────────

def simplify_schema_for_cortex(schema):
    simplified = {"type": "object", "properties": {}}
    for field_name, field_def in schema.get("properties", {}).items():
        prop = {}
        if "anyOf" in field_def:
            types = [opt["type"] for opt in field_def["anyOf"] if "type" in opt]
            prop["type"] = types if len(types) > 1 else types[0]
        elif "type" in field_def:
            prop["type"] = field_def["type"]
        if "description" in field_def:
            prop["description"] = field_def["description"]
        simplified["properties"][field_name] = prop
    return simplified


def schema_for_group(group_key):
    full_schema = ClaimExtractionValidated.model_json_schema()
    group_fields = FIELD_GROUPS[group_key]["fields"]
    full_simplified = simplify_schema_for_cortex(full_schema)
    return {
        "type": "object",
        "properties": {
            k: v for k, v in full_simplified["properties"].items()
            if k in group_fields
        }
    }


# ── Per-group extraction with validate/fix loop ───────────────────────────────

def extract_group(session, adjuster_notes, group_key, max_retries):
    group_info   = FIELD_GROUPS[group_key]
    fields       = group_info["fields"]
    label        = group_info["label"]
    cortex_calls = 0

    field_descriptions = "\\n".join(
        f"- {f}: {ClaimExtractionValidated.model_fields[f].description}"
        for f in fields
    )
    extraction_prompt = json.dumps([
        {
            "role": "system",
            "content": (
                f"You are an expert insurance data extraction system. "
                f"Extract ONLY the following {label} fields from the adjuster notes. "
                f"Be precise with numbers, dates, and codes. "
                f"If a field is not mentioned or cannot be reasonably inferred, use null.\\n\\n"
                f"Fields to extract:\\n{field_descriptions}"
            )
        },
        {"role": "user", "content": adjuster_notes}
    ])
    group_schema = json.dumps(schema_for_group(group_key))

    cortex_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-3-5-sonnet', PARSE_JSON(?), {'temperature': 0, 'response_format': {'type': 'json', 'schema': PARSE_JSON(?)}}) :structured_output[0]:raw_message::STRING AS result"

    result = session.sql(cortex_sql, [extraction_prompt, group_schema]).collect()[0][0]
    extracted    = json.loads(result)
    cortex_calls += 1

    retry_count = 0
    errors      = []
    is_valid    = False

    for attempt in range(max_retries + 1):
        try:
            validated = ClaimExtractionValidated(**extracted)
            extracted = {f: getattr(validated, f) for f in fields}
            is_valid  = True
            errors    = []
            break
        except ValidationError as e:
            errors = [
                {
                    "field":       ".".join(str(x) for x in err["loc"]),
                    "message":     err["msg"],
                    "input_value": str(err.get("input", ""))[:100],
                }
                for err in e.errors()
                if ".".join(str(x) for x in err["loc"]) in fields
            ]
            if not errors:
                is_valid = True
                break
            if attempt < max_retries:
                error_desc = "\\n".join(
                    f"- Field {e['field']}: {e['message']} (current: {e['input_value']})"
                    for e in errors
                )
                fix_prompt = json.dumps([
                    {
                        "role": "system",
                        "content": (
                            f"You are an expert insurance data extraction system. "
                            f"A previous extraction for {label} fields had validation errors. "
                            f"Fix ONLY the fields with errors. Keep correct fields unchanged.\\n\\n"
                            f"Fields:\\n{field_descriptions}"
                        )
                    },
                    {
                        "role": "user",
                        "content": (
                            f"Original adjuster notes:\\n{adjuster_notes}\\n\\n"
                            f"Previous extraction:\\n{json.dumps(extracted, indent=2)}\\n\\n"
                            f"Validation errors:\\n{error_desc}\\n\\n"
                            "Please provide the corrected extraction."
                        )
                    }
                ])
                result = session.sql(cortex_sql, [fix_prompt, group_schema]).collect()[0][0]
                extracted    = json.loads(result)
                cortex_calls += 1
                retry_count  += 1
        except Exception:
            raise

    return {
        "group_key":    group_key,
        "fields":       extracted,
        "is_valid":     is_valid,
        "retry_count":  retry_count,
        "cortex_calls": cortex_calls,
        "errors":       errors,
    }


# ── Process one claim ─────────────────────────────────────────────────────────

def process_claim(session, claim_id, adjuster_notes, max_retries):
    start_time = time.time()

    group_results = [
        extract_group(session, adjuster_notes, gk, max_retries)
        for gk in FIELD_GROUPS
    ]

    merged        = {}
    total_calls   = 0
    total_retries = 0
    all_valid     = True
    all_errors    = []

    for gr in group_results:
        merged.update(gr["fields"])
        total_calls   += gr["cortex_calls"]
        total_retries += gr["retry_count"]
        all_valid      = all_valid and gr["is_valid"]
        all_errors.extend(gr["errors"])

    elapsed   = round(time.time() - start_time, 2)
    node_path = " | ".join(
        f"{gr['group_key']}({gr['cortex_calls']}calls)" for gr in group_results
    )
    merged["_metadata"] = {
        "claim_id":                claim_id,
        "is_valid":                all_valid,
        "retry_count":             total_retries,
        "cortex_calls":            total_calls,
        "processing_time_seconds": elapsed,
    }

    merged_json = json.dumps(merged)
    errors_json = json.dumps(all_errors)

    session.sql("DELETE FROM EXTRACTION_RESULTS WHERE claim_id = ?", [claim_id]).collect()

    session.sql(
        "INSERT INTO EXTRACTION_RESULTS (claim_id, extracted_data, is_valid, retry_count, cortex_calls, processing_time_seconds, node_path, extraction_errors) VALUES (?, PARSE_JSON(?), ?, ?, ?, ?, ?, PARSE_JSON(?))",
        [claim_id, merged_json, all_valid, total_retries, total_calls, elapsed, node_path, errors_json]
    ).collect()

    session.sql(
        "UPDATE EXTRACTION_PROGRESS SET status = 'completed', completed_at = CURRENT_TIMESTAMP() WHERE claim_id = ?",
        [claim_id]
    ).collect()

    return {
        "claim_id":        claim_id,
        "is_valid":        all_valid,
        "cortex_calls":    total_calls,
        "retry_count":     total_retries,
        "processing_time": elapsed,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def run(session, batch_size, max_workers, max_retries, stale_minutes):
    start_time = time.time()

    unprocessed = session.sql(
        "SELECT an.claim_id, an.adjuster_notes FROM FIMA_CLAIMS an LEFT JOIN EXTRACTION_PROGRESS ep ON an.claim_id::STRING = ep.claim_id WHERE ep.claim_id IS NULL OR ep.status = 'failed' OR (ep.status = 'in_progress' AND DATEDIFF('minute', ep.started_at, CURRENT_TIMESTAMP()) > ?) ORDER BY an.claim_id LIMIT ?",
        [stale_minutes, batch_size]
    ).collect()

    if not unprocessed:
        return {"status": "complete", "message": "All claims already processed", "processed": 0}

    results = []
    errors  = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for row in unprocessed:
            claim_id = str(row[0])
            session.sql(
                "MERGE INTO EXTRACTION_PROGRESS t USING (SELECT ? AS claim_id) s ON t.claim_id = s.claim_id WHEN MATCHED THEN UPDATE SET status = 'in_progress', started_at = CURRENT_TIMESTAMP() WHEN NOT MATCHED THEN INSERT (claim_id, status, started_at) VALUES (s.claim_id, 'in_progress', CURRENT_TIMESTAMP())",
                [claim_id]
            ).collect()
            futures[pool.submit(process_claim, session, claim_id, row[1], max_retries)] = claim_id

        for future in as_completed(futures):
            claim_id = futures[future]
            try:
                results.append(future.result())
            except Exception as e:
                session.sql(
                    "UPDATE EXTRACTION_PROGRESS SET status = 'failed', completed_at = CURRENT_TIMESTAMP() WHERE claim_id = ?",
                    [claim_id]
                ).collect()
                errors.append({"claim_id": claim_id, "error": str(e)[:500]})

    total_time  = round(time.time() - start_time, 2)
    total_calls = sum(r["cortex_calls"] for r in results)
    avg_time    = round(sum(r["processing_time"] for r in results) / len(results), 2) if results else 0

    return {
        "status":                 "complete",
        "processed":              len(results),
        "failed":                 len(errors),
        "total_cortex_calls":     total_calls,
        "wall_clock_seconds":     total_time,
        "avg_claim_time_seconds": avg_time,
        "errors":                 errors if errors else None,
    }
$$;


CREATE OR REPLACE STAGE manuals_stage 
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

ALTER GIT REPOSITORY claims_extraction_repo FETCH;

COPY FILES 
    INTO @manuals_stage 
    FROM @claims_extraction_repo/branches/main/06-2025-claims-manual.pdf;

CREATE OR REPLACE TABLE CLAIMS_MANUAL_CHUNKS (
    relative_path STRING,
    chunk_index INTEGER,
    chunk_text STRING
);