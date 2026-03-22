USE ROLE ATTENDEE_ROLE;
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
PACKAGES = ('snowflake-snowpark-python', 'pydantic', 'langgraph')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import json
import time
import operator
from typing import TypedDict, Optional, Annotated
from concurrent.futures import ThreadPoolExecutor, as_completed
from pydantic import BaseModel, Field, ValidationError, field_validator
from langgraph.graph import StateGraph, START, END
from langgraph.types import Send


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


# ── Cortex helper ─────────────────────────────────────────────────────────────

# Store session reference for use in node functions
_session = None

CORTEX_SQL = "SELECT SNOWFLAKE.CORTEX.COMPLETE('claude-sonnet-4-6', PARSE_JSON(?), PARSE_JSON(?)) :structured_output[0]:raw_message::STRING AS result"

def cortex_complete(prompt_json, schema_json):
    options = json.dumps({"temperature": 0, "max_tokens": 8192, "response_format": {"type": "json", "schema": json.loads(schema_json)}})
    return _session.sql(CORTEX_SQL, [prompt_json, options]).collect()[0][0]


# ── LangGraph State Types ─────────────────────────────────────────────────────

class ExtractionState(TypedDict):
    claim_id: str
    adjuster_notes: str
    max_retries: int
    group_results: Annotated[list, operator.add]
    validated_extraction: dict
    extraction_errors: list
    is_valid: bool
    retry_count: int
    node_history: Annotated[list, operator.add]
    cortex_calls: int
    processing_time: float


class WorkerOutput(TypedDict):
    group_results: Annotated[list, operator.add]
    node_history: Annotated[list, operator.add]


class GroupWorkerState(TypedDict):
    claim_id: str
    adjuster_notes: str
    group_key: str
    group_fields: list
    group_label: str
    max_retries: int
    extracted_fields: dict
    extraction_errors: list
    is_valid: bool
    retry_count: int
    cortex_calls: int
    processing_time: float
    group_results: Annotated[list, operator.add]
    node_history: Annotated[list, operator.add]


# ── LangGraph Node Functions ──────────────────────────────────────────────────

def extract_group_node(state: GroupWorkerState) -> dict:
    """Extract fields for a single group via Cortex."""
    start_time = time.time()
    group_key = state["group_key"]
    fields = state["group_fields"]
    label = state["group_label"]

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
                f"If a field is not mentioned or cannot be reasonably inferred from the notes, use null.\\n\\n"
                f"Fields to extract:\\n{field_descriptions}"
            )
        },
        {"role": "user", "content": state["adjuster_notes"]}
    ])

    group_schema = json.dumps(schema_for_group(group_key))

    try:
        result = cortex_complete(extraction_prompt, group_schema)
        extracted = json.loads(result)
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "extracted_fields": {},
            "extraction_errors": [{"field": "all", "message": f"Cortex extraction failed: {str(e)[:200]}", "type": "cortex_error", "input_value": ""}],
            "is_valid": False,
            "cortex_calls": 1,
            "processing_time": elapsed,
            "node_history": [f"extract_{group_key}_error"],
        }

    elapsed = time.time() - start_time
    return {
        "extracted_fields": extracted,
        "cortex_calls": 1,
        "processing_time": elapsed,
        "node_history": [f"extract_{group_key}"],
    }


def validate_group_node(state: GroupWorkerState) -> dict:
    """Validate a single group's extracted fields using Pydantic domain rules."""
    group_key = state["group_key"]
    fields = state["group_fields"]
    raw = state["extracted_fields"]

    try:
        validated = ClaimExtractionValidated(**raw)
        validated_fields = {f: getattr(validated, f) for f in fields}

        result = {
            "extracted_fields": validated_fields,
            "extraction_errors": [],
            "is_valid": True,
            "node_history": [f"validate_{group_key}_pass"],
            "group_results": [{
                "group_key": group_key,
                "fields": validated_fields,
                "is_valid": True,
                "retry_count": state.get("retry_count", 0),
                "cortex_calls": state.get("cortex_calls", 0),
                "processing_time": state.get("processing_time", 0),
                "errors": [],
            }],
        }
        return result

    except Exception as e:
        errors = []
        if hasattr(e, 'errors'):
            for err in e.errors():
                field_name = ".".join(str(x) for x in err["loc"])
                if field_name in fields:
                    errors.append({
                        "field": field_name,
                        "message": err["msg"],
                        "type": err["type"],
                        "input_value": str(err.get("input", ""))[:100]
                    })
        else:
            errors.append({"field": "unknown", "message": str(e), "type": "general"})

        result = {
            "extraction_errors": errors,
            "is_valid": False,
            "node_history": [f"validate_{group_key}_fail"],
        }

        # No group-specific errors means this group is actually valid
        if not errors:
            result["is_valid"] = True
            result["node_history"] = [f"validate_{group_key}_pass"]
            result["group_results"] = [{
                "group_key": group_key,
                "fields": raw,
                "is_valid": True,
                "retry_count": state.get("retry_count", 0),
                "cortex_calls": state.get("cortex_calls", 0),
                "processing_time": state.get("processing_time", 0),
                "errors": [],
            }]
        # Max retries exhausted — finalize with errors so we don't block merge
        elif state.get("retry_count", 0) >= state.get("max_retries", 2):
            result["group_results"] = [{
                "group_key": group_key,
                "fields": raw,
                "is_valid": False,
                "retry_count": state.get("retry_count", 0),
                "cortex_calls": state.get("cortex_calls", 0),
                "processing_time": state.get("processing_time", 0),
                "errors": errors,
            }]

        return result


def fix_group_node(state: GroupWorkerState) -> dict:
    """Re-prompt Cortex with specific validation errors to fix a single group."""
    start_time = time.time()
    group_key = state["group_key"]
    label = state["group_label"]
    fields = state["group_fields"]

    error_descriptions = "\\n".join([
        f"- Field '{e['field']}': {e['message']} (current value: {e['input_value']})"
        for e in state["extraction_errors"]
    ])

    field_descriptions = "\\n".join(
        f"- {f}: {ClaimExtractionValidated.model_fields[f].description}"
        for f in fields
    )

    fix_prompt = json.dumps([
        {
            "role": "system",
            "content": (
                f"You are an expert insurance data extraction system. "
                f"A previous extraction attempt for {label} fields had validation errors. "
                f"Fix ONLY the fields with errors. "
                f"Keep all correctly extracted fields unchanged.\\n\\n"
                f"Fields:\\n{field_descriptions}"
            )
        },
        {
            "role": "user",
            "content": (
                f"Original adjuster notes:\\n{state['adjuster_notes']}\\n\\n"
                f"Previous extraction (with errors):\\n{json.dumps(state['extracted_fields'], indent=2)}\\n\\n"
                f"Validation errors to fix:\\n{error_descriptions}\\n\\n"
                "Please provide the corrected extraction for these fields only."
            )
        }
    ])

    group_schema = json.dumps(schema_for_group(group_key))

    try:
        result = cortex_complete(fix_prompt, group_schema)
        extracted = json.loads(result)
    except Exception as e:
        elapsed = time.time() - start_time
        return {
            "extracted_fields": state.get("extracted_fields", {}),
            "extraction_errors": [{"field": "all", "message": f"Cortex fix failed: {str(e)[:200]}", "type": "cortex_error", "input_value": ""}],
            "is_valid": False,
            "retry_count": state.get("retry_count", 0) + 1,
            "cortex_calls": state.get("cortex_calls", 0) + 1,
            "processing_time": state.get("processing_time", 0) + (time.time() - start_time),
            "node_history": [f"fix_{group_key}_error"],
        }

    elapsed = time.time() - start_time

    return {
        "extracted_fields": extracted,
        "retry_count": state.get("retry_count", 0) + 1,
        "cortex_calls": state.get("cortex_calls", 0) + 1,
        "processing_time": state.get("processing_time", 0) + elapsed,
        "node_history": [f"fix_{group_key}"],
    }


def merge_groups_node(state: ExtractionState) -> dict:
    """Merge validated results from all group workers into one extraction dict."""
    merged = {}
    total_calls = 0
    total_time = 0.0
    total_retries = 0
    all_valid = True
    all_errors = []

    for group_result in state.get("group_results", []):
        merged.update(group_result["fields"])
        total_calls += group_result.get("cortex_calls", 0)
        total_time += group_result.get("processing_time", 0)
        total_retries += group_result.get("retry_count", 0)
        if not group_result.get("is_valid", False):
            all_valid = False
        all_errors.extend(group_result.get("errors", []))

    return {
        "validated_extraction": merged,
        "is_valid": all_valid,
        "cortex_calls": total_calls,
        "processing_time": total_time,
        "retry_count": total_retries,
        "extraction_errors": all_errors,
        "node_history": ["merge"],
    }


def should_retry_group(state: GroupWorkerState) -> str:
    """Decide whether to retry this group's extraction or finish the worker."""
    if state.get("is_valid", False):
        return END
    elif state.get("retry_count", 0) < state.get("max_retries", 2):
        return "fix_group"
    else:
        return END


def finalize_node(state: ExtractionState) -> dict:
    """Finalize the extraction result with metadata."""
    result = state.get("validated_extraction", {})

    result["_metadata"] = {
        "claim_id": state["claim_id"],
        "is_valid": state.get("is_valid", False),
        "retry_count": state.get("retry_count", 0),
        "cortex_calls": state.get("cortex_calls", 0),
        "node_path": " -> ".join(state.get("node_history", []) + ["finalize"]),
        "processing_time_seconds": round(state.get("processing_time", 0), 2),
        "had_errors": len(state.get("extraction_errors", [])) > 0
    }

    return {
        "validated_extraction": result,
        "node_history": ["finalize"]
    }


def fan_out_to_groups(state: ExtractionState) -> list:
    """Route to one extraction worker per field group using LangGraph Send."""
    return [
        Send("group_worker", {
            "claim_id": state["claim_id"],
            "adjuster_notes": state["adjuster_notes"],
            "group_key": group_key,
            "group_fields": group_info["fields"],
            "group_label": group_info["label"],
            "max_retries": state.get("max_retries", 2),
            "extracted_fields": {},
            "extraction_errors": [],
            "is_valid": False,
            "retry_count": 0,
            "cortex_calls": 0,
            "processing_time": 0.0,
            "group_results": [],
            "node_history": [],
        })
        for group_key, group_info in FIELD_GROUPS.items()
    ]


# ── Build LangGraph ───────────────────────────────────────────────────────────

# Worker subgraph: extract → validate → fix loop
worker_builder = StateGraph(GroupWorkerState, output_schema=WorkerOutput)
worker_builder.add_node(node="extract_group", action=extract_group_node)
worker_builder.add_node(node="validate_group", action=validate_group_node)
worker_builder.add_node(node="fix_group", action=fix_group_node)

def should_route_after_extract(state: GroupWorkerState) -> str:
    """Route to fix if extraction returned malformed JSON, otherwise validate."""
    if state.get("extraction_errors"):
        if state.get("retry_count", 0) < state.get("max_retries", 2):
            return "fix_group"
        return END
    return "validate_group"

worker_builder.add_edge(start_key=START, end_key="extract_group")
worker_builder.add_conditional_edges(
    source="extract_group",
    path=should_route_after_extract,
    path_map={"validate_group": "validate_group", "fix_group": "fix_group", END: END}
)
worker_builder.add_conditional_edges(
    source="validate_group",
    path=should_retry_group,
    path_map={"fix_group": "fix_group", END: END}
)
worker_builder.add_edge("fix_group", "validate_group")
worker_subgraph = worker_builder.compile()

# Parent graph: fan-out → workers → merge → finalize
workflow = StateGraph(ExtractionState)
workflow.add_node("group_worker", worker_subgraph)
workflow.add_node("merge", merge_groups_node)
workflow.add_node("finalize", finalize_node)

workflow.add_conditional_edges(
    START,
    fan_out_to_groups,
    ["group_worker"]
)
workflow.add_edge("group_worker", "merge")
workflow.add_edge("merge", "finalize")
workflow.add_edge("finalize", END)

extraction_graph = workflow.compile()


# ── Process one claim via LangGraph ───────────────────────────────────────────

def process_claim(session, claim_id, adjuster_notes, max_retries):
    global _session
    _session = session

    initial_state = {
        "claim_id": claim_id,
        "adjuster_notes": adjuster_notes,
        "max_retries": max_retries,
        "group_results": [],
        "validated_extraction": {},
        "extraction_errors": [],
        "retry_count": 0,
        "is_valid": False,
        "node_history": [],
        "cortex_calls": 0,
        "processing_time": 0.0,
    }

    start_time = time.time()
    result = extraction_graph.invoke(initial_state)
    elapsed = round(time.time() - start_time, 2)

    node_path = " -> ".join(result["node_history"])
    merged_json = json.dumps(result["validated_extraction"])
    errors_json = json.dumps(result.get("extraction_errors", []))

    session.sql("DELETE FROM EXTRACTION_RESULTS WHERE claim_id = ?", [claim_id]).collect()

    session.sql(
        "INSERT INTO EXTRACTION_RESULTS (claim_id, extracted_data, is_valid, retry_count, cortex_calls, processing_time_seconds, node_path, extraction_errors) SELECT ?, PARSE_JSON(?), ?, ?, ?, ?, ?, PARSE_JSON(?)",
        [claim_id, merged_json, result["is_valid"], result["retry_count"], result["cortex_calls"], elapsed, node_path, errors_json]
    ).collect()

    session.sql(
        "UPDATE EXTRACTION_PROGRESS SET status = 'completed', completed_at = CURRENT_TIMESTAMP() WHERE claim_id = ?",
        [claim_id]
    ).collect()

    return {
        "claim_id":        claim_id,
        "is_valid":        result["is_valid"],
        "cortex_calls":    result["cortex_calls"],
        "retry_count":     result["retry_count"],
        "processing_time": elapsed,
        "node_path":       node_path,
    }


# ── Main entry point ──────────────────────────────────────────────────────────

def run(session, batch_size, max_workers, max_retries, stale_minutes):
    global _session
    _session = session
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