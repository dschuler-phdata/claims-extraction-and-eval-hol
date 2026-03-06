-- ============================================================================
-- ITERATIVE BATCH DATA GENERATION SCRIPT
-- Run this in Snowflake to generate the pre-built claims dataset for the lab.
-- Process in batches so you can check costs before committing to the full run.
--
-- Usage:
--   1. Run Steps 1-2 once to set up the sample and target tables.
--   2. Run Step 3 repeatedly — each run processes the next BATCH_SIZE rows.
--   3. Run Step 4 between batches to check costs.
--   4. When satisfied, run Step 5 to export.
-- ============================================================================

USE DATABASE CLAIMS_EXTRACTION_LAB;
USE SCHEMA HOL;
USE WAREHOUSE CLAIMS_LAB_WH;

ALTER SESSION SET QUERY_TAG = '{"lab": "claims-extraction", "section": "data-generation"}';

-- ╔══════════════════════════════════════════════════════════════════════╗
-- ║ CONFIGURATION — change these as needed                             ║
-- ╚══════════════════════════════════════════════════════════════════════╝
SET SAMPLE_SIZE  = 500;       -- total rows to include in the sample
SET BATCH_SIZE   = 100000;    -- rows to process per batch run

-- ============================================================================
-- Step 1 (run once): Create the curated sample from the raw parquet data
-- ============================================================================
CREATE OR REPLACE TABLE CLAIMS_SAMPLE AS
SELECT
    ROW_NUMBER() OVER (ORDER BY RANDOM()) AS claim_id,
    *
FROM (
    SELECT *
    FROM RAW_CLAIMS
    WHERE building_damage_amount > 0
      AND contents_damage_amount > 0
      AND flood_event IS NOT NULL
      AND water_depth IS NOT NULL
      AND state IS NOT NULL
      AND flood_zone IS NOT NULL
    LIMIT $SAMPLE_SIZE
);

SELECT COUNT(*) AS sample_size FROM CLAIMS_SAMPLE;

-- ============================================================================
-- Step 2 (run once): Create the ADJUSTER_NOTES table (empty).
--   This is separate from the insert so we can append in batches.
-- ============================================================================
CREATE TABLE IF NOT EXISTS ADJUSTER_NOTES AS
SELECT cs.*, NULL::STRING AS adjuster_notes
FROM CLAIMS_SAMPLE cs
WHERE FALSE;  -- schema only, no rows

-- ============================================================================
-- Step 3 (run repeatedly): Process the next batch of unprocessed rows.
--   Re-run this block as many times as needed. Each execution picks up
--   the next BATCH_SIZE rows that haven't been processed yet.
-- ============================================================================
INSERT INTO ADJUSTER_NOTES
SELECT
    cs.*,
    SNOWFLAKE.CORTEX.COMPLETE(
        'claude-sonnet-4-6',
        [
            {
                'role': 'system',
                'content': 'You are an experienced NFIP flood insurance adjuster writing field notes after a property inspection. Write realistic adjuster notes including all of the provided data points naturally in the narrative. Vary your writing style -- sometimes use bullet points, sometimes paragraphs, sometimes mix both. Notes should use adjuster shorthand where applicable and sometimes be brief updates. Include realistic observations about damage conditions, property state, water marks, mold, and structural concerns. Notes should be 150-500 words. Do not use markdown formatting for bold, italics, etc.. just keep plain text. '
            },
            {
                'role': 'user',
                'content': CONCAT(
                    'Write adjuster field notes for this flood claim. Embed ALL these facts naturally:\n',
                    '- Claim ID: ', claim_id::STRING, '\n',
                    '- Date of Loss: ', date_of_loss::STRING, '\n',
                    '- State: ', state, '\n',
                    '- County Code: ', county_code, '\n',
                    '- Flood Zone: ', flood_zone, '\n',
                    '- Flood Event: ', COALESCE(flood_event, 'Unknown'), '\n',
                    '- Water Depth: ', COALESCE(water_depth::STRING, 'Unknown'), ' inches\n',
                    '- Flood Duration: ', COALESCE(flood_water_duration::STRING, 'Unknown'), ' hours\n',
                    '- Building Damage: $', building_damage_amount::STRING, '\n',
                    '- Contents Damage: $', contents_damage_amount::STRING, '\n',
                    '- Building Property Value: $', COALESCE(building_property_value::STRING, 'Unknown'), '\n',
                    '- Contents Property Value: $', COALESCE(contents_property_value::STRING, 'Unknown'), '\n',
                    '- Basement Type Code: ', COALESCE(basement_type::STRING, 'N/A'), '\n',
                    '- Floodproofed: ', COALESCE(floodproofed::STRING, 'Unknown')
                )
            }
        ],
        {'temperature': 0.8}
    ):choices[0]:messages::STRING AS adjuster_notes
FROM CLAIMS_SAMPLE cs
WHERE cs.claim_id NOT IN (SELECT claim_id FROM ADJUSTER_NOTES)
LIMIT $BATCH_SIZE;

-- ============================================================================
-- Step 4: Check progress and costs
-- ============================================================================

-- 4a. Progress
SELECT
    (SELECT COUNT(*) FROM ADJUSTER_NOTES) AS rows_processed,
    (SELECT COUNT(*) FROM CLAIMS_SAMPLE)  AS total_rows,
    ROUND(rows_processed / NULLIF(total_rows, 0) * 100, 1) AS pct_complete;

-- 4b. Cortex credit usage for this lab's queries
--     (looks at the last 24 hours; adjust the time window if needed)
SELECT
    COUNT(*)                          AS query_count,
    SUM(credits_used_cloud_services)  AS total_credits,
    SUM(credits_used_cloud_services)
        / NULLIF(COUNT(*), 0)         AS avg_credits_per_query,
    MIN(start_time)                   AS first_query,
    MAX(end_time)                     AS last_query
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
    DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    CURRENT_TIMESTAMP()
))
WHERE query_tag LIKE '%claims-extraction%'
  AND query_type = 'INSERT';

-- 4c. Preview a few generated notes
SELECT claim_id, state, building_damage_amount,
       LEFT(adjuster_notes, 200) AS note_preview
FROM ADJUSTER_NOTES
ORDER BY claim_id
LIMIT 5;

-- ============================================================================
-- Step 5 (run once, after all batches): Export to a stage as compressed parquet.
-- ============================================================================
CREATE OR REPLACE STAGE export_stage;

COPY INTO @export_stage/claims_with_notes
FROM (
    SELECT
        claim_id,
        date_of_loss,
        year_of_loss,
        state,
        county_code,
        flood_zone,
        flood_event,
        building_damage_amount,
        contents_damage_amount,
        amount_paid_building,
        amount_paid_contents,
        building_property_value,
        contents_property_value,
        building_replacement_cost,
        contents_replacement_cost,
        water_depth,
        elevation_difference,
        basement_type,
        flood_characteristics,
        flood_water_duration,
        floodproofed,
        elevation_certificate,
        building_description_code,
        building_deductible_code,
        contents_deductible_code,
        state_owned,
        policy_count,
        adjuster_notes
    FROM ADJUSTER_NOTES
)
FILE_FORMAT = (TYPE = PARQUET)
HEADER = TRUE
OVERWRITE = TRUE;

-- Download from SnowSQL or the Snowflake UI:
--   GET @export_stage/claims_with_notes file:///tmp/;
--   mv /tmp/claims_with_notes_*.parquet claims_with_notes.snappy.parquet

LIST @export_stage;
