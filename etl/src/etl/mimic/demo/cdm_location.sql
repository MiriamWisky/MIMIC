CREATE
OR REPLACE TABLE cdm_location
(
    location_id           INTEGER     NOT NULL ,
    address_1             STRING             ,
    address_2             STRING             ,
    city                  STRING             ,
    STATE                 STRING             ,
    zip                   STRING             ,
    county                STRING             ,
    location_source_value STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INTEGER,
    trace_id                      STRING
)
;

INSERT INTO cdm_location
SELECT 1                      AS location_id,
       CAST(NULL AS STRING)   AS address_1,
       CAST(NULL AS STRING)   AS address_2,
       CAST(NULL AS STRING)   AS city,
       'MA'                   AS state,
       CAST(NULL AS STRING)   AS zip,
       CAST(NULL AS STRING)   AS county,
       'Beth Israel Hospital' AS location_source_value,
       --
       'location.null'        AS unit_id,
       'null'                 AS load_table_id,
       0                      AS load_row_id,
       CAST(NULL AS STRING)   AS trace_id
;
