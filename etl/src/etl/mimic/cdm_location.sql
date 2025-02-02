DROP TABLE IF EXISTS cdm_location;

CREATE TABLE cdm_location
(
    location_id           INTEGER     NOT NULL ,
    address_1             text             ,
    address_2             text             ,
    city                  text             ,
    STATE                 text             ,
    zip                   text             ,
    county                text             ,
    location_source_value text             ,
    -- 
    unit_id                       text,
    load_table_id                 text,
    load_row_id                   INTEGER,
    trace_id                      text
)
;

INSERT INTO cdm_location
SELECT 1                      AS location_id,
       CAST(NULL AS text)   AS address_1,
       CAST(NULL AS text)   AS address_2,
       CAST(NULL AS text)   AS city,
       'MA'                   AS state,
       CAST(NULL AS text)   AS zip,
       CAST(NULL AS text)   AS county,
       'Beth Israel Hospital' AS location_source_value,
       --
       'location.null'        AS unit_id,
       'null'                 AS load_table_id,
       0                      AS load_row_id,
       CAST(NULL AS text)   AS trace_id
;
