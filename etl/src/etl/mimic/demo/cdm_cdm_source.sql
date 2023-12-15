CREATE
OR REPLACE TABLE cdm_cdm_source
(
    cdm_source_name                 STRING        NOT NULL ,
    cdm_source_abbreviation         STRING             ,
    cdm_holder                      STRING             ,
    source_description              STRING             ,
    source_documentation_reference  STRING             ,
    cdm_etl_reference               STRING             ,
    source_release_date             DATE               ,
    cdm_release_date                DATE               ,
    cdm_version                     STRING             ,
    vocabulary_version              STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INTEGER,
    trace_id                      STRING
)
;

INSERT INTO cdm_cdm_source
SELECT 'MIMIC-DEMO'                                                                       AS cdm_source_name,
       'MIMIC-DEMO'                                                                       AS cdm_source_abbreviation,
       'Tufts CTSI'                                                                       AS cdm_holder,
       concat('MIMIC-IV is a publicly available database of patients ',
              'admitted to the Beth Israel Deaconess Medical Center in Boston, MA, USA.') AS source_description,
       'https://mimic-iv.mit.edu/docs/'                                                   AS source_documentation_reference,
       'https://github.com/OHDSI/MIMIC/'                                                  AS cdm_etl_reference,
       CURRENT_DATE                                                                       AS source_release_date, -- to look up
       CURRENT_DATE                                                                       AS cdm_release_date,
       '5.3.1'                                                                            AS cdm_version,
       v.vocabulary_version                                                               AS vocabulary_version,
       --
       'cdm.source'                                                                       AS unit_id,
       'none'                                                                             AS load_table_id,
       1                                                                                  AS load_row_id,
       to_json(struct(
               'mimiciv' AS trace_id
           ))                                                                             AS trace_id

FROM voc_vocabulary v
WHERE v.vocabulary_id = 'None'
;

