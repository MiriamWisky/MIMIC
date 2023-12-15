--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_specimen
(
    specimen_id                 INTEGER     NOT NULL ,
    person_id                   INTEGER     NOT NULL ,
    specimen_concept_id         INTEGER     NOT NULL ,
    specimen_type_concept_id    INTEGER     NOT NULL ,
    specimen_date               DATE      NOT NULL ,
    specimen_datetime           TIMESTAMP           ,
    quantity                    NUMERIC            ,
    unit_concept_id             INTEGER              ,
    anatomic_site_concept_id    INTEGER              ,
    disease_status_concept_id   INTEGER              ,
    specimen_source_id          STRING             ,
    specimen_source_value       STRING             ,
    unit_source_value           STRING             ,
    anatomic_site_source_value  STRING             ,
    disease_status_source_value STRING             ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INTEGER,
    trace_id                      STRING
)
;


INSERT INTO cdm_specimen
SELECT src.specimen_id                    AS specimen_id,
       per.person_id                      AS person_id,
       COALESCE(src.target_concept_id, 0) AS specimen_concept_id,
       32856                              AS specimen_type_concept_id, -- OMOP4976929 Lab
       CAST(src.start_datetime AS DATE)   AS specimen_date,
       src.start_datetime                 AS specimen_datetime,
       CAST(NULL AS NUMERIC)              AS quantity,
       CAST(NULL AS INTEGER)              AS unit_concept_id,
       0                                  AS anatomic_site_concept_id,
       0                                  AS disease_status_concept_id,
       src.trace_id                       AS specimen_source_id,
       src.source_code                    AS specimen_source_value,
       CAST(NULL AS STRING)               AS unit_source_value,
       CAST(NULL AS STRING)               AS anatomic_site_source_value,
       CAST(NULL AS STRING)               AS disease_status_source_value,
       --
       concat('specimen.', src.unit_id)   AS unit_id,
       src.load_table_id                  AS load_table_id,
       src.load_row_id                    AS load_row_id,
       src.trace_id                       AS trace_id
FROM lk_specimen_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS STRING) = per.person_source_value
WHERE src.target_domain_id = 'Specimen'
;
