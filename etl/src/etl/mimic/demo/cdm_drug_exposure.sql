CREATE
OR REPLACE TABLE cdm_drug_exposure
(
    drug_exposure_id              INTEGER       NOT NULL ,
    person_id                     INTEGER       NOT NULL ,
    drug_concept_id               INTEGER       NOT NULL ,
    drug_exposure_start_date      DATE        NOT NULL ,
    drug_exposure_start_datetime  TIMESTAMP             ,
    drug_exposure_end_date        DATE        NOT NULL ,
    drug_exposure_end_datetime    TIMESTAMP             ,
    verbatim_end_date             DATE                 ,
    drug_type_concept_id          INTEGER       NOT NULL ,
    stop_reason                   STRING               ,
    refills                       INTEGER                ,
    quantity                      NUMERIC              ,
    days_supply                   INTEGER                ,
    sig                           STRING               ,
    route_concept_id              INTEGER                ,
    lot_number                    STRING               ,
    provider_id                   INTEGER                ,
    visit_occurrence_id           INTEGER                ,
    visit_detail_id               INTEGER                ,
    drug_source_value             STRING               ,
    drug_source_concept_id        INTEGER                ,
    route_source_value            STRING               ,
    dose_unit_source_value        STRING               ,
    -- 
    unit_id                       STRING,
    load_table_id                 STRING,
    load_row_id                   INTEGER,
    trace_id                      STRING
)
;

INSERT INTO cdm_drug_exposure
SELECT hash(uuid())                     AS drug_exposure_id,
       per.person_id                    AS person_id,
       src.target_concept_id            AS drug_concept_id,
       CAST(src.start_datetime AS DATE) AS drug_exposure_start_date,
       src.start_datetime               AS drug_exposure_start_datetime,
       CAST(src.end_datetime AS DATE)   AS drug_exposure_end_date,
       src.end_datetime                 AS drug_exposure_end_datetime,
       CAST(NULL AS DATE)               AS verbatim_end_date,
       src.type_concept_id              AS drug_type_concept_id,
       CAST(NULL AS STRING)             AS stop_reason,
       CAST(NULL AS INTEGER)            AS refills,
       src.quantity                     AS quantity,
       CAST(NULL AS INTEGER)            AS days_supply,
       CAST(NULL AS STRING)             AS sig,
       src.route_concept_id             AS route_concept_id,
       CAST(NULL AS STRING)             AS lot_number,
       CAST(NULL AS INTEGER)            AS provider_id,
       vis.visit_occurrence_id          AS visit_occurrence_id,
       CAST(NULL AS INTEGER)            AS visit_detail_id,
       src.source_code                  AS drug_source_value,
       src.source_concept_id            AS drug_source_concept_id,
       src.route_source_code            AS route_source_value,
       src.dose_unit_source_code        AS dose_unit_source_value,
       --
       concat('drug.', src.unit_id)     AS unit_id,
       src.load_table_id                AS load_table_id,
       src.load_row_id                  AS load_row_id,
       src.trace_id                     AS trace_id
FROM lk_drug_mapped src
         INNER JOIN
     cdm_person per
     ON CAST(src.subject_id AS STRING) = per.person_source_value
         INNER JOIN
     cdm_visit_occurrence vis
     ON vis.visit_source_value =
        concat(CAST(src.subject_id AS STRING), '|', CAST(src.hadm_id AS STRING))
WHERE src.target_domain_id = 'Drug'
;
