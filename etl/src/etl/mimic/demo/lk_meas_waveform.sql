
CREATE
OR REPLACE TABLE lk_waveform_clean AS
SELECT wh.subject_id         AS subject_id,
       concat(
               wh.reference_id, '.',
               COALESCE(src.visit_detail___source, 'Unknown'), '.',
               CAST(COALESCE(src.visit_detail___start_from_minutes, -1) AS STRING), '.',
               CAST(COALESCE(src.visit_detail___report_minutes, -1) AS STRING), '.',
               CAST(COALESCE(src.visit_detail___sumarize_minutes, -1) AS STRING), '.',
               COALESCE(src.visit_detail___method, 'UNKNOWN'), '.',
               src.source_code
           )                 AS reference_id, -- make the field unique for Visit_detail_source_value
       src.mx_datetime       AS start_datetime,
       src.value_as_number   AS value_as_number,
       src.source_code       AS source_code,
       src.unit_source_value AS unit_source_value,
       --
       'waveforms.poc_3'     AS unit_id,
       src.load_table_id     AS load_table_id,
       src.load_row_id       AS load_row_id,
       src.trace_id          AS trace_id
FROM src_waveform_mx_3 src -- wm
         INNER JOIN
     src_waveform_header_3 wh
     ON wh.case_id = src.case_id
;

-- -------------------------------------------------------------------
-- lk_wf_hadm_id
-- pick additional hadm_id by event start_datetime
-- row_num is added to select the earliest if more than one hadm_ids are found
-- -------------------------------------------------------------------

CREATE
OR REPLACE TABLE lk_wf_hadm_id AS
SELECT src.trace_id AS event_trace_id,
       adm.hadm_id  AS hadm_id,
       row_number()    over (
        partition BY src.trace_id
        ORDER BY adm.start_datetime
    )                                   AS row_num
FROM lk_waveform_clean src
         INNER JOIN
     lk_admissions_clean adm
     ON adm.subject_id = src.subject_id
         AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
;

-- -------------------------------------------------------------------
-- lk_meas_waveform_mapped
-- Rule 10 (waveform)
-- reference_id = visit_detail_source_value
-- -------------------------------------------------------------------


CREATE
OR REPLACE TABLE lk_meas_waveform_mapped AS
SELECT hash(uuid())                                AS measurement_id,
       src.subject_id                              AS subject_id,
       hadm.hadm_id                                AS hadm_id,      -- get hadm_id by TIMESTAMP period
       src.reference_id                            AS reference_id, -- make field unique for visit_detail_source_value
       COALESCE(vc2.concept_id, 0)                 AS target_concept_id,
       COALESCE(vc2.domain_id, 'Measurement')      AS target_domain_id,
       src.start_datetime                          AS start_datetime,
       src.value_as_number                         AS value_as_number,
       if(src.unit_source_value IS NOT NULL,
          COALESCE(uc.target_concept_id, 0), NULL) AS unit_concept_id,
       src.source_code                             AS source_code,
       COALESCE(vc1.concept_id, 0)                 AS source_concept_id,
       src.unit_source_value                       AS unit_source_value,
       --
       src.unit_id                                 AS unit_id,
       src.load_table_id                           AS load_table_id,
       src.load_row_id                             AS load_row_id,
       src.trace_id                                AS trace_id
FROM lk_waveform_clean src
         -- mapping of the main source code
-- mapping for measurement unit
         LEFT JOIN
     lk_meas_unit_concept uc
     ON uc.source_code = src.unit_source_value
         -- supposing that the standard mapping is supplemented with custom concepts for waveform specific units
         LEFT JOIN
     voc_concept vc1
     ON vc1.concept_code = src.source_code
         AND vc1.vocabulary_id = 'mimiciv_meas_wf'
         -- supposing that the standard mapping is supplemented with custom concepts for waveform specific values
         LEFT JOIN
     voc_concept_relationship vr
     ON vc1.concept_id = vr.concept_id_1
         AND vr.relationship_id = 'Maps to'
         LEFT JOIN
     voc_concept vc2
     ON vc2.concept_id = vr.concept_id_2
         AND vc2.standard_concept = 'S'
         AND vc2.invalid_reason IS NULL
         LEFT JOIN
     lk_wf_hadm_id hadm
     ON hadm.event_trace_id = src.trace_id
         AND hadm.row_num = 1
;

