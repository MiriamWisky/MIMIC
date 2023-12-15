CREATE
OR REPLACE TABLE src_procedureevents AS
SELECT hadm_id    AS hadm_id,
       subject_id AS subject_id,
       stay_id    AS stay_id,
       itemid     AS itemid,
       starttime  AS starttime,
       value AS VALUE,
    CAST(0 AS INTEGER)                    AS cancelreason, -- MIMIC IV 2.0 change, the field is removed
    --
    'procedureevents'                   AS load_table_id,
    hash(uuid())   AS load_row_id,
    to_json(struct(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        starttime AS starttime
    ))                                  AS trace_id
FROM
    procedureevents_mimic
;

-- -------------------------------------------------------------------
-- src_d_items
-- -------------------------------------------------------------------

CREATE
OR REPLACE TABLE src_d_items AS
SELECT itemid       AS itemid,
       label        AS label,
       linksto      AS linksto,
       -- abbreviation
       -- category
       -- unitname
       -- param_type
       -- lownormalvalue
       -- highnormalvalue
       --
       'd_items'    AS load_table_id,
       hash(uuid()) AS load_row_id,
       to_json(struct(
               itemid AS itemid,
               linksto AS linksto
           ))       AS trace_id
FROM d_items_mimic
;

-- -------------------------------------------------------------------
-- src_datetimeevents
-- -------------------------------------------------------------------

CREATE
OR REPLACE TABLE src_datetimeevents AS
SELECT subject_id AS subject_id,
       hadm_id    AS hadm_id,
       stay_id    AS stay_id,
       itemid     AS itemid,
       charttime  AS charttime,
       value AS VALUE,
    --
    'datetimeevents'                    AS load_table_id,
    hash(uuid())   AS load_row_id,
    to_json(struct(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    datetimeevents_mimic
;


CREATE
OR REPLACE TABLE src_chartevents AS
SELECT subject_id AS subject_id,
       hadm_id    AS hadm_id,
       stay_id    AS stay_id,
       itemid     AS itemid,
       charttime  AS charttime,
       value AS VALUE,
    valuenum    AS valuenum,
    valueuom    AS valueuom,
    --
    'chartevents'                       AS load_table_id,
    hash(uuid())   AS load_row_id,
    to_json(struct(
        subject_id AS subject_id,
        hadm_id AS hadm_id,
        stay_id AS stay_id,
        charttime AS charttime
    ))                                  AS trace_id
FROM
    chartevents_mimic
;