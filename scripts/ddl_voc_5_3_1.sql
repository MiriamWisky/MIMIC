DROP SCHEMA IF EXISTS vocabulary CASCADE;
CREATE SCHEMA IF NOT EXISTS vocabulary AUTHORIZATION postgres;
SET search_path TO vocabulary;

CREATE TABLE concept (
  concept_id          INTEGER       NOT NULL ,
  concept_name        text      NOT NULL ,
  domain_id           text      NOT NULL ,
  vocabulary_id       text      NOT NULL ,
  concept_class_id    text      NOT NULL ,
  standard_concept    text               ,
  concept_code        text      NOT NULL ,
  valid_start_DATE    DATE        NOT NULL ,
  valid_end_DATE      DATE        NOT NULL ,
  invalid_reason      text
)
;


CREATE TABLE vocabulary (
  vocabulary_id         text      NOT NULL,
  vocabulary_name       text      NOT NULL,
  vocabulary_reference  text      NOT NULL,
  vocabulary_version    text              ,
  vocabulary_concept_id INTEGER       NOT NULL
)
;


CREATE TABLE domain (
  domain_id         text      NOT NULL,
  domain_name       text      NOT NULL,
  domain_concept_id INTEGER       NOT NULL
)
;


CREATE TABLE concept_class (
  concept_class_id          text      NOT NULL,
  concept_class_name        text      NOT NULL,
  concept_class_concept_id  INTEGER       NOT NULL
)
;


CREATE TABLE concept_relationship (
  concept_id_1      INTEGER     NOT NULL,
  concept_id_2      INTEGER     NOT NULL,
  relationship_id   text    NOT NULL,
  valid_start_DATE  DATE      NOT NULL,
  valid_end_DATE    DATE      NOT NULL,
  invalid_reason    text
  )
;


CREATE TABLE relationship (
  relationship_id         text      NOT NULL,
  relationship_name       text      NOT NULL,
  is_hierarchical         text      NOT NULL,
  defines_ancestry        text      NOT NULL,
  reverse_relationship_id text      NOT NULL,
  relationship_concept_id INTEGER       NOT NULL
)
;


CREATE TABLE concept_synonym (
  concept_id            INTEGER       NOT NULL,
  concept_synonym_name  text      NOT NULL,
  language_concept_id   INTEGER       NOT NULL
)
;


CREATE TABLE concept_ancestor (
  ancestor_concept_id       INTEGER   NOT NULL,
  descendant_concept_id     INTEGER   NOT NULL,
  min_levels_of_separation  INTEGER   NOT NULL,
  max_levels_of_separation  INTEGER   NOT NULL
)
;


-- CREATE TABLE source_to_concept_map (
--   source_code             text      NOT NULL,
--   source_concept_id       INTEGER       NOT NULL,
--   source_vocabulary_id    text      NOT NULL,
--   source_code_description text              ,
--   target_concept_id       INTEGER       NOT NULL,
--   target_vocabulary_id    text      NOT NULL,
--   valid_start_DATE        DATE        NOT NULL,
--   valid_end_DATE          DATE        NOT NULL,
--   invalid_reason          text
-- )
-- ;


CREATE TABLE drug_strength (
  drug_concept_id             INTEGER     NOT NULL,
  ingredient_concept_id       INTEGER     NOT NULL,
  amount_value                NUMERIC           ,
  amount_unit_concept_id      INTEGER             ,
  numerator_value             NUMERIC           ,
  numerator_unit_concept_id   INTEGER             ,
  denominator_value           NUMERIC           ,
  denominator_unit_concept_id INTEGER             ,
  box_size                    INTEGER             ,
  valid_start_DATE            DATE       NOT NULL,
  valid_end_DATE              DATE       NOT NULL,
  invalid_reason              text
)
;

------------------------------------------------------------------------------
-- 1. gcpt_cs_place_of_service_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_cs_place_of_service_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 2. gcpt_drug_ndc_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_drug_ndc_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 3. gcpt_drug_route_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_drug_route_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 4. gcpt_meas_chartevents_main_mod_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_meas_chartevents_main_mod_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 5. gcpt_meas_chartevents_value_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_meas_chartevents_value_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 6. gcpt_meas_lab_loinc_mod_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_meas_lab_loinc_mod_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 7. gcpt_meas_unit_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_meas_unit_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 8. gcpt_meas_waveforms_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_meas_waveforms_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 9. gcpt_micro_antibiotic_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_micro_antibiotic_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 10. gcpt_micro_microtest_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_micro_microtest_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 11. gcpt_micro_organism_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_micro_organism_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 12. gcpt_micro_resistance_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_micro_resistance_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 13. gcpt_micro_specimen_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_micro_specimen_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 14. gcpt_mimic_generated_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_mimic_generated_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 15. gcpt_obs_drgcodes_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_obs_drgcodes_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 16. gcpt_obs_insurance_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_obs_insurance_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 17. gcpt_obs_marital_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_obs_marital_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 18. gcpt_per_ethnicity_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_per_ethnicity_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 19. gcpt_proc_datetimeevents_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_proc_datetimeevents_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 20. gcpt_proc_itemid_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_proc_itemid_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);

------------------------------------------------------------------------------
-- 21. gcpt_vis_admission_mimic
------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gcpt_vis_admission_mimic (
    concept_name                     TEXT,
    source_concept_id               INTEGER,
    source_vocabulary_id            TEXT,
    source_domain_id                TEXT,
    source_concept_class_id         TEXT,
    standard_concept                CHAR(1),
    concept_code                    TEXT,
    valid_start_date                DATE,
    valid_end_date                  DATE,
    invalid_reason                  CHAR(1),
    target_concept_id               INTEGER,
    relationship_id                 TEXT,
    reverese_relationship_id        TEXT,
    relationship_valid_start_date   DATE,
    relationship_end_date           DATE,
    invalid_reason_cr               TEXT
);
