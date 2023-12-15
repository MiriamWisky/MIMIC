CREATE
OR REPLACE TABLE cdm_cohort_definition (
  cohort_definition_id            INTEGER       NOT NULL,
  cohort_definition_name          STRING      NOT NULL,
  cohort_definition_description   STRING              ,
  definition_type_concept_id      INTEGER       NOT NULL,
  cohort_definition_syntax        STRING              ,
  subject_concept_id              INTEGER       NOT NULL,
  cohort_initiation_date          DATE
)
;


CREATE
OR REPLACE TABLE cdm_attribute_definition (
  attribute_definition_id     INTEGER       NOT NULL,
  attribute_name              STRING      NOT NULL,
  attribute_description       STRING              ,
  attribute_type_concept_id   INTEGER       NOT NULL,
  attribute_syntax            STRING
)
;


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
  vocabulary_version              STRING
)
;


CREATE
OR REPLACE TABLE cdm_metadata
(
  metadata_concept_id       INTEGER       NOT NULL ,
  metadata_type_concept_id  INTEGER       NOT NULL ,
  NAME                      STRING      NOT NULL ,
  value_as_string           STRING               ,
  value_as_concept_id       INTEGER                ,
  metadata_date             DATE                 ,
  metadata_datetime         TIMESTAMP
)
;



--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_person
(
  person_id                   INTEGER     NOT NULL ,
  gender_concept_id           INTEGER     NOT NULL ,
  year_of_birth               INTEGER     NOT NULL ,
  month_of_birth              INTEGER              ,
  day_of_birth                INTEGER              ,
  birth_datetime              TIMESTAMP           ,
  race_concept_id             INTEGER     NOT NULL,
  ethnicity_concept_id        INTEGER     NOT NULL,
  location_id                 INTEGER              ,
  provider_id                 INTEGER              ,
  care_site_id                INTEGER              ,
  person_source_value         STRING             ,
  gender_source_value         STRING             ,
  gender_source_concept_id    INTEGER              ,
  race_source_value           STRING             ,
  race_source_concept_id      INTEGER              ,
  ethnicity_source_value      STRING             ,
  ethnicity_source_concept_id INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_observation_period
(
  observation_period_id             INTEGER   NOT NULL ,
  person_id                         INTEGER   NOT NULL ,
  observation_period_start_date     DATE    NOT NULL ,
  observation_period_end_date       DATE    NOT NULL ,
  period_type_concept_id            INTEGER   NOT NULL
)
;


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
  disease_status_source_value STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_death
(
  person_id               INTEGER     NOT NULL ,
  death_date              DATE      NOT NULL ,
  death_datetime          TIMESTAMP           ,
  death_type_concept_id   INTEGER     NOT NULL ,
  cause_concept_id        INTEGER              ,
  cause_source_value      STRING             ,
  cause_source_concept_id INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_visit_occurrence
(
  visit_occurrence_id           INTEGER     NOT NULL ,
  person_id                     INTEGER     NOT NULL ,
  visit_concept_id              INTEGER     NOT NULL ,
  visit_start_date              DATE      NOT NULL ,
  visit_start_datetime          TIMESTAMP           ,
  visit_end_date                DATE      NOT NULL ,
  visit_end_datetime            TIMESTAMP           ,
  visit_type_concept_id         INTEGER     NOT NULL ,
  provider_id                   INTEGER              ,
  care_site_id                  INTEGER              ,
  visit_source_value            STRING             ,
  visit_source_concept_id       INTEGER              ,
  admitting_source_concept_id   INTEGER              ,
  admitting_source_value        STRING             ,
  discharge_to_concept_id       INTEGER              ,
  discharge_to_source_value     STRING             ,
  preceding_visit_occurrence_id INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_visit_detail
(
  visit_detail_id                    INTEGER     NOT NULL ,
  person_id                          INTEGER     NOT NULL ,
  visit_detail_concept_id            INTEGER     NOT NULL ,
  visit_detail_start_date            DATE      NOT NULL ,
  visit_detail_start_datetime        TIMESTAMP           ,
  visit_detail_end_date              DATE      NOT NULL ,
  visit_detail_end_datetime          TIMESTAMP           ,
  visit_detail_type_concept_id       INTEGER     NOT NULL ,
  provider_id                        INTEGER              ,
  care_site_id                       INTEGER              ,
  admitting_source_concept_id        INTEGER              ,
  discharge_to_concept_id            INTEGER              ,
  preceding_visit_detail_id          INTEGER              ,
  visit_detail_source_value          STRING             ,
  visit_detail_source_concept_id     INTEGER              ,
  admitting_source_value             STRING             ,
  discharge_to_source_value          STRING             ,
  visit_detail_parent_id             INTEGER              ,
  visit_occurrence_id                INTEGER     NOT NULL
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_procedure_occurrence
(
  procedure_occurrence_id     INTEGER     NOT NULL ,
  person_id                   INTEGER     NOT NULL ,
  procedure_concept_id        INTEGER     NOT NULL ,
  procedure_date              DATE      NOT NULL ,
  procedure_datetime          TIMESTAMP           ,
  procedure_type_concept_id   INTEGER     NOT NULL ,
  modifier_concept_id         INTEGER              ,
  quantity                    INTEGER              ,
  provider_id                 INTEGER              ,
  visit_occurrence_id         INTEGER              ,
  visit_detail_id             INTEGER              ,
  procedure_source_value      STRING             ,
  procedure_source_concept_id INTEGER              ,
  modifier_source_value      STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
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
  dose_unit_source_value        STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_device_exposure
(
  device_exposure_id              INTEGER       NOT NULL ,
  person_id                       INTEGER       NOT NULL ,
  device_concept_id               INTEGER       NOT NULL ,
  device_exposure_start_date      DATE        NOT NULL ,
  device_exposure_start_datetime  TIMESTAMP             ,
  device_exposure_end_date        DATE                 ,
  device_exposure_end_datetime    TIMESTAMP             ,
  device_type_concept_id          INTEGER       NOT NULL ,
  unique_device_id                STRING               ,
  quantity                        INTEGER                ,
  provider_id                     INTEGER                ,
  visit_occurrence_id             INTEGER                ,
  visit_detail_id                 INTEGER                ,
  device_source_value             STRING               ,
  device_source_concept_id        INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_condition_occurrence
(
  condition_occurrence_id       INTEGER     NOT NULL ,
  person_id                     INTEGER     NOT NULL ,
  condition_concept_id          INTEGER     NOT NULL ,
  condition_start_date          DATE      NOT NULL ,
  condition_start_datetime      TIMESTAMP           ,
  condition_end_date            DATE               ,
  condition_end_datetime        TIMESTAMP           ,
  condition_type_concept_id     INTEGER     NOT NULL ,
  stop_reason                   STRING             ,
  provider_id                   INTEGER              ,
  visit_occurrence_id           INTEGER              ,
  visit_detail_id               INTEGER              ,
  condition_source_value        STRING             ,
  condition_source_concept_id   INTEGER              ,
  condition_status_source_value STRING             ,
  condition_status_concept_id   INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_measurement
(
  measurement_id                INTEGER     NOT NULL ,
  person_id                     INTEGER     NOT NULL ,
  measurement_concept_id        INTEGER     NOT NULL ,
  measurement_date              DATE      NOT NULL ,
  measurement_datetime          TIMESTAMP           ,
  measurement_time              STRING             ,
  measurement_type_concept_id   INTEGER     NOT NULL ,
  operator_concept_id           INTEGER              ,
  value_as_number               NUMERIC            ,
  value_as_concept_id           INTEGER              ,
  unit_concept_id               INTEGER              ,
  range_low                     NUMERIC            ,
  range_high                    NUMERIC            ,
  provider_id                   INTEGER              ,
  visit_occurrence_id           INTEGER              ,
  visit_detail_id               INTEGER              ,
  measurement_source_value      STRING             ,
  measurement_source_concept_id INTEGER              ,
  unit_source_value             STRING             ,
  value_source_value            STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_note
(
  note_id               INTEGER       NOT NULL ,
  person_id             INTEGER       NOT NULL ,
  note_date             DATE        NOT NULL ,
  note_datetime         TIMESTAMP             ,
  note_type_concept_id  INTEGER       NOT NULL ,
  note_class_concept_id INTEGER       NOT NULL ,
  note_title            STRING               ,
  note_text             STRING               ,
  encoding_concept_id   INTEGER       NOT NULL ,
  language_concept_id   INTEGER       NOT NULL ,
  provider_id           INTEGER                ,
  visit_occurrence_id   INTEGER                ,
  visit_detail_id       INTEGER                ,
  note_source_value     STRING
)
;



CREATE
OR REPLACE TABLE cdm_note_nlp
(
  note_nlp_id                 INTEGER                ,
  note_id                     INTEGER                ,
  section_concept_id          INTEGER                ,
  snippet                     STRING               ,
  OFFSET                      STRING               ,
  lexical_variant             STRING      NOT NULL ,
  note_nlp_concept_id         INTEGER                ,
  note_nlp_source_concept_id  INTEGER                ,
  nlp_system                  STRING               ,
  nlp_date                    DATE        NOT NULL ,
  nlp_datetime                TIMESTAMP             ,
  term_exists                 STRING               ,
  term_temporal               STRING               ,
  term_modifiers              STRING
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_observation
(
  observation_id                INTEGER     NOT NULL ,
  person_id                     INTEGER     NOT NULL ,
  observation_concept_id        INTEGER     NOT NULL ,
  observation_date              DATE      NOT NULL ,
  observation_datetime          TIMESTAMP           ,
  observation_type_concept_id   INTEGER     NOT NULL ,
  value_as_number               NUMERIC        ,
  value_as_string               STRING         ,
  value_as_concept_id           INTEGER          ,
  qualifier_concept_id          INTEGER          ,
  unit_concept_id               INTEGER          ,
  provider_id                   INTEGER          ,
  visit_occurrence_id           INTEGER          ,
  visit_detail_id               INTEGER          ,
  observation_source_value      STRING         ,
  observation_source_concept_id INTEGER          ,
  unit_source_value             STRING         ,
  qualifier_source_value        STRING
)
;


CREATE
OR REPLACE TABLE cdm_fact_relationship
(
  domain_concept_id_1     INTEGER     NOT NULL ,
  fact_id_1               INTEGER     NOT NULL ,
  domain_concept_id_2     INTEGER     NOT NULL ,
  fact_id_2               INTEGER     NOT NULL ,
  relationship_concept_id INTEGER     NOT NULL
)
;


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
  location_source_value STRING
)
;


CREATE
OR REPLACE TABLE cdm_care_site
(
  care_site_id                  INTEGER       NOT NULL ,
  care_site_name                STRING               ,
  place_of_service_concept_id   INTEGER                ,
  location_id                   INTEGER                ,
  care_site_source_value        STRING               ,
  place_of_service_source_value STRING
)
;


CREATE
OR REPLACE TABLE cdm_provider
(
  provider_id                 INTEGER       NOT NULL ,
  provider_name               STRING               ,
  npi                         STRING               ,
  dea                         STRING               ,
  specialty_concept_id        INTEGER                ,
  care_site_id                INTEGER                ,
  year_of_birth               INTEGER                ,
  gender_concept_id           INTEGER                ,
  provider_source_value       STRING               ,
  specialty_source_value      STRING               ,
  specialty_source_concept_id INTEGER                ,
  gender_source_value         STRING               ,
  gender_source_concept_id    INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_payer_plan_period
(
  payer_plan_period_id          INTEGER     NOT NULL ,
  person_id                     INTEGER     NOT NULL ,
  payer_plan_period_start_date  DATE      NOT NULL ,
  payer_plan_period_end_date    DATE      NOT NULL ,
  payer_concept_id              INTEGER              ,
  payer_source_value            STRING             ,
  payer_source_concept_id       INTEGER              ,
  plan_concept_id               INTEGER              ,
  plan_source_value             STRING             ,
  plan_source_concept_id        INTEGER              ,
  sponsor_concept_id            INTEGER              ,
  sponsor_source_value          STRING             ,
  sponsor_source_concept_id     INTEGER              ,
  family_source_value           STRING             ,
  stop_reason_concept_id        INTEGER              ,
  stop_reason_source_value      STRING             ,
  stop_reason_source_concept_id INTEGER
)
;


CREATE
OR REPLACE TABLE cdm_cost
(
  cost_id                   INTEGER     NOT NULL ,
  cost_event_id             INTEGER     NOT NULL ,
  cost_domain_id            STRING    NOT NULL ,
  cost_type_concept_id      INTEGER     NOT NULL ,
  currency_concept_id       INTEGER              ,
  total_charge              NUMERIC            ,
  total_cost                NUMERIC            ,
  total_paid                NUMERIC            ,
  paid_by_payer             NUMERIC            ,
  paid_by_patient           NUMERIC            ,
  paid_patient_copay        NUMERIC            ,
  paid_patient_coinsurance  NUMERIC            ,
  paid_patient_deductible   NUMERIC            ,
  paid_by_primary           NUMERIC            ,
  paid_ingredient_cost      NUMERIC            ,
  paid_dispensing_fee       NUMERIC            ,
  payer_plan_period_id      INTEGER              ,
  amount_allowed            NUMERIC            ,
  revenue_code_concept_id   INTEGER              ,
  revenue_code_source_value  STRING            ,
  drg_concept_id            INTEGER              ,
  drg_source_value          STRING
)
;


--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE
OR REPLACE TABLE cdm_cohort
(
  cohort_definition_id  INTEGER   NOT NULL ,
  subject_id            INTEGER   NOT NULL ,
  cohort_start_date     DATE      NOT NULL ,
  cohort_end_date       DATE      NOT NULL
)
;


--HINT DISTRIBUTE_ON_KEY(subject_id)
CREATE
OR REPLACE TABLE cdm_cohort_attribute
(
  cohort_definition_id    INTEGER     NOT NULL ,
  subject_id              INTEGER     NOT NULL ,
  cohort_start_date       DATE      NOT NULL ,
  cohort_end_date         DATE      NOT NULL ,
  attribute_definition_id INTEGER     NOT NULL ,
  value_as_number         NUMERIC            ,
  value_as_concept_id     INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_drug_era
(
  drug_era_id         INTEGER     NOT NULL ,
  person_id           INTEGER     NOT NULL ,
  drug_concept_id     INTEGER     NOT NULL ,
  drug_era_start_date DATE      NOT NULL ,
  drug_era_end_date   DATE      NOT NULL ,
  drug_exposure_count INTEGER              ,
  gap_days            INTEGER
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_dose_era
(
  dose_era_id           INTEGER     NOT NULL ,
  person_id             INTEGER     NOT NULL ,
  drug_concept_id       INTEGER     NOT NULL ,
  unit_concept_id       INTEGER     NOT NULL ,
  dose_value            NUMERIC   NOT NULL ,
  dose_era_start_date   DATE      NOT NULL ,
  dose_era_end_date     DATE      NOT NULL
)
;


--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE
OR REPLACE TABLE cdm_condition_era
(
  condition_era_id            INTEGER     NOT NULL ,
  person_id                   INTEGER     NOT NULL ,
  condition_concept_id        INTEGER     NOT NULL ,
  condition_era_start_date    DATE      NOT NULL ,
  condition_era_end_date      DATE      NOT NULL ,
  condition_occurrence_count  INTEGER
)
;