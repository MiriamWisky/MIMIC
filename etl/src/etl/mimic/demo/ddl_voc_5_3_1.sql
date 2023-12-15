CREATE
OR REPLACE TABLE voc_concept (
  concept_id          INTEGER       NOT NULL ,
  concept_name        STRING      NOT NULL ,
  domain_id           STRING      NOT NULL ,
  vocabulary_id       STRING      NOT NULL ,
  concept_class_id    STRING      NOT NULL ,
  standard_concept    STRING               ,
  concept_code        STRING      NOT NULL ,
  valid_start_DATE    DATE        NOT NULL ,
  valid_end_DATE      DATE        NOT NULL ,
  invalid_reason      STRING
)
;


CREATE
OR REPLACE TABLE voc_vocabulary (
  vocabulary_id         STRING      NOT NULL,
  vocabulary_name       STRING      NOT NULL,
  vocabulary_reference  STRING      NOT NULL,
  vocabulary_version    STRING              ,
  vocabulary_concept_id INTEGER       NOT NULL
)
;


CREATE
OR REPLACE TABLE voc_domain (
  domain_id         STRING      NOT NULL,
  domain_name       STRING      NOT NULL,
  domain_concept_id INTEGER       NOT NULL
)
;


CREATE
OR REPLACE TABLE voc_concept_class (
  concept_class_id          STRING      NOT NULL,
  concept_class_name        STRING      NOT NULL,
  concept_class_concept_id  INTEGER       NOT NULL
)
;


CREATE
OR REPLACE TABLE voc_concept_relationship (
  concept_id_1      INTEGER     NOT NULL,
  concept_id_2      INTEGER     NOT NULL,
  relationship_id   STRING    NOT NULL,
  valid_start_DATE  DATE      NOT NULL,
  valid_end_DATE    DATE      NOT NULL,
  invalid_reason    STRING
  )
;


CREATE
OR REPLACE TABLE voc_relationship (
  relationship_id         STRING      NOT NULL,
  relationship_name       STRING      NOT NULL,
  is_hierarchical         STRING      NOT NULL,
  defines_ancestry        STRING      NOT NULL,
  reverse_relationship_id STRING      NOT NULL,
  relationship_concept_id INTEGER       NOT NULL
)
;


CREATE
OR REPLACE TABLE voc_concept_synonym (
  concept_id            INTEGER       NOT NULL,
  concept_synonym_name  STRING      NOT NULL,
  language_concept_id   INTEGER       NOT NULL
)
;


CREATE
OR REPLACE TABLE voc_concept_ancestor (
  ancestor_concept_id       INTEGER   NOT NULL,
  descendant_concept_id     INTEGER   NOT NULL,
  min_levels_of_separation  INTEGER   NOT NULL,
  max_levels_of_separation  INTEGER   NOT NULL
)
;


CREATE
OR REPLACE TABLE voc_source_to_concept_map (
  source_code             STRING      NOT NULL,
  source_concept_id       INTEGER       NOT NULL,
  source_vocabulary_id    STRING      NOT NULL,
  source_code_description STRING              ,
  target_concept_id       INTEGER       NOT NULL,
  target_vocabulary_id    STRING      NOT NULL,
  valid_start_DATE        DATE        NOT NULL,
  valid_end_DATE          DATE        NOT NULL,
  invalid_reason          STRING
)
;


CREATE
OR REPLACE TABLE voc_drug_strength (
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
  invalid_reason              STRING
)
;

