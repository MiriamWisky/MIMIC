-- Set the search path to the vocabulary schema
SET search_path TO vocabulary;

TRUNCATE TABLE concept;
\COPY concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason) FROM 'path_to_CONCEPT.csv' CSV DELIMITER E'\t' QUOTE E'\x01' ESCAPE E'\x01' HEADER NULL '';


TRUNCATE TABLE vocabulary;
\COPY vocabulary (vocabulary_id, vocabulary_name, vocabulary_reference, vocabulary_version, vocabulary_concept_id) FROM 'path_to_VOCABULARY.csv' CSV DELIMITER E'\t' HEADER NULL '';


TRUNCATE TABLE domain;
\COPY domain (domain_id, domain_name, domain_concept_id) FROM 'path_to_DOMAIN.csv' CSV DELIMITER E'\t' HEADER NULL '';


TRUNCATE TABLE concept_class;
\COPY concept_class (concept_class_id, concept_class_name, concept_class_concept_id) FROM 'path_to_CONCEPT_CLASS.csv' CSV DELIMITER E'\t' HEADER NULL '';


TRUNCATE TABLE relationship;
\COPY relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id) FROM 'path_to_RELATIONSHIP.csv' CSV DELIMITER E'\t' HEADER NULL '';


TRUNCATE TABLE concept_relationship;
\COPY concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason) FROM 'path_to_CONCEPT_RELATIONSHIP.csv' CSV DELIMITER E'\t' HEADER NULL '';


TRUNCATE TABLE concept_synonym;
\COPY concept_synonym (concept_id, concept_synonym_name, language_concept_id) FROM 'path_to_CONCEPT_SYNONYM.csv' CSV DELIMITER E'\t' QUOTE E'\x01' ESCAPE E'\x01' HEADER NULL '';


TRUNCATE TABLE concept_ancestor;
\COPY concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation, max_levels_of_separation) FROM 'path_to_CONCEPT_ANCESTOR.csv' CSV DELIMITER E'\t' HEADER NULL '';


TRUNCATE TABLE drug_strength;
\COPY drug_strength (drug_concept_id, ingredient_concept_id, amount_value, amount_unit_concept_id, numerator_value, numerator_unit_concept_id, denominator_value, denominator_unit_concept_id, box_size, valid_start_date, valid_end_date, invalid_reason) FROM 'path_to_DRUG_STRENGTH.csv' CSV DELIMITER E'\t' HEADER NULL '';

TRUNCATE TABLE gcpt_cs_place_of_service_mimic;
\COPY gcpt_cs_place_of_service_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_cs_place_of_service.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';

TRUNCATE TABLE gcpt_drug_ndc_mimic;
\COPY gcpt_drug_ndc_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_drug_ndc.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_drug_route_mimic;
\COPY gcpt_drug_route_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_drug_route.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';

TRUNCATE TABLE gcpt_meas_chartevents_main_mod_mimic;
\COPY gcpt_meas_chartevents_main_mod_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_meas_chartevents_main_mod.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_meas_chartevents_value_mimic;
\COPY gcpt_meas_chartevents_value_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_meas_chartevents_value.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_meas_lab_loinc_mod_mimic;
\COPY gcpt_meas_lab_loinc_mod_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_meas_lab_loinc_mod.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_meas_unit_mimic;
\COPY gcpt_meas_unit_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_meas_unit.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_meas_waveforms_mimic;
\COPY gcpt_meas_waveforms_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_meas_waveforms.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_micro_antibiotic_mimic;
\COPY gcpt_micro_antibiotic_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_micro_antibiotic.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_micro_microtest_mimic;
\COPY gcpt_micro_microtest_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_micro_microtest.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_micro_organism_mimic;
\COPY gcpt_micro_organism_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_micro_organism.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_micro_resistance_mimic;
\COPY gcpt_micro_resistance_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_micro_resistance.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_micro_specimen_mimic;
\COPY gcpt_micro_specimen_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_micro_specimen.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_mimic_generated_mimic;
\COPY gcpt_mimic_generated_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_mimic_generated.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_obs_drgcodes_mimic;
\COPY gcpt_obs_drgcodes_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_obs_drgcodes.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_obs_insurance_mimic;
\COPY gcpt_obs_insurance_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_obs_insurance.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_obs_marital_mimic;
\COPY gcpt_obs_marital_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_obs_marital.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_per_ethnicity_mimic;
\COPY gcpt_per_ethnicity_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_per_ethnicity.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_proc_datetimeevents_mimic;
\COPY gcpt_proc_datetimeevents_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_proc_datetimeevents.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_proc_itemid_mimic;
\COPY gcpt_proc_itemid_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_proc_itemid.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';


TRUNCATE TABLE gcpt_vis_admission_mimic;
\COPY gcpt_vis_admission_mimic (concept_name, source_concept_id, source_vocabulary_id, source_domain_id, source_concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason, target_concept_id, relationship_id, reverese_relationship_id, relationship_valid_start_date, relationship_end_date, invalid_reason_cr) FROM 'path_to_gcpt_vis_admission.csv' CSV HEADER QUOTE '"' DELIMITER ',' ENCODING 'UTF8' NULL '';
