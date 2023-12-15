INSERT INTO
    note_nlp (
                 note_nlp_id,
                 note_id,
                 section_concept_id,
                 snippet,
                 offset,
                 lexical_variant,
                 note_nlp_concept_id,
                 note_nlp_source_concept_id,
                 nlp_system,
                 nlp_date,
                 nlp_datetime,
                 term_exists,
                 term_temporal,
                 term_modifiers
             )
SELECT note_nlp_id,
       note_id,
       section_concept_id,
       snippet,
       offset,
       lexical_variant,
       note_nlp_concept_id,
       note_nlp_source_concept_id,
       nlp_system,
       nlp_date,
       nlp_datetime,
       term_exists,
       term_temporal,
       term_modifiers
FROM note_nlp_53;