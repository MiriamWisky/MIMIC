INSERT INTO
    note (
        note_id,
        person_id,
        note_date,
        note_datetime,
        note_type_concept_id,
        note_class_concept_id,
        note_title,
        note_text,
        encoding_concept_id,
        language_concept_id,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        note_source_value,
        note_event_id,
        note_event_field_concept_id
         )
SELECT note_id,
       person_id,
       note_date,
       note_datetime,
       note_type_concept_id,
       note_class_concept_id,
       note_title,
       note_text,
       encoding_concept_id,
       language_concept_id,
       provider_id,
       visit_occurrence_id,
       visit_detail_id,
       note_source_value,
       CAST(NULL AS INTEGER) AS note_event_id,
       CAST(NULL AS INTEGER) AS note_event_field_concept_id
FROM note_53;

