INSERT INTO
    death (
        person_id,
        death_date,
        death_datetime,
        death_type_concept_id,
        cause_concept_id,
        cause_source_value,
        cause_source_concept_id
    )
SELECT
    person_id,
    death_date,
    death_datetime,
    death_type_concept_id,
    cause_concept_id,
    cause_source_value,
    cause_source_concept_id
FROM
    death_53;