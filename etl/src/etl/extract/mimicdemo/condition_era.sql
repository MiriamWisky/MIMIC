INSERT INTO
    condition_era (
        condition_era_id,
        person_id,
        condition_concept_id,
        condition_era_start_date,
        condition_era_end_date,
        condition_occurrence_count
    )
SELECT
    condition_era_id,
    person_id,
    condition_concept_id,
    condition_era_start_date,
    condition_era_end_date,
    condition_occurrence_count
FROM
    condition_era_53;