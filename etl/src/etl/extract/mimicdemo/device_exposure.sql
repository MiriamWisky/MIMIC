INSERT INTO
    device_exposure (
        device_exposure_id,
        person_id,
        device_concept_id,
        device_exposure_start_date,
        device_exposure_start_datetime,
        device_exposure_end_date,
        device_exposure_end_datetime,
        device_type_concept_id,
        unique_device_id,
        production_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        visit_detail_id,
        device_source_value,
        device_source_concept_id,
        unit_concept_id,
        unit_source_value,
        unit_source_concept_id
    )
SELECT
    device_exposure_id,
    person_id,
    device_concept_id,
    device_exposure_start_date,
    device_exposure_start_datetime,
    device_exposure_end_date,
    device_exposure_end_datetime,
    device_type_concept_id,
    unique_device_id,
    CAST(NULL AS STRING) AS production_id,
    quantity,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    device_source_value,
    device_source_concept_id,
    CAST(NULL AS INTEGER) AS unit_concept_id,
    CAST(NULL AS STRING) AS unit_source_value,
    CAST(NULL AS INTEGER) AS unit_source_concept_id
FROM
    device_exposure_53;