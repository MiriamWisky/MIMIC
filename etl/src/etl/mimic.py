import datetime
import itertools
import os
import typing

from .config import (
    ETL_DIR,
    ARES_DATA_ROOT,
    PRODUCTION_SCHEMA,
    TEMP_SCHEMA,
    VOCABULARY_SCHEMA,
)
from .common import (
    CDM_TABLES,
    DELIVERED_TABLES,
    VOCABULARY_TABLES,
    archive_and_rename_schema,
    create_or_replace_schema,
    execute_sql,
    generate_flow_run_name,
    get_last_cdm_release_date,
    get_schemas_as_list,
    ingest_omop,
    load_and_execute_sql,
    load_sql,
    orchestrate_sql_w_dependencies,
    rename_schema,
    subprocess_run,
    view_tables,
)

def mimic_etl():
    print(f"Started etl pipeline for transforming the MIMIC dataset.")
    t0 = datetime.datetime.now()
    target_schema = PRODUCTION_SCHEMA

    stage_schema = 'stage_' + target_schema
    print("Populating staging area...")
    create_or_replace_schema(
        stage_schema,
        "Staging area for MIMIC Transformation Process",
    )
    load_and_execute_sql(
        'cdm-ddl-5_3.sql',
        schema=stage_schema
    )

    load_mimic_source(
        # This function should load mimic source tables
    )

    view_tables(
        VOCABULARY_SCHEMA,
        VOCABULARY_TABLES,
        schema=stage_schema,
    )

    print("Building MIMIC...")
    base_etl_dir = os.path.join(ETL_DIR, 'mimic')

    orchestrate_sql_w_dependencies(base_etl_dir, stage_schema)

    schemas = get_schemas_as_list()

    if PRODUCTION_SCHEMA in schemas:
        archive_suffix = get_last_cdm_release_date(PRODUCTION_SCHEMA)
        archive_and_rename_schema(TEMP_SCHEMA, PRODUCTION_SCHEMA, archive_suffix)
    else:
        rename_schema(TEMP_SCHEMA, PRODUCTION_SCHEMA)

    subprocess_run(
        ['Rscript', os.path.join(ETL_DIR, 'ares.R'), ARES_DATA_ROOT, mode, PRODUCTION_SCHEMA],
        cwd='/ares',
        check=True,
    )

    t1 = datetime.datetime.now()
    minutes = round((t1 - t0).total_seconds() / 60)
    print(f"Successfully finished ETL pipeline for creating the MIMIC OMOP Instance in {minutes} minutes.")


