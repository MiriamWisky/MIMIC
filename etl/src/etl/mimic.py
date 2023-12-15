import datetime
import os
import pandas as pd
from pydicom.filereader import dcmread
from pydicom.filewriter import dcmwrite
import shutil
import typing

import prefect

from .config import (
    DATABRICKS_JDBC_URL,
    DATABRICKS_ACCESS_TOKEN,
    MIMIC_DEMO_SCHEMA,
    MIMIC_DEMO_SOURCE_SCHEMA,
    MIMIC_FULL_SCHEMA,
    MIMIC_FULL_SOURCE_SCHEMA,
    VOCABULARY_SCHEMA,
    ETL_DIR,
)
from .common import (
    GENERATED_TABLES_5_3,
    MIMIC_EXPORT,
    MIMIC_TABLES_1,
    MIMIC_TABLES_2,
    VOCABULARY_TABLES,
    clone_tables,
    create_or_replace_schema,
    databricks_cursor,
    execute_sql,
    insert_tables,
    generate_flow_run_name,
    get_ddl,
    load_and_execute_sql,
    load_and_execute_sql_concurrently,
    orchestrate_sql_w_dependencies,
    save_tables_as_csv,
    refresh_webapi,
    subprocess_run,
    update_source,
    upload_file,
)

WAVEFORM_REGISTRY = 'waveform_person_link'
IMAGE_REGISTRY = 'image_person_link'

@prefect.flow(flow_run_name=generate_flow_run_name)
def mimic_etl(mode: typing.Literal['demo', 'full'] = 'demo',run_qc: bool = True):
    logger = prefect.get_run_logger()
    logger.info(f"Started etl pipeline for transforming the {mode} MIMIC dataset.")
    t0 = datetime.datetime.now()
    if mode == 'demo':
        target_schema = MIMIC_DEMO_SCHEMA
        source_schema = MIMIC_DEMO_SOURCE_SCHEMA
    else:
        target_schema = MIMIC_FULL_SCHEMA
        source_schema = MIMIC_FULL_SOURCE_SCHEMA

    stage_schema = 'stage_' + target_schema
    logger.info("Populating staging area...")
    create_or_replace_schema(
        stage_schema,
        "Staging area for MIMIC Transformation Process",
    )
    load_and_execute_sql_concurrently(
        'cdm-ddl-5_3.sql',
        schema=stage_schema
    )

    clone_tables(
        source_schema,
        MIMIC_TABLES_1,
        schema=stage_schema,
        shallow=True,
        suffix='_mimic',
    )

    clone_tables(
        source_schema,
        MIMIC_TABLES_2,
        schema=stage_schema,
        shallow=True,
        suffix='_mimic',
    )

    insert_tables(
        VOCABULARY_SCHEMA,
        VOCABULARY_TABLES,
        schema=stage_schema,
    )

    logger.info("Building MIMIC...")
    base_etl_dir = os.path.join(ETL_DIR, 'mimic', mode)

    orchestrate_sql_w_dependencies(base_etl_dir, stage_schema)


    if run_qc:
        results_ddl = get_ddl(
            'results',
            dialect='spark',
            schema=stage_schema,
            vocabSchema=stage_schema,
            tempSchema=stage_schema,
        )
        execute_sql("ddl/results", results_ddl)

        subprocess_run(
            [os.path.join(ETL_DIR, 'dqd-sql.R'), stage_schema, '5.3', ETL_DIR],
            cwd='/run/ares',
            check=True,
        )
        base_dqd_dir = os.path.join(ETL_DIR, 'dqd', '5_3')
        orchestrate_sql_w_dependencies(base_dqd_dir, stage_schema)

        subprocess_run(
            [os.path.join(ETL_DIR, 'ares.R'), stage_schema],
            cwd='/run/ares',
            check=True,
        )
        achilles_ddl = get_ddl(
            'achilles',
            dialect='spark',
            schema=stage_schema,
            vocabSchema=stage_schema,
        )
        achilles_ddl = achilles_ddl.replace(
            "WITH insertion_temp",
            ", insertion_temp",
        )
        execute_sql("ddl/achilles", achilles_ddl)
        logger.info("Exporting generated data...")
        create_or_replace_schema(
            target_schema,
            "OMOP CDM and derived tables for TRDW",
        )

        clone_tables(
            stage_schema,
            GENERATED_TABLES_5_3,
            schema=target_schema,
        )
        source_name = "MIMIC"
        source_key = 'mimic'
        priority = 2
        if mode == 'demo':
            source_id = 8
            source_name += " (DEMO)"
            source_key += '-demo'
            priority = 2
        elif mode == 'full':
            source_id = 9
            source_name += " (FULL)"
            source_key += '-full'
            priority = 2
        update_source(
            id=source_id,
            name=source_name,
            key=source_key,
            connection=DATABRICKS_JDBC_URL,
            dialect='spark',
            username='token',
            password=DATABRICKS_ACCESS_TOKEN,
            schema=target_schema,
            priority=priority,
        )
        refresh_webapi()


    t1 = datetime.datetime.now()
    minutes = round((t1 - t0).total_seconds() / 60)
    logger.info(f"Successfully finished ETL pipeline for creating the {mode} MIMIC OMOP Instance in {minutes} minutes.")



def get_mimic_person_map(stage_schema: str = 'stage_trdw_extract_mimicdemo'):
    with databricks_cursor() as c:
        get_pm_sql = f"SELECT person_source_value, person_id FROM ctsi.{stage_schema}.person;"
        c.execute(get_pm_sql)
        dbdata = c.fetchall()
        person_map = {}
        for row in dbdata:
            person_map[row[0]] = row[1]

    return person_map

def upload_processed_registry(df: pd.DataFrame,
                             outfile: str,
                             tablename: str,
                             stage_schema: str):
    df.to_csv(
        outfile,
        index=True
    )
    uploaded_csv = upload_file(
        outfile,
        stage_schema,
    )
    create_registry_upload_sql(tablename,
                            uploaded_csv)
    load_and_execute_sql(
        'multimodal.sql',
        schema=stage_schema,
    )

def create_registry_upload_sql(table_name: str,
                           csv_name: str):
    # This function gets around the fact that databricks adds single quotes to string
    # arguments, which causes the cursor execution to fail when trying to input a table name
    sql_table = f"""CREATE TABLE {table_name} (
                      file_id integer,
                      subject_id string,
                      person_id integer,
                      session_id integer,
                      session_date string,
                      session_time string,
                      source_file string,
                      target_file string
                    )
                    USING CSV
                    LOCATION '{csv_name}'
                    OPTIONS (
                      header 'true',
                      mode 'FAILFAST'
                    );
    """

    with open(os.path.join(ETL_DIR, 'multimodal.sql'), 'w') as file:
        file.write(sql_table)



def parse_and_transform_images(parent_source_dir: str,
                             target_extract_dir: str,
                             stage_schema: str):
    # Function to iterate over the nested directory structure with images,
    # load and process those images, and then output them in a standard format
    # as requested by the CHoRUS Bridge2AI Data Acquisition Team

    subfolders = [f.path for f in os.scandir(parent_source_dir) if f.is_dir()]
    pm = get_mimic_person_map(stage_schema)
    file_tracker = pd.DataFrame(columns=["SUBJECT", "PERSON", "SESSION", "DATE", "TIME", "SRCFILE", "TRGFILE"])
    os.makedirs(target_extract_dir, exist_ok=True)
    for sub in subfolders:
        ssfs = [f.path for f in os.scandir(sub) if f.is_dir()]
        for ssf in ssfs:
            sssfs = [f.path for f in os.scandir(ssf) if f.is_dir()]
            subject_id = os.path.basename(os.path.normpath(ssf))[1:]
            os.makedirs(os.path.join(target_extract_dir, str(pm[subject_id])), exist_ok=True)
            for sssf in sssfs:
                session_id = int(os.path.basename(os.path.normpath(sssf))[1:])
                output_dir = os.path.join(target_extract_dir, str(pm[subject_id]), 'Images')
                os.makedirs(output_dir, exist_ok=True)
                for f in os.listdir(sssf):
                    if f[0] != '.' and f[-4:] == '.dcm':
                        try:
                            input_file = os.path.join(sssf, f)
                            dicom_data = dcmread(input_file)
                            # possibly add processing/deidentification here if needed
                            output_file_id = str(pm[dicom_data.PatientID]) + '_' + \
                                dicom_data.StudyDate + '_' + \
                                str(dicom_data.StudyTime).split('.')[0] + '_' + \
                                dicom_data.Modality + '_' + \
                                str(dicom_data.SeriesNumber).zfill(2) + '_' + \
                                str(dicom_data.InstanceNumber).zfill(3) + '.dcm'
                            output_file = os.path.join(output_dir, output_file_id)
                            dcmwrite(output_file, dicom_data)
                            file_tracker.loc[len(file_tracker.index)] = [subject_id,
                                                                         pm[subject_id],
                                                                         session_id,
                                                                         dicom_data.StudyDate,
                                                                         str(dicom_data.StudyTime).split('.')[0],
                                                                         f,
                                                                         output_file_id]
                        except Exception as e:
                            print(f"Failed to process file: {input_file} due to {e}")

    upload_processed_registry(file_tracker,
                  os.path.join(ETL_DIR, 'images_processed.csv'),
                  IMAGE_REGISTRY,
                  stage_schema
    )


def parse_and_transform_waveforms(parent_source_dir: str,
                                   target_extract_dir: str,
                                   stage_schema: str):
    # Function to iterate over the nested directory structure with waveforms,
    # load and process those signals, and then output them in a standard format
    # as requested by the CHoRUS Bridge2AI Data Acquisition Team

    subfolders = [f.path for f in os.scandir(parent_source_dir) if f.is_dir()]
    pm = get_mimic_person_map(stage_schema)
    file_tracker = pd.DataFrame(columns=["SUBJECT", "PERSON", "SESSION", "DATE", "TIME", "SRCFILE", "TRGFILE"])
    os.makedirs(target_extract_dir, exist_ok=True)
    for sub in subfolders:
        ssfs = [f.path for f in os.scandir(sub) if f.is_dir()]
        for ssf in ssfs:
            sssfs = [f.path for f in os.scandir(ssf) if f.is_dir()]
            subject_id = os.path.basename(os.path.normpath(ssf))[1:]
            os.makedirs(os.path.join(target_extract_dir, str(pm[subject_id])), exist_ok=True)
            for sssf in sssfs:
                session_id = int(os.path.basename(os.path.normpath(sssf)))
                output_dir = os.path.join(target_extract_dir, str(pm[subject_id]), 'Waveforms')
                os.makedirs(output_dir, exist_ok=True)
                for f in sorted(os.listdir(sssf)):
                    if f[0] != '.' and f[-4:] == '.hea' and '_' not in f:
                        with open(os.path.join(sssf, f)) as wav_study_info:
                            all_data = [line.strip() for line in wav_study_info.readlines()]
                            row_cnt = 0
                            running_duration = 0
                            duration_dict = {}
                            offset_dict = {}
                            for row in all_data:
                                row_array = row.split()
                                if len(row_array) == 2 and '#wfdb' not in row_array:
                                    duration_dict[row_array[0]] = row_array[1]
                                    offset_dict[row_array[0]] = running_duration
                                    running_duration = running_duration + int(row_array[1])
                                elif 'subjectid' in row_array:
                                    subject_id = row_array[2]
                                elif row_cnt == 1:
                                    study_date = row_array[5][-4:] + row_array[5][3:4].zfill(2) + row_array[5][
                                                                                                  0:2].zfill(2)
                                    study_time = row_array[4][0:8].replace(':', '')
                                row_cnt += 1
                for f in os.listdir(sssf):
                    if f[0] != '.':
                        input_file = os.path.join(sssf, f)
                        try:
                            base_file_id = str(pm[subject_id]) + '_' + \
                                           str(session_id) + '_' + \
                                           study_date + '_' + \
                                           study_time
                            if '_' in f and f[-4:] != '.hea':
                                output_file_id = base_file_id + '_' + \
                                                 str(offset_dict[f[:-5]]) + '_' + \
                                                 duration_dict[f[:-5]] + '_' + \
                                                 f[-8:]
                            elif '_' in f and f[-4:] == '.hea':
                                output_file_id = base_file_id + '_' + \
                                                 str(offset_dict[f[:-4]]) + '_' + \
                                                 duration_dict[f[:-4]] + '_' + \
                                                 f[-7:]
                            elif f[-7:] == '.csv.gz':  # study header file, processed csv
                                output_file_id = base_file_id + f[-7:]
                            else:  # compressed processed csv
                                output_file_id = base_file_id + f[-4:]
                            output_file = os.path.join(output_dir, output_file_id)
                            # possibly add wfdb file processing here if needed
                            shutil.copyfile(input_file, output_file)
                            file_tracker.loc[len(file_tracker.index)] = [subject_id,
                                                                     pm[subject_id],
                                                                     session_id,
                                                                     study_date,
                                                                     study_time,
                                                                     f,
                                                                     output_file_id]
                        except Exception as e:
                            print(f"Failed to process file: {input_file} due to {e}")

    upload_processed_registry(file_tracker,
                  os.path.join(ETL_DIR, 'waveforms_processed.csv'),
                  WAVEFORM_REGISTRY,
                  stage_schema
    )


def parse_and_transform_omop(mode: str,
                             catalog: str,
                             stage_schema: str,
                             target_extract_dir: str):
    # Function to produce single-person OMOP tables as csv files
    # within the per-person directory structure for CHoRUS B2AI
    base_etl_dir = os.path.join(ETL_DIR, 'extract', mode, 'perperson')
    create_per_person_sql(catalog,
                          stage_schema,
                          base_etl_dir)
    orchestrate_sql_w_dependencies(base_etl_dir, stage_schema)
    pm = get_mimic_person_map(stage_schema)
    for person in pm.values():
        output_dir = os.path.join(target_extract_dir, str(person), 'OMOP')
        os.makedirs(output_dir, exist_ok=True)
        save_tables_as_csv(
            stage_schema,
            [tbl + '__' + str(person) for tbl in MIMIC_EXPORT],
            output_dir,
        )

def create_per_person_sql(
            catalog: str,
            stage_schema: str,
            base_etl_dir: str):
    # Function to write SQL code that creates temporary
    # OMOP tables for each person in the MIMIC dataset
    pm = get_mimic_person_map(stage_schema)
    for table in MIMIC_EXPORT:
        sql_out = ""
        for person in pm.values():
            if table == 'note_nlp':
                single_person_table = f"""
                            CREATE OR REPLACE TABLE {catalog}.{stage_schema}.{table}__{person} AS (
                            SELECT nlp.* FROM {catalog}.{stage_schema}.note_nlp nlp
                            INNER JOIN {catalog}.{stage_schema}.note n ON nlp.note_id = n.note_id 
                            WHERE n.person_id = {person}
                            );
                            """
            else:
                single_person_table = f"""
                            CREATE OR REPLACE TABLE {catalog}.{stage_schema}.{table}__{person} AS (
                            SELECT * FROM {catalog}.{stage_schema}.{table} WHERE person_id = {person}
                            );
                            """
            sql_out = sql_out + "\n" + single_person_table
            if table == 'procedure_occurrence':
                insert_wf_sql = f"""
                INSERT INTO {catalog}.{stage_schema}.{table}__{person}
                SELECT
                    file_id + 2000000000 AS procedure_occurrence_id,
                    CAST(person_id AS integer) AS person_id,
                    CAST(4141651 AS integer) AS procedure_concept_id, -- Measuring and Monitoring Procedure
                    CAST(to_date(session_date, 'yyyymmdd') AS DATE) AS procedure_date,
                    CAST(concat(substr(session_date,1,4),
                                                 '-',substr(session_date,5,2),
                                                 '-',substr(session_date,7,2),
                                                 ' ',substr(session_time,1,2),
                                                 ':',substr(session_time,3,2),
                                                 ':',substr(session_time,5,2)) AS TIMESTAMP) AS procedure_datetime,
                    CAST(NULL AS DATE) AS procedure_end_date,
                    CAST(NULL AS TIMESTAMP) AS procedure_end_datetime,
                    CAST(32880 AS integer) AS procedure_type_concept_id,
                    CAST(NULL AS integer) AS modifier_concept_id,
                    CAST(NULL AS integer) AS quantity,
                    CAST(NULL AS integer) AS provider_id,
                    CAST(NULL AS integer) AS visit_occurrence_id,
                    CAST(NULL AS integer) AS visit_detail_id,
                    CAST(target_file AS STRING) AS procedure_source_value,
                    CAST(NULL AS integer) AS procedure_source_concept_id,
                    CAST(NULL AS VARCHAR(50)) AS modifier_source_value
                FROM
                    {catalog}.{stage_schema}.{WAVEFORM_REGISTRY} WHERE person_id = {person};
                """
                insert_img_sql = f"""
                    INSERT INTO {catalog}.{stage_schema}.{table}__{person}
                    SELECT
                        file_id + 2001000000 AS procedure_occurrence_id,
                        CAST(person_id AS integer) AS person_id,
                        CAST(4180938 AS integer) AS procedure_concept_id, -- Imaging Procedure
                        CAST(to_date(session_date, 'yyyymmdd') AS DATE) AS procedure_date,
                        CAST(concat(substr(session_date,1,4),
                                                 '-',substr(session_date,5,2),
                                                 '-',substr(session_date,7,2),
                                                 ' ',substr(session_time,1,2),
                                                 ':',substr(session_time,3,2),
                                                 ':',substr(session_time,5,2)) AS TIMESTAMP) AS procedure_datetime,
                        CAST(NULL AS DATE) AS procedure_end_date,
                        CAST(NULL AS TIMESTAMP) AS procedure_end_datetime,
                        CAST(32880 AS integer) AS procedure_type_concept_id,
                        CAST(NULL AS integer) AS modifier_concept_id,
                        CAST(NULL AS integer) AS quantity,
                        CAST(NULL AS integer) AS provider_id,
                        CAST(NULL AS integer) AS visit_occurrence_id,
                        CAST(NULL AS integer) AS visit_detail_id,
                        CAST(target_file AS STRING) AS procedure_source_value,
                        CAST(NULL AS integer) AS procedure_source_concept_id,
                        CAST(NULL AS VARCHAR(50)) AS modifier_source_value
                    FROM
                        {catalog}.{stage_schema}.{IMAGE_REGISTRY} WHERE person_id = {person};
                    """
                sql_out = sql_out + '\n' + insert_wf_sql + '\n' + insert_img_sql
        with open(os.path.join(base_etl_dir, table + '-mimic.sql'), 'w') as file:
            file.write(sql_out)

