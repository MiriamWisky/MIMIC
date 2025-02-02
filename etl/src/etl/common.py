from itertools import chain
import contextlib
import datetime
from typing import List
import os
import subprocess
import urllib.request
import yaml

import azure.identity
import azure.storage.blob
import psycopg
import pyarrow, pyarrow.csv
import sqlparse

from .config import (
    ETL_DIR,
    PGHOST,
    PGPORT,
    PGUSER,
    PGPASSWORD,
)

VOCABULARY_TABLES = [
    'concept',
    'vocabulary',
    'domain',
    'concept_class',
    'concept_relationship',
    'relationship',
    'concept_synonym',
    'concept_ancestor',
    'drug_strength'
    # ,
    # 'concept_recommended',
    # 'source_to_concept_map'
]

CDM_TABLES = [
    'person',
    'observation_period',
    'visit_occurrence',
    'visit_detail',
    'condition_occurrence',
    'drug_exposure',
    'procedure_occurrence',
    'device_exposure',
    'measurement',
    'observation',
    'death',
    'note',
    'note_nlp',
    'specimen',
    'fact_relationship',
    'location',
    'care_site',
    'provider',
    'payer_plan_period',
    'cost',
    'drug_era',
    'dose_era',
    'condition_era',
    'episode',
    'episode_event',
    'metadata',
    'cdm_source',
    'concept',
    'vocabulary',
    'domain',
    'concept_class',
    'concept_relationship',
    'relationship',
    'concept_synonym',
    'concept_ancestor',
    'source_to_concept_map',
    'drug_strength',
    'cohort',
    'cohort_definition',
]

DELIVERED_TABLES = [
    'person',
    'observation_period',
    'visit_occurrence',
    'visit_detail',
    'condition_occurrence',
    'drug_exposure',
    'procedure_occurrence',
    'device_exposure',
    'measurement',
    'observation',
    'death',
    'note',
    'note_nlp',
    'specimen',
    'drug_era',
    'dose_era',
    'condition_era',
]

SITE_LIST = ['columbia',
             'duke',
             'emory',
             'mgh',
             'mit',
             'mayo',
             'nationwide',
             'newmexico',
             'ucla',
             'ucsf',
             'florida',
             'pittsburgh',
             'seattle',
             'virginia']

@contextlib.contextmanager
def postgresql_cursor():
    
    pgdbname = os.environ['MODEPGDB']
    with psycopg.connect(f'postgresql://{PGUSER}:{PGPASSWORD}@{PGHOST}:{PGPORT}/{pgdbname}') as conn:
        with conn.cursor() as c:
            yield c
def subprocess_run(command, input=None, cwd=None, env=None, check=True):
    
    print(' '.join(command))
    if isinstance(input, str):
        input = input.encode('utf-8')
    return subprocess.run(command, input=input, cwd=cwd, env=env, check=check)


def execute_sql(
        name: str,
        qs: str|list[str],
        args: dict|None = None,
        schema: str|None = None,
        cursor = None) -> None:
    """Execute a sequence of SQL queries."""
    
    if isinstance(qs, str):
        qs = sqlparse.split(qs)
    if schema is not None:
        qs = [f"SET search_path TO {schema};"] + qs
    if cursor is not None:
        for q in qs:
            print(q)
            cursor.execute(q)
    else:
        with postgresql_cursor() as c:
            for q in qs:
                print(q)
                c.execute(q)


def load_sql(filename: str) -> list[str]:
    """Load SQL queries from a file."""
    with open(os.path.join(ETL_DIR, filename)) as f:
        return sqlparse.split(f.read())

def load_and_execute_sql(
        filename: str,
        args: dict|None = None,
        schema: str|None = None,
        cursor = None) -> None:
    execute_sql(filename, load_sql(filename), args, schema, cursor)


def create_or_replace_schema(schema: str, comment: str) -> None:
    """Create a schema with the given name, replacing any existing schema."""
    execute_sql(
        f"SCHEMA {schema}",
        [
            f"DROP SCHEMA IF EXISTS {schema} CASCADE;",
            f"CREATE SCHEMA {schema};",
        ],
    )


def archive_and_rename_schema(src_schema: str, trg_schema: str, date_suffix: str) -> None:
    run_suffix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    execute_sql(
        f"RENAME SCHEMA {src_schema} TO {trg_schema}",
        [
            f"ALTER SCHEMA {trg_schema} RENAME TO {trg_schema}_{date_suffix}_{run_suffix};",
            f"ALTER SCHEMA {src_schema} RENAME TO {trg_schema};",
        ]
    )


def rename_schema(src_schema: str, trg_schema: str) -> None:
    execute_sql(
        f"RENAME SCHEMA {src_schema} TO {trg_schema}",
        [
            f"ALTER SCHEMA {src_schema} RENAME TO {trg_schema};",
        ]
    )


def drop_schema(schema) -> None:
    execute_sql(
        f"DROP {schema}",
        [
            f"DROP SCHEMA {schema} CASCADE;",
        ]
    )


def get_schemas_as_list() -> List[str]:
    with postgresql_cursor() as c:
        c.execute("SELECT schema_name FROM information_schema.schemata;")
        return [row[0] for row in c.fetchall()]


def view_tables(
        source_schema: str,
        source_tables: list[str],
        schema: str,
        prefix: str = '',
        suffix: str = '') -> None:
    """Creates views of tables in a different schema."""
    names = []
    qs = []
    for table in source_tables:
        names.append(f"CREATE VIEW OF {table} IN {source_schema}")
        source_name = f"{source_schema}.{table}"
        target_name = f"{schema}.{prefix + table + suffix}"
        qs.append(
            f"CREATE VIEW {target_name} AS \n"
            f"SELECT * FROM  {source_name};"
        )
    execute_sql(names, qs)


def pipe_table_transfer(
        source_dbname: str,
        source_schema: str,
        source_tables: list[str],
        trg_schema: str,
        trg_dbname: str,
        prefix: str = '',
        suffix: str = '') -> None:
    """Creates tables in a different database using the dblink extension."""
    names = []
    qs = []
    tmp_file = f'/tmp/transfer-{source_dbname}.sh'
    with open(tmp_file, 'w') as t:
        t.write("#!/bin/bash\n")

    for table in source_tables:
        names.append(f"TRANSFER {table} IN {source_dbname} to {trg_dbname}")
        source_name = f"{source_schema}.{table}"
        target_name = f"{trg_schema}.{prefix + table + suffix}"
        tmp_shell = f"""
                psql -d {source_dbname} -U {PGUSER} \
                -c \"\copy {source_name} TO STDOUT CSV\" | \
                psql -d {trg_dbname} -U {PGUSER} \
                -c \"\copy {target_name} FROM STDIN CSV\"
                """
        with open(tmp_file, 'a') as t:
            t.write(tmp_shell)
            t.write("\n")

    subprocess_run(['chmod', '+x', tmp_file], check=True)
    subprocess_run([tmp_file], check=True)


def load_yaml(
        base_path_dev: str,
        file_name: str = 'orchestrate.yaml') -> dict:
    with open(os.path.join(base_path_dev, file_name), "r") as stream:
        orchestration = yaml.safe_load(stream)
    return orchestration


def extract_dep_layers(deps, script):
    dep_list = set(deps[script])
    rval = set()
    while dep_list:
        dep_script = dep_list.pop()
        if dep_script != script:
            rval.add(dep_script)
            to_add = set(deps.get(dep_script,[]))
            dep_list.update(to_add.difference(rval))
    return sorted(rval)


def process_yaml(
        orchestration: dict,
        base_path_dev: str) -> dict:
    dep_compact = {}
    dep_full = {}
    for transform in orchestration['transformations']['orchestrate']['sequence']:
        for idx, script in enumerate(transform['sql']):
            script_path = os.path.join(base_path_dev, transform['sub_path'], script)
            file_list = [
                script_path
            ]
            if transform['depends']:
                dep_list = [
                    os.path.join(base_path_dev, dep['sub_path'], script)
                    for dep in transform['depends'] for script in dep['sql']
                ]
            else:
                dep_list = []
            dep_compact[script_path] = dep_list
            if not transform['parallel'] and idx > 0:
                dep_compact[script_path].append(os.path.join(base_path_dev, transform['sub_path'], transform['sql'][idx - 1]))
    for script in dep_compact:
        dep_full[script] = extract_dep_layers(dep_compact, script)
    return dep_full


def create_yaml_para_groupings(
        deps: dict) -> list:

    groups = []
    completed = []
    while len(completed) < len(deps):
        next_group = [
            file
            for file in deps.keys()
            if file not in completed and all(dep in completed for dep in deps[file])]
        assert next_group
        groups.append(next_group)
        completed.extend(next_group)
    return groups

def orchestrate_sql_w_dependencies(
        base_etl_dir: str,
        stage_schema: str
) -> None:
    # Function executes multiple sql scripts concurrently
    my_orchest = load_yaml(
        base_etl_dir,
        'orchestrate.yaml'
    )
    dependencies = process_yaml(
        my_orchest,
        base_etl_dir
    )
    grouped_filenames = create_yaml_para_groupings(
        dependencies
    )
    for filenames in grouped_filenames:
        for filename in filenames:
            execute_sql(
                filename,
                load_sql(filename),
                schema=stage_schema
            )


def get_last_cdm_release_date(schema: str) -> str:
    with postgresql_cursor() as c:
        get_rd_sql = f"SELECT TO_CHAR(cdm_release_date, 'YYYYMMDD') AS release_date_str FROM {schema}.cdm_source;"
        c.execute(get_rd_sql)
        release_date_str = c.fetchone()

    return release_date_str[0]



def copy_db_data_from_stdin(
        schema: str,
        table: str,
        copy_path: str,
        delim: str,
        header = True,
) -> None:
    hd = '2' if header else '1'
    pgdbname = os.environ['MODEPGDB']
    tmp_file = '/tmp/load.sh'
    tmp_shell = f"""
    tail -q -n +{hd} {copy_path} | 
    psql -d {pgdbname} -h {PGHOST} -U {PGUSER} -p {PGPORT} 
    -c \"\copy {schema}.{table} FROM STDIN CSV DELIMITER E'{delim}'\"
    """
    with open(tmp_file, 'w') as t:
        t.write("#!/bin/bash\n")
        t.write(tmp_shell)
    subprocess_run(['chmod', '+x', tmp_file], check=True)
    subprocess_run([tmp_file], check=True)

def ingest_omop(
        tables: list[str],
        schema: str,
        copy_path: str,
        delim: str,
        header = True,
) -> None:
    
    for table in tables:
        try:
            new_path = os.path.join(copy_path, table + "__*.csv")
            copy_db_data_from_stdin(schema, table, new_path, delim, header)
            print(f'Copied {table} from {new_path} to database')
        except Exception as e:
            print(f'Unable to copy {table} from {copy_path} to database: {e}')
            continue


def update_source(
        id: int,
        name: str,
        key: str,
        connection: str,
        dialect: str,
        username: str,
        password: str,
        schema: str,
        priority: int = 1,
) -> None:
    """Add information about a CDM dataset to WebAPI database"""
    with postgresql_cursor() as c:
        c.execute(
            """
            DELETE FROM source_daimon
            WHERE source_id = %s
            """,
            (id,),
        )
        c.execute(
            """
            INSERT INTO source (source_id, source_name, source_key, source_connection, source_dialect, username, password, is_cache_enabled)
            VALUES (%s, %s, %s, %s, %s, %s, %s, false)
            ON CONFLICT (source_id) DO UPDATE
            SET
              source_name = EXCLUDED.source_name,
              source_key = EXCLUDED.source_key,
              source_connection = EXCLUDED.source_connection,
              source_dialect = EXCLUDED.source_dialect,
              username = EXCLUDED.username,
              password = EXCLUDED.password,
              is_cache_enabled = EXCLUDED.is_cache_enabled
            """,
            (id, name, key, connection, dialect, username, password),
        )
        c.executemany(
            """
            INSERT INTO source_daimon (source_daimon_id, source_id, daimon_type, table_qualifier, priority)
            VALUES (%s, %s, %s, %s, %s)
            """,
            # Daimon types (https://github.com/OHDSI/WebAPI/blob/master/src/main/java/org/ohdsi/webapi/source/SourceDaimon.java#L45):
            # 0 = CDM, 1 = Vocabulary, 2 = Results, 3 = CEM, 4 = CEMResults, 5 = Temp
            [
                (10 * id + 0, id, 0, schema, priority),
                (10 * id + 1, id, 1, schema, priority),
                (10 * id + 2, id, 2, schema, priority),
                (10 * id + 5, id, 5, schema, priority),
            ],
        )


