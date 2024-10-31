psql -d postgres -f ./create.sql
psql -d postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=mimic-iv2 -f ./load_gz.sql
psql -d postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=mimic-iv2 -f ./constraint.sql
psql -d postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=mimic-iv2 -f ./index.sql