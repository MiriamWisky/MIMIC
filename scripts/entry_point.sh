docker exec -it postgres_container psql -U postgres -f /data/scripts/create.sql && \
docker exec -it postgres_container psql -U postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=/data/mimic-iv2 -f /data/scripts/load_gz.sql && \
docker exec -it postgres_container psql -U postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=/data/mimic-iv2 -f /data/scripts/constraint.sql && \
docker exec -it postgres_container psql -U postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=/data/mimic-iv2 -f /data/scripts/index.sql