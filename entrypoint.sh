# for postgreSQL container:

# docker-compose up -d

# docker exec -it postgres_container psql -U postgres -f /data/scripts/create.sql && \
# docker exec -it postgres_container psql -U postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=/data/mimic-iv2 -f /data/scripts/load_gz.sql && \
# docker exec -it postgres_container psql -U postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=/data/mimic-iv2 -f /data/scripts/constraint.sql && \
# docker exec -it postgres_container psql -U postgres -v ON_ERROR_STOP=1 -v mimic_data_dir=/data/mimic-iv2 -f /data/scripts/index.sql

# docker exec -it postgres_container psql -U postgres -f /data/scripts/ddl_voc_5_3_1.sql && \
# docker exec -it postgres_container psql -U postgres -f /data/scripts/load_vocabulary_data.sql



psql -h <HOST> -p <PORT> -U <USER> -d <DATABASE> \
     -f /scripts/create.sql


psql -h <HOST> -p <PORT> -U <USER> -d <DATABASE> \
     -v ON_ERROR_STOP=1 \
     -v mimic_data_dir=/mimic-iv-2.2 \
     -f /scripts/load.sql


psql -h <HOST> -p <PORT> -U <USER> -d <DATABASE> \
     -v ON_ERROR_STOP=1 \
     -v mimic_data_dir=/mimic-iv-2.2 \
     -f /scripts/constraint.sql


psql -h <HOST> -p <PORT> -U <USER> -d <DATABASE> \
     -v ON_ERROR_STOP=1 \
     -v mimic_data_dir=/mimic-iv-2.2 \
     -f /scripts/index.sql


psql -h <HOST> -p <PORT> -U <USER> -d <DATABASE> \
     -f /scripts/ddl_voc_5_3_1.sql


psql -h <HOST> -p <PORT> -U <USER> -d <DATABASE> \
     -f /scripts/load_vocabulary_data.sql
