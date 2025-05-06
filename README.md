## Getting Started with the ETL Process

MIMIC-to-OMOP ETL using PostgreSQL.

### 1. Configure database and schema settings

Edit the following files:

- `etl/.env.example` — database credentials and host info
- `etl/src/etl/config.py` — target schema names (e.g. `omopcdm`, `vocabulary`, etc.)

### 2. Run the ETL pipeline

To execute the full ETL process using your configured PostgreSQL database, run:

```bash
docker-compose up --build
```

This will start the ETL process using your configured database and schemas.

### 3. (Optional) Build MIMIC + vocabulary database from CSVs

To populate a PostgreSQL database from raw CSV files, see the example Bash commands in [`entrypoint.sh`](entrypoint.sh).

This script includes steps for:

- Creation of MIMIC-IV tables
- Loading MIMIC-IV data from CSV files
- Loading OMOP vocabularies (from [Athena](https://athena.ohdsi.org/))

Update connection info and file paths as needed for your system.

### Folder Overview

| Path                        | Description                                                  |
|-----------------------------|--------------------------------------------------------------|
| `etl/src/etl/`              | Main Python ETL pipeline                                     |
| `scripts/`                  | SQL scripts to load MIMIC-IV data into PostgreSQL               |
| `handle_omop_data/`         | Post-ETL: adds OMOP primary keys, foreign keys, and indexes          |

---

### Code Provenance & Licensing

| Path                     | Source repo / commit                                             | License     |
|--------------------------|------------------------------------------------------------------|-------------|
| `scripts/*.sql`          | [OHDSI/MIMIC](https://github.com/OHDSI/MIMIC) | Apache 2.0  |
| `handle_omop_data/*.sql` | [OHDSI/CommonDataModel](https://github.com/OHDSI/CommonDataModel/tree/v5.4.0/inst/ddl/5.3/postgresql) | Apache 2.0  |

---

### Authors

Maintained by:

**Miriam Wisky** and **Miriam Allalouf**  
*Software Engineering Department, Azrieli College of Engineering, Jerusalem*