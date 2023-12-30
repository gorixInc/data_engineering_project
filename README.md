**To run the project:**

1. Build the docker images:
   ```
   docker compose build
   ```

2. Start the containers in detached mode:
   ```
   docker compose up -d
   ```

3. Upgrade the Airflow database to the latest version:
   ```
   docker-compose exec airflow-webserver alembic upgrade head
   ```

**postgres db Access:**
- Host: `postgres`
- port: `5432`
- User: `airflow`
- Password: `airflow`

**pgAdmin Access:**
- URL: `localhost:5050`
- User: `admin@admin.com`
- Password: `root`

**Airflow Webserver Access:**

- URL: `localhost:8080`
- User: `airflow`
- Password: `airflow`

**Airflow Connections:**

- ID: `postgres_main`
- Type: `Postgres`
- Host: `postgres`
- Schema: `main`
- Login: `airflow`
- Password: `airflow`


**Usage**
- Put raw data files into data/raw_data/input