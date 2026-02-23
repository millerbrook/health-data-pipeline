# Airflow Health Data Pipeline: Quick Start Guide

## 1. Prerequisites
- Python 3.11
- Docker (for local Postgres, if needed)
- Neon database credentials (for cloud DB)

## 2. Environment Setup
1. Clone the repository and navigate to the project directory:
   ```bash
   cd ~/airflow_tutorial
   ```
2. Ensure `.env.local` contains your Neon connection string:
   ```bash
   export AIRFLOW_CONN_HEALTH_DB="postgresql://<user>:<password>@<neon_host>/<dbname>?sslmode=require&channel_binding=require"
   ```
   (Replace with your actual Neon credentials.)

## 3. Start the Development Environment
1. Open a terminal and run:
   ```bash
   bash dev.sh
   ```
   This will:
   - Activate the Python virtual environment
   - Load environment variables
   - Start local Postgres (if configured)

2. In the same terminal, check the connection variable:
   ```bash
   echo $AIRFLOW_CONN_HEALTH_DB
   ```
   It should print your Neon connection string.

## 4. Start Airflow
Open two terminals (or tabs):

### Terminal 1: Start the Airflow Webserver
```bash
bash dev.sh
airflow webserver --port 8080
```
- Access the Airflow UI at: http://localhost:8080

### Terminal 2: Start the Airflow Scheduler
```bash
bash dev.sh
airflow scheduler
```

## 5. Run the DAG
- In the Airflow UI, trigger the `health_data_pipeline` DAG manually or set a schedule.
- Monitor logs for task progress and errors.

## 6. Verify Data in Neon
- Connect to your Neon database using `psql` or a GUI.
- Example:
  ```bash
  psql "<your Neon connection string>"
  SELECT * FROM nutrition_daily ORDER BY date DESC LIMIT 5;
  ```

## 7. Start the Streamlit Dashboard (Optional)
```bash
bash dev.sh
streamlit run dashboard.py
```

---

**Tips:**
- Always start Airflow processes from a shell where `bash dev.sh` was run.
- If you change `.env.local`, restart your terminals and Airflow processes.
- For troubleshooting, check Airflow task logs and confirm the correct DB connection string is loaded.
