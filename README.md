# Personal Health Data Pipeline

An Apache Airflow pipeline for processing MyFitnessPal nutrition data and Garmin Connect exercise data.

## Setup

1. Install dependencies:
   ```bash
   ./dev.sh
   uv pip install -e .

## Add New Data to Neon (Simple Checklist)

Use this whenever you add new CSV/PDF exports and want the Streamlit app to show the latest data.

1. Drop new files into the local folders:
   - data/exports/myfitnesspal/
   - data/exports/garmin/
   - data/exports/fitindex/
   - data/processed/lab_results/

2. Make sure your local Airflow points to Neon:
   ```bash
   export AIRFLOW_CONN_HEALTH_DB="postgresql://neondb_owner:YOUR_PASSWORD@YOUR_NEON_HOST/neondb?sslmode=require"
   ```

3. Run the ETL tasks locally (writes into Neon):
   ```bash
   airflow tasks test health_data_pipeline extract_nutrition 2026-02-15
   airflow tasks test health_data_pipeline transform_nutrition 2026-02-15
   airflow tasks test health_data_pipeline load_nutrition 2026-02-15

   airflow tasks test health_data_pipeline extract_activities 2026-02-15
   airflow tasks test health_data_pipeline transform_activities 2026-02-15
   airflow tasks test health_data_pipeline load_activities 2026-02-15

   airflow tasks test health_data_pipeline extract_fitindex 2026-02-15
   airflow tasks test health_data_pipeline transform_fitindex 2026-02-15
   airflow tasks test health_data_pipeline load_body_composition 2026-02-15

   airflow tasks test health_data_pipeline extract_lab 2026-02-15
   airflow tasks test health_data_pipeline transform_lab 2026-02-15
   airflow tasks test health_data_pipeline load_lab 2026-02-15

   airflow tasks test health_data_pipeline data_quality_check 2026-02-15
   ```

4. Refresh your Streamlit app. It reads directly from Neon.

Tip: Save the Neon connection in .env.local so dev.sh loads it automatically.
