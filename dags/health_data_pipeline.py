from datetime import datetime, timedelta
from pathlib import Path
import sys
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor

# Add project root to Python path so we can import from utils
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from utils.lab_parser import parse_all_lab_pdfs

def task_failure_alert(context):
    """Called when a task fails."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    exception = context.get('exception')
    log_url = context.get('task_instance').log_url
    
    print("\n" + "="*60)
    print("❌ TASK FAILED")
    print("="*60)
    print(f"DAG: {dag_id}")
    print(f"Task: {task_id}")
    print(f"Exception: {exception}")
    print(f"Log URL: {log_url}")
    print("="*60 + "\n")


def task_retry_alert(context):
    """Called when a task is retrying."""
    task_instance = context['task_instance']
    print(f"\n⚠️  RETRYING: {task_instance.task_id} (attempt {task_instance.try_number})\n")


def task_success_alert(context):
    """Called when a task succeeds (useful after retries)."""
    task_instance = context['task_instance']
    if task_instance.try_number > 1:
        print(f"\n✅ RECOVERED: {task_instance.task_id} succeeded after {task_instance.try_number} attempts\n")

# Configuration
PROJECT_DIR = Path(__file__).parent.parent
MYFITNESSPAL_DIR = PROJECT_DIR / "data" / "exports" / "myfitnesspal"
GARMIN_DIR = PROJECT_DIR / "data" / "exports" / "garmin"
FITINDEX_DIR = PROJECT_DIR / "data" / "exports" / "fitindex"
LAB_DIR = PROJECT_DIR / "data" / "exports" / "lab_results"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

default_args = {
    'owner': 'brook',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # Increased from 1 to 2
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,  # Wait longer each retry
    'max_retry_delay': timedelta(minutes=30),  # Cap retry delay
    'on_failure_callback': task_failure_alert,
    'on_retry_callback': task_retry_alert,
    'on_success_callback': task_success_alert,
}

def extract_nutrition_data(**context):
    """Extract MyFitnessPal nutrition data from CSV."""
    try:
        csv_files = list(MYFITNESSPAL_DIR.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No MyFitnessPal CSV files found in {MYFITNESSPAL_DIR}")
        
        # Get the most recent file
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        print(f"Processing nutrition file: {latest_file}")
        
        # Read CSV with validation
        df = pd.read_csv(latest_file)
        
        # Validate required columns exist
        required_cols = ['Date', 'Meal', 'Calories', 'Protein (g)', 'Carbohydrates (g)', 'Fat (g)']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}. Found columns: {df.columns.tolist()}")
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = df['Date'].isna().sum()
        if invalid_dates > 0:
            print(f"⚠️  WARNING: Found {invalid_dates} rows with invalid dates (will be skipped)")
            df = df.dropna(subset=['Date'])
        
        if len(df) == 0:
            raise ValueError("No valid data after date parsing")
        
        # Data quality checks
        print(f"Loaded {len(df)} nutrition records")
        print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        print(f"Meals: {df['Meal'].value_counts().to_dict()}")
        
        # Store in XCom for next task
        output_path = PROCESSED_DIR / f"nutrition_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_path, index=False)
        context['ti'].xcom_push(key='nutrition_file', value=str(output_path))
        
        return len(df)
        
    except FileNotFoundError as e:
        print(f"❌ File Error: {e}")
        raise  # Re-raise to mark task as failed
    except pd.errors.ParserError as e:
        print(f"❌ CSV Parsing Error: {e}")
        print(f"File may be corrupted or wrong format: {latest_file}")
        raise
    except ValueError as e:
        print(f"❌ Validation Error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected Error in extract_nutrition_data: {type(e).__name__}: {e}")
        raise

def extract_activity_data(**context):
    """Extract Garmin activity data from CSV."""
    try:
        csv_files = list(GARMIN_DIR.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No Garmin CSV files found in {GARMIN_DIR}")
        
        # Get the most recent file
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        print(f"Processing activity file: {latest_file}")
        
        # Read CSV
        df = pd.read_csv(latest_file)
        
        # Validate required columns
        required_cols = ['Activity Type', 'Date', 'Calories']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}. Found columns: {df.columns.tolist()}")
        
        # Convert Date column to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = df['Date'].isna().sum()
        if invalid_dates > 0:
            print(f"⚠️  WARNING: Found {invalid_dates} rows with invalid dates (will be skipped)")
            df = df.dropna(subset=['Date'])
        
        if len(df) == 0:
            raise ValueError("No valid data after date parsing")
        
        # Clean numeric columns (remove commas from numbers like "2,150")
        numeric_cols = ['Distance', 'Calories', 'Steps']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace(',', '').replace('--', pd.NA)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Data quality checks
        print(f"Loaded {len(df)} activity records")
        print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        print(f"Activity types: {df['Activity Type'].value_counts().to_dict()}")
        
        # Store in XCom for next task
        output_path = PROCESSED_DIR / f"activities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_path, index=False)
        context['ti'].xcom_push(key='activity_file', value=str(output_path))
        
        return len(df)
        
    except FileNotFoundError as e:
        print(f"❌ File Error: {e}")
        raise
    except pd.errors.ParserError as e:
        print(f"❌ CSV Parsing Error: {e}")
        print(f"File may be corrupted or wrong format: {latest_file}")
        raise
    except ValueError as e:
        print(f"❌ Validation Error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected Error in extract_activity_data: {type(e).__name__}: {e}")
        raise

def extract_fitindex_data(**context):
    """Extract FitIndex body composition data from CSV."""
    try:
        csv_files = list(FITINDEX_DIR.glob("*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No FitIndex CSV files found in {FITINDEX_DIR}")
        
        # Get the most recent file
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        print(f"Processing FitIndex file: {latest_file}")
        
        # Read CSV
        df = pd.read_csv(latest_file)
        
        # Validate required columns exist
        required_cols = ['Time of Measurement', 'Weight(lb)', 'BMI', 'Body Fat(%)']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}. Found columns: {df.columns.tolist()}")
        
        # Parse the date/time column (format: "January 31, 2026 6:32:52 AM")
        df['Date'] = pd.to_datetime(df['Time of Measurement'], errors='coerce')
        
        # Check for invalid dates
        invalid_dates = df['Date'].isna().sum()
        if invalid_dates > 0:
            print(f"⚠️  WARNING: Found {invalid_dates} rows with invalid dates (will be skipped)")
            df = df.dropna(subset=['Date'])
        
        if len(df) == 0:
            raise ValueError("No valid data after date parsing")
        
        # Extract just the date (ignore time) since we want one measurement per day
        df['Date'] = df['Date'].dt.date
        
        # If multiple measurements per day, keep the latest one
        df = df.sort_values('Time of Measurement').drop_duplicates(subset=['Date'], keep='last')
        
        # Data quality checks
        print(f"Loaded {len(df)} body composition records")
        print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        print(f"Weight range: {df['Weight(lb)'].min():.1f} - {df['Weight(lb)'].max():.1f} lbs")
        
        # Store in XCom for next task
        output_path = PROCESSED_DIR / f"fitindex_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_path, index=False)
        context['ti'].xcom_push(key='fitindex_file', value=str(output_path))
        
        return len(df)
        
    except FileNotFoundError as e:
        print(f"❌ File Error: {e}")
        raise
    except pd.errors.ParserError as e:
        print(f"❌ CSV Parsing Error: {e}")
        print(f"File may be corrupted or wrong format: {latest_file}")
        raise
    except ValueError as e:
        print(f"❌ Validation Error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected Error in extract_fitindex_data: {type(e).__name__}: {e}")
        raise

def extract_lab_results(**context):
    """Extract lab results from PDF files using the lab_parser utility."""
    try:
        pdf_files = list(LAB_DIR.glob("*.pdf"))
        
        if not pdf_files:
            raise FileNotFoundError(f"No lab report PDF files found in {LAB_DIR}")
        
        print(f"Found {len(pdf_files)} lab report PDFs to process")
        
        # Parse all PDFs in the lab directory
        df = parse_all_lab_pdfs(LAB_DIR)
        
        if len(df) == 0:
            raise ValueError("No lab results extracted from PDFs")
        
        # Data quality checks
        print(f"Extracted {len(df)} lab test results")
        print(f"Date range: {df['test_date'].min()} to {df['test_date'].max()}")
        print(f"Unique tests: {df['test_name'].nunique()}")
        print(f"Panels: {df['panel_name'].unique()}")
        
        # Store in XCom for next task
        output_path = PROCESSED_DIR / f"lab_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        df.to_parquet(output_path, index=False)
        context['ti'].xcom_push(key='lab_results_file', value=str(output_path))
        
        return len(df)
        
    except FileNotFoundError as e:
        print(f"❌ File Error: {e}")
        raise
    except ValueError as e:
        print(f"❌ Validation Error: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected Error in extract_lab_results: {type(e).__name__}: {e}")
        raise

def transform_lab_results(**context):
    """Validate and prepare lab results for loading."""
    lab_results_file = context['ti'].xcom_pull(task_ids='extract_lab', key='lab_results_file')
    df = pd.read_parquet(lab_results_file)
    
    print(f"Transforming {len(df)} lab test results")
    
    # Ensure test_date is datetime
    df['test_date'] = pd.to_datetime(df['test_date'], errors='coerce')
    
    # Validate required columns
    required_cols = ['test_date', 'panel_name', 'test_name', 'is_abnormal']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Ensure boolean is_abnormal
    df['is_abnormal'] = df['is_abnormal'].astype(bool)
    
    # Fill nulls in optional text fields
    text_cols = ['result_text', 'result_flag', 'result_unit', 'reference_range_text', 'notes']
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].fillna('')
    
    # Sort by test date and test name
    df = df.sort_values(['test_date', 'test_name']).reset_index(drop=True)
    
    print(f"Transformed {len(df)} lab records")
    print(f"  Abnormal results: {df['is_abnormal'].sum()}")
    print(f"  Panels: {df['panel_name'].nunique()}")
    print(f"  Unique tests: {df['test_name'].nunique()}")
    
    # Store for loading
    output_path = PROCESSED_DIR / f"lab_results_transformed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_path, index=False)
    context['ti'].xcom_push(key='lab_results_transformed_file', value=str(output_path))
    
    return len(df)

def load_lab_results(**context):
    """Load lab results into PostgreSQL."""
    lab_results_file = context['ti'].xcom_pull(task_ids='transform_lab', key='lab_results_transformed_file')
    df = pd.read_parquet(lab_results_file)
    
    print(f"Loading {len(df)} lab test results")
    
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='health_db')
    
    # Track inserts vs updates
    inserted = 0
    skipped = 0  # Duplicate UNIQUE(test_date, test_name) constraint
    
    # Helper function to convert NaN to None for SQL NULL compatibility
    def clean_value(val):
        if pd.isna(val):
            return None
        return val
    
    # Insert row by row (lab_results uses UNIQUE constraint, not upsert friendly)
    for _, row in df.iterrows():
        try:
            hook.run("""
                INSERT INTO lab_results (
                    test_date, panel_name, test_name, result_value, result_text,
                    result_flag, result_unit, reference_range_text,
                    reference_range_low, reference_range_high, is_abnormal, notes
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, parameters=(
                row['test_date'].strftime('%Y-%m-%d') if pd.notna(row['test_date']) else None,
                row['panel_name'],
                row['test_name'],
                clean_value(row.get('result_value')),
                clean_value(row.get('result_text')),
                clean_value(row.get('result_flag')),
                clean_value(row.get('result_unit')),
                clean_value(row.get('reference_range_text')),
                clean_value(row.get('reference_range_low')),
                clean_value(row.get('reference_range_high')),
                row['is_abnormal'],
                clean_value(row.get('notes')),
            ))
            inserted += 1
        except Exception as e:
            # Likely a duplicate UNIQUE constraint violation
            if 'unique constraint' in str(e).lower():
                skipped += 1
            else:
                print(f"  Error inserting {row['test_name']}: {e}")
                skipped += 1
    
    print(f"Loaded {inserted} lab results, skipped {skipped} duplicates")
    return {'inserted': inserted, 'skipped': skipped}

def transform_nutrition_data(**context):
    """Aggregate nutrition data by day."""
    nutrition_file = context['ti'].xcom_pull(task_ids='extract_nutrition', key='nutrition_file')
    df = pd.read_parquet(nutrition_file)
    
    # Group by date and sum all numeric columns
    daily_nutrition = df.groupby('Date').agg({
        'Calories': 'sum',
        'Fat (g)': 'sum',
        'Saturated Fat': 'sum',
        'Polyunsaturated Fat': 'sum',
        'Monounsaturated Fat': 'sum',
        'Trans Fat': 'sum',
        'Cholesterol': 'sum',
        'Carbohydrates (g)': 'sum',
        'Protein (g)': 'sum',
        'Fiber': 'sum',
        'Sugar': 'sum',
        'Sodium (mg)': 'sum',
        'Potassium': 'sum',
        'Vitamin A': 'sum',
        'Vitamin C': 'sum',
        'Calcium': 'sum',
        'Iron': 'sum',
    }).reset_index()
    
    # Add meal counts
    meal_counts = df.groupby('Date')['Meal'].count().rename('meal_count')
    daily_nutrition = daily_nutrition.merge(meal_counts, on='Date', how='left')
    
    # Rename columns to match database schema (snake_case)
    daily_nutrition = daily_nutrition.rename(columns={
        'Date': 'date',
        'Calories': 'calories',
        'Fat (g)': 'fat_g',
        'Saturated Fat': 'saturated_fat',
        'Polyunsaturated Fat': 'polyunsaturated_fat',
        'Monounsaturated Fat': 'monounsaturated_fat',
        'Trans Fat': 'trans_fat',
        'Cholesterol': 'cholesterol',
        'Carbohydrates (g)': 'carbohydrates_g',
        'Protein (g)': 'protein_g',
        'Fiber': 'fiber',
        'Sugar': 'sugar',
        'Sodium (mg)': 'sodium_mg',
        'Potassium': 'potassium',
        'Vitamin A': 'vitamin_a',
        'Vitamin C': 'vitamin_c',
        'Calcium': 'calcium',
        'Iron': 'iron',
    })
    
    print(f"Aggregated {len(daily_nutrition)} daily nutrition summaries")
    print(daily_nutrition.head())
    
    # Store for loading
    output_path = PROCESSED_DIR / f"daily_nutrition_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    daily_nutrition.to_parquet(output_path, index=False)
    context['ti'].xcom_push(key='daily_nutrition_file', value=str(output_path))
    
    return len(daily_nutrition)

def transform_activity_data(**context):
    """Aggregate activity data by day."""
    activity_file = context['ti'].xcom_pull(task_ids='extract_activities', key='activity_file')
    df = pd.read_parquet(activity_file)
    
    # Group by date and activity type
    daily_activities = df.groupby(['Date', 'Activity Type']).agg({
        'Distance': 'sum',
        'Calories': 'sum',
        'Time': 'count',  # Count of activities
        'Steps': 'sum',
    }).reset_index()
    
    # Pivot to get activity types as columns
    activity_summary = df.groupby('Date').agg({
        'Calories': 'sum',
        'Distance': 'sum',
        'Steps': 'sum',
    }).reset_index()
    
    # Add activity counts
    activity_counts = df.groupby('Date')['Activity Type'].count().rename('activity_count')
    activity_summary = activity_summary.merge(activity_counts, on='Date', how='left')
    
    # Rename columns to match database schema (snake_case)
    activity_summary = activity_summary.rename(columns={
        'Date': 'date',
        'Calories': 'calories_burned',
        'Distance': 'distance',
        'Steps': 'steps',
    })
    
    print(f"Aggregated {len(activity_summary)} daily activity summaries")
    print(activity_summary.head())
    
    # Store for loading
    output_path = PROCESSED_DIR / f"daily_activities_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    activity_summary.to_parquet(output_path, index=False)
    context['ti'].xcom_push(key='daily_activity_file', value=str(output_path))
    
    return len(activity_summary)

def transform_fitindex_data(**context):
    """Transform FitIndex body composition data for loading."""
    fitindex_file = context['ti'].xcom_pull(task_ids='extract_fitindex', key='fitindex_file')
    df = pd.read_parquet(fitindex_file)
    
    # Rename columns to match database schema (snake_case, no special chars)
    column_mapping = {
        'Date': 'date',
        'Weight(lb)': 'weight_lb',
        'BMI': 'bmi',
        'Body Fat(%)': 'body_fat_pct',
        'Fat-free Body Weight(lb)': 'fat_free_body_weight_lb',
        'Subcutaneous Fat(%)': 'subcutaneous_fat_pct',
        'Visceral Fat': 'visceral_fat',
        'Body Water(%)': 'body_water_pct',
        'Skeletal Muscle(%)': 'skeletal_muscle_pct',
        'Muscle Mass(lb)': 'muscle_mass_lb',
        'Bone Mass(lb)': 'bone_mass_lb',
        'Protein(%)': 'protein_pct',
        'BMR(kcal)': 'bmr_kcal',
        'Metabolic Age': 'metabolic_age',
    }
    
    df = df.rename(columns=column_mapping)
    
    # Select only the columns we need (drop Time of Measurement, Remarks)
    columns_to_keep = list(column_mapping.values())
    df = df[columns_to_keep]
    
    # Data validation
    # Check for reasonable weight values
    if (df['weight_lb'] < 140).any() or (df['weight_lb'] > 300).any():
        print("⚠️  WARNING: Found unusual weight values")
        print(df[['date', 'weight_lb']][(df['weight_lb'] < 140) | (df['weight_lb'] > 300)])
    
    # Check for reasonable body fat percentages
    if (df['body_fat_pct'] < 3).any() or (df['body_fat_pct'] > 25).any():
        print("⚠️  WARNING: Found unusual body fat percentages")
        print(df[['date', 'body_fat_pct']][(df['body_fat_pct'] < 3) | (df['body_fat_pct'] > 25)])
    
    print(f"Transformed {len(df)} body composition records")
    print(df.head())
    
    # Store for loading
    output_path = PROCESSED_DIR / f"daily_body_composition_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(output_path, index=False)
    context['ti'].xcom_push(key='daily_body_composition_file', value=str(output_path))
    
    return len(df)

def load_nutrition(**context):
    """Load nutrition data into PostgreSQL."""
    nutrition_file = context['ti'].xcom_pull(task_ids='transform_nutrition', key='daily_nutrition_file')
    df = pd.read_parquet(nutrition_file)
    
    print(f"Loading {len(df)} nutrition records")
    
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='health_db')
    
    # Convert date to string for PostgreSQL
    df['date_str'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Track inserts vs updates
    inserted = 0
    updated = 0
    
    # Helper function to convert NaN to None for SQL NULL compatibility
    def clean_value(val):
        if pd.isna(val):
            return None
        return val
    
    # Insert row by row with UPSERT logic
    for _, row in df.iterrows():
        result = hook.run("""
            INSERT INTO nutrition_daily (
                date, calories, fat_g, saturated_fat, polyunsaturated_fat,
                monounsaturated_fat, trans_fat, cholesterol, carbohydrates_g, protein_g,
                fiber, sugar, sodium_mg, potassium, vitamin_a, vitamin_c, calcium, iron, meal_count
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (date) DO UPDATE SET
                calories = EXCLUDED.calories,
                fat_g = EXCLUDED.fat_g,
                saturated_fat = EXCLUDED.saturated_fat,
                polyunsaturated_fat = EXCLUDED.polyunsaturated_fat,
                monounsaturated_fat = EXCLUDED.monounsaturated_fat,
                trans_fat = EXCLUDED.trans_fat,
                cholesterol = EXCLUDED.cholesterol,
                carbohydrates_g = EXCLUDED.carbohydrates_g,
                protein_g = EXCLUDED.protein_g,
                fiber = EXCLUDED.fiber,
                sugar = EXCLUDED.sugar,
                sodium_mg = EXCLUDED.sodium_mg,
                potassium = EXCLUDED.potassium,
                vitamin_a = EXCLUDED.vitamin_a,
                vitamin_c = EXCLUDED.vitamin_c,
                calcium = EXCLUDED.calcium,
                iron = EXCLUDED.iron,
                meal_count = EXCLUDED.meal_count,
                created_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted;
        """, parameters=(
            row['date_str'],
            clean_value(row.get('calories')),
            clean_value(row.get('fat_g')),
            clean_value(row.get('saturated_fat')),
            clean_value(row.get('polyunsaturated_fat')),
            clean_value(row.get('monounsaturated_fat')),
            clean_value(row.get('trans_fat')),
            clean_value(row.get('cholesterol')),
            clean_value(row.get('carbohydrates_g')),
            clean_value(row.get('protein_g')),
            clean_value(row.get('fiber')),
            clean_value(row.get('sugar')),
            clean_value(row.get('sodium_mg')),
            clean_value(row.get('potassium')),
            clean_value(row.get('vitamin_a')),
            clean_value(row.get('vitamin_c')),
            clean_value(row.get('calcium')),
            clean_value(row.get('iron')),
            clean_value(row.get('meal_count')),
        ), handler=lambda cur: cur.fetchone())
        
        if result and result[0]:
            inserted += 1
        else:
            updated += 1
    
    print(f"Loaded {inserted} new nutrition records, updated {updated} existing records")
    return {'inserted': inserted, 'updated': updated}

def load_activities(**context):
    """Load activity data into PostgreSQL."""
    activity_file = context['ti'].xcom_pull(task_ids='transform_activities', key='daily_activity_file')
    df = pd.read_parquet(activity_file)
    
    print(f"Loading {len(df)} activity records")
    
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='health_db')
    
    # Convert date to string for PostgreSQL
    df['date_str'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Track inserts vs updates
    inserted = 0
    updated = 0
    
    # Helper function to convert NaN to None for SQL NULL compatibility
    def clean_value(val):
        if pd.isna(val):
            return None
        return val
    
    # Insert row by row with UPSERT logic
    for _, row in df.iterrows():
        result = hook.run("""
            INSERT INTO activity_daily (
                date, calories_burned, distance, steps, activity_count
            ) VALUES (
                %s, %s, %s, %s, %s
            )
            ON CONFLICT (date) DO UPDATE SET
                calories_burned = EXCLUDED.calories_burned,
                distance = EXCLUDED.distance,
                steps = EXCLUDED.steps,
                activity_count = EXCLUDED.activity_count,
                created_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted;
        """, parameters=(
            row['date_str'],
            clean_value(row.get('calories_burned')),
            clean_value(row.get('distance')),
            clean_value(row.get('steps')),
            clean_value(row.get('activity_count')),
        ), handler=lambda cur: cur.fetchone())
        
        if result and result[0]:
            inserted += 1
        else:
            updated += 1
    
    print(f"Loaded {inserted} new activity records, updated {updated} existing records")
    return {'inserted': inserted, 'updated': updated}

def load_body_composition(**context):
    """Load body composition data into PostgreSQL."""
    body_comp_file = context['ti'].xcom_pull(task_ids='transform_fitindex', key='daily_body_composition_file')
    df = pd.read_parquet(body_comp_file)
    
    print(f"Loading {len(df)} body composition records")
    
    # Connect to PostgreSQL
    hook = PostgresHook(postgres_conn_id='health_db')
    
    # Convert date to string for PostgreSQL
    df['date_str'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Track inserts vs updates
    inserted = 0
    updated = 0
    
    # Helper function to convert NaN to None for SQL NULL compatibility
    def clean_value(val):
        if pd.isna(val):
            return None
        return val
    
    # Insert row by row with UPSERT logic
    for _, row in df.iterrows():
        result = hook.run("""
            INSERT INTO body_composition (
                date, weight_lb, bmi, body_fat_pct, fat_free_body_weight_lb,
                subcutaneous_fat_pct, visceral_fat, body_water_pct, 
                skeletal_muscle_pct, muscle_mass_lb, bone_mass_lb, 
                protein_pct, bmr_kcal, metabolic_age
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (date) DO UPDATE SET
                weight_lb = EXCLUDED.weight_lb,
                bmi = EXCLUDED.bmi,
                body_fat_pct = EXCLUDED.body_fat_pct,
                fat_free_body_weight_lb = EXCLUDED.fat_free_body_weight_lb,
                subcutaneous_fat_pct = EXCLUDED.subcutaneous_fat_pct,
                visceral_fat = EXCLUDED.visceral_fat,
                body_water_pct = EXCLUDED.body_water_pct,
                skeletal_muscle_pct = EXCLUDED.skeletal_muscle_pct,
                muscle_mass_lb = EXCLUDED.muscle_mass_lb,
                bone_mass_lb = EXCLUDED.bone_mass_lb,
                protein_pct = EXCLUDED.protein_pct,
                bmr_kcal = EXCLUDED.bmr_kcal,
                metabolic_age = EXCLUDED.metabolic_age,
                created_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted;
        """, parameters=(
            row['date_str'],
            clean_value(row.get('weight_lb')),
            clean_value(row.get('bmi')),
            clean_value(row.get('body_fat_pct')),
            clean_value(row.get('fat_free_body_weight_lb')),
            clean_value(row.get('subcutaneous_fat_pct')),
            clean_value(row.get('visceral_fat')),
            clean_value(row.get('body_water_pct')),
            clean_value(row.get('skeletal_muscle_pct')),
            clean_value(row.get('muscle_mass_lb')),
            clean_value(row.get('bone_mass_lb')),
            clean_value(row.get('protein_pct')),
            clean_value(row.get('bmr_kcal')),
            clean_value(row.get('metabolic_age')),
        ), handler=lambda cur: cur.fetchone())
        
        if result and result[0]:
            inserted += 1
        else:
            updated += 1
    
    print(f"Loaded {inserted} new body composition records, updated {updated} existing records")
    return {'inserted': inserted, 'updated': updated}

def check_data_quality(**context):
    """Run data quality checks on the processed data."""
    hook = PostgresHook(postgres_conn_id='health_db')
    
    # Check nutrition data
    nutrition_result = hook.get_first("""
        SELECT 
            COUNT(*) as total_days,
            MAX(date) as latest_date,
            MIN(date) as earliest_date
        FROM nutrition_daily
    """)
    
    if nutrition_result:
        total_days, latest_date, earliest_date = nutrition_result
        print(f"Nutrition data quality check:")
        print(f"  Total days: {total_days}")
        print(f"  Date range: {earliest_date} to {latest_date}")
        
        # Check for anomalies
        anomalies = hook.get_first("""
            SELECT COUNT(*) 
            FROM nutrition_daily 
            WHERE calories < 500 OR calories > 5000
        """)
        
        if anomalies[0] > 0:
            print(f"  ⚠️  WARNING: {anomalies[0]} days with unusual calorie intake")
    
    # Check activity data
    activity_result = hook.get_first("""
        SELECT 
            COUNT(*) as total_days,
            MAX(date) as latest_date,
            MIN(date) as earliest_date
        FROM activity_daily
    """)
    
    if activity_result:
        total_days, latest_date, earliest_date = activity_result
        print(f"Activity data quality check:")
        print(f"  Total days: {total_days}")
        print(f"  Date range: {earliest_date} to {latest_date}")
    
    # Check body composition data
    body_result = hook.get_first("""
        SELECT 
            COUNT(*) as total_days,
            MAX(date) as latest_date,
            MIN(date) as earliest_date
        FROM body_composition
    """)
    
    if body_result:
        total_days, latest_date, earliest_date = body_result
        print(f"Body composition data quality check:")
        print(f"  Total days: {total_days}")
        print(f"  Date range: {earliest_date} to {latest_date}")
    
    return True

# Create the DAG
with DAG(
    'health_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for MyFitnessPal, Garmin, and FitIndex health data',
    schedule_interval=None,
    catchup=False,
    tags=['health', 'etl', 'personal'],
) as dag:
    
    create_tables = PostgresOperator(
    task_id='create_tables',
    postgres_conn_id='health_db',
    sql="""
        -- Drop old merged table (clean migration)
        DROP TABLE IF EXISTS daily_health_summary;
        
        -- Nutrition data table
        CREATE TABLE IF NOT EXISTS nutrition_daily (
            date DATE PRIMARY KEY,
            calories INTEGER,
            fat_g FLOAT,
            saturated_fat FLOAT,
            polyunsaturated_fat FLOAT,
            monounsaturated_fat FLOAT,
            trans_fat FLOAT,
            cholesterol FLOAT,
            carbohydrates_g FLOAT,
            protein_g FLOAT,
            fiber FLOAT,
            sugar FLOAT,
            sodium_mg FLOAT,
            potassium FLOAT,
            vitamin_a FLOAT,
            vitamin_c FLOAT,
            calcium FLOAT,
            iron FLOAT,
            meal_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_nutrition_daily_date 
        ON nutrition_daily(date DESC);
        
        -- Activity data table
        CREATE TABLE IF NOT EXISTS activity_daily (
            date DATE PRIMARY KEY,
            calories_burned INTEGER,
            distance FLOAT,
            steps INTEGER,
            activity_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_activity_daily_date 
        ON activity_daily(date DESC);
        
        -- Body composition data table
        CREATE TABLE IF NOT EXISTS body_composition (
            date DATE PRIMARY KEY,
            weight_lb FLOAT,
            bmi FLOAT,
            body_fat_pct FLOAT,
            fat_free_body_weight_lb FLOAT,
            subcutaneous_fat_pct FLOAT,
            visceral_fat FLOAT,
            body_water_pct FLOAT,
            skeletal_muscle_pct FLOAT,
            muscle_mass_lb FLOAT,
            bone_mass_lb FLOAT,
            protein_pct FLOAT,
            bmr_kcal INTEGER,
            metabolic_age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_body_composition_date 
        ON body_composition(date DESC);

        -- Lab results data table
        CREATE TABLE IF NOT EXISTS lab_results (
            id SERIAL PRIMARY KEY,
            test_date DATE NOT NULL,
            panel_name VARCHAR(255) NOT NULL,
            test_name VARCHAR(255) NOT NULL,
            result_value FLOAT,
            result_text VARCHAR(255),
            result_flag VARCHAR(10),
            result_unit VARCHAR(50),
            reference_range_text VARCHAR(500),
            reference_range_low FLOAT,
            reference_range_high FLOAT,
            is_abnormal BOOLEAN,
            notes TEXT,
            lab_name VARCHAR(255) DEFAULT 'Quest Diagnostics',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(test_date, test_name)
        );
        
        CREATE INDEX IF NOT EXISTS idx_lab_results_date 
        ON lab_results(test_date DESC);
        
        CREATE INDEX IF NOT EXISTS idx_lab_results_panel 
        ON lab_results(panel_name);
        
        CREATE INDEX IF NOT EXISTS idx_lab_results_test_name 
        ON lab_results(test_name);
        
        CREATE INDEX IF NOT EXISTS idx_lab_results_abnormal 
        ON lab_results(is_abnormal) WHERE is_abnormal = TRUE;
    """
)

    migrate_nutrition_schema = PostgresOperator(
        task_id='migrate_nutrition_schema',
        postgres_conn_id='health_db',
        sql="""
            ALTER TABLE nutrition_daily
                ADD COLUMN IF NOT EXISTS trans_fat FLOAT,
                ADD COLUMN IF NOT EXISTS potassium FLOAT,
                ADD COLUMN IF NOT EXISTS vitamin_a FLOAT,
                ADD COLUMN IF NOT EXISTS vitamin_c FLOAT,
                ADD COLUMN IF NOT EXISTS calcium FLOAT,
                ADD COLUMN IF NOT EXISTS iron FLOAT;
        """
    )
    # Extract tasks
    extract_nutrition = PythonOperator(
        task_id='extract_nutrition',
        python_callable=extract_nutrition_data,
    )
    
    extract_activities = PythonOperator(
        task_id='extract_activities',
        python_callable=extract_activity_data,
    )
    
    extract_fitindex = PythonOperator(
        task_id='extract_fitindex',
        python_callable=extract_fitindex_data,
    )
    
    # Transform tasks
    transform_nutrition = PythonOperator(
        task_id='transform_nutrition',
        python_callable=transform_nutrition_data,
    )
    
    transform_activities = PythonOperator(
        task_id='transform_activities',
        python_callable=transform_activity_data,
    )
    
    transform_fitindex = PythonOperator(
        task_id='transform_fitindex',
        python_callable=transform_fitindex_data,
    )
    
    # Load tasks
    load_nutrition_data = PythonOperator(
        task_id='load_nutrition',
        python_callable=load_nutrition,
    )
    
    load_activity_data = PythonOperator(
        task_id='load_activities',
        python_callable=load_activities,
    )
    
    load_body_comp = PythonOperator(
        task_id='load_body_composition',
        python_callable=load_body_composition,
    )
    
    # Quality check
    quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=check_data_quality,
    )
    
    # Lab data extract/transform/load tasks
    extract_lab = PythonOperator(
        task_id='extract_lab',
        python_callable=extract_lab_results,
    )
    
    transform_lab = PythonOperator(
        task_id='transform_lab',
        python_callable=transform_lab_results,
    )
    
    load_lab = PythonOperator(
        task_id='load_lab',
        python_callable=load_lab_results,
    )
    
    # Define task dependencies
    create_tables >> migrate_nutrition_schema >> [extract_nutrition, extract_activities, extract_fitindex, extract_lab]
    
    extract_nutrition >> transform_nutrition >> load_nutrition_data
    extract_activities >> transform_activities >> load_activity_data
    extract_fitindex >> transform_fitindex >> load_body_comp
    extract_lab >> transform_lab >> load_lab
    
    [load_nutrition_data, load_activity_data, load_body_comp, load_lab] >> quality_check