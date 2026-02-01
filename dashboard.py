import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
# Create figure with plotly graph objects for better control
import plotly.graph_objects as go

# Database connection function
@st.cache_data
def load_data():
    """Load health data from PostgreSQL."""
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    query = """
        SELECT 
            date,
            calories_intake,
            calories_burned,
            net_calories,
            "Protein (g)" as protein,
            "Fat (g)" as fat,
            "Saturated Fat" as saturated_fat,
            "Polyunsaturated Fat" as polyunsaturated_fat,
            "Monounsaturated Fat" as monounsaturated_fat,
            "Cholesterol" as cholesterol,
            "Carbohydrates (g)" as carbs,
            meal_count,
            activity_count,
            steps
        FROM daily_health_summary
        ORDER BY date ASC
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Convert date column to datetime
    df['date'] = pd.to_datetime(df['date'])
    
    return df

st.title("🏃 Health Dashboard")

# Load data
try:
    df = load_data()
    st.success(f"✅ Loaded {len(df)} days of data!")
    
    # Calculate average metrics
    avg_intake = df['calories_intake'].mean()
    avg_burned = df['calories_burned'].mean()
    avg_net = df['net_calories'].mean()
    
    # Display metrics in 3 columns
    st.subheader("📊 Summary Metrics")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Avg Calories Intake",
            value=f"{avg_intake:.0f}"
        )
    
    with col2:
        st.metric(
            label="Avg Calories Burned", 
            value=f"{avg_burned:.0f}"
        )
    
    with col3:
        st.metric(
            label="Avg Net Calories",
            value=f"{avg_net:.0f}"
        )

    df_melted = df.melt(id_vars=['date'], 
                        value_vars=['calories_intake', 'calories_burned', 'net_calories'],
                        var_name='calorie_type', 
                        value_name='calories')

    # Calories Over Time Chart
    st.subheader("📈 Calories Over Time")

    fig = go.Figure()
    
    # Add each trace separately with distinct styling
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['calories_intake'],
        name='Calories Intake',
        mode='markers',  # Only markers (no connecting lines since data is sparse)
        marker=dict(
            size=12,
            color='rgb(31, 119, 180)',  # Bright blue
            symbol='circle',
            line=dict(width=2, color='white')  # White border makes them pop
        ),
    ))
    
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['calories_burned'],
        name='Calories Burned',
        mode='lines+markers',
        marker=dict(size=6, color='rgb(255, 127, 14)'),  # Orange
        line=dict(width=2),
        connectgaps=False
    ))
    
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['net_calories'],
        name='Net Calories',
        mode='lines+markers',
        marker=dict(size=6, color='rgb(214, 39, 40)'),  # Red
        line=dict(width=2),
        connectgaps=False
    ))
    
    # Update layout
    fig.update_layout(
        title='Calories Intake, Burned, and Net Over Time',
        xaxis_title='Date',
        yaxis_title='Calories',
        hovermode='x unified',
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)
    # Show first few rows to verify
    st.subheader("Preview of Your Data")
    preview_df = df.head(10).copy()
    preview_df['date'] = preview_df['date'].dt.strftime('%Y-%m-%d')

    st.dataframe(preview_df, use_container_width=True, hide_index=True)
    
except Exception as e:
    st.error(f"❌ Error loading data: {e}")