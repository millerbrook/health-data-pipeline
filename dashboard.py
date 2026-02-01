import streamlit as st
from datetime import timedelta
import pandas as pd
import psycopg2
import plotly.express as px
# Create figure with plotly graph objects for better control
import plotly.graph_objects as go

# Optional scipy for trend lines
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

# Database connection function
@st.cache_data
def load_data():

    """Load health data from PostgreSQL by joining all 3 tables."""
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    
    query = """
        SELECT 
            COALESCE(n.date, a.date, b.date) as date,
            
            -- Nutrition data
            n.calories as calories_intake,
            n.protein_g as protein,
            n.fat_g as fat,
            n.carbohydrates_g as carbs,
            n.meal_count,
            
            -- Activity data
            a.calories_burned,
            a.distance,
            a.steps,
            a.activity_count,
            
            -- Body composition data
            b.weight_lb,
            b.bmi,
            b.body_fat_pct,
            b.muscle_mass_lb,
            b.bmr_kcal,
            
            -- Calculate net calories
            (COALESCE(n.calories, 0) - COALESCE(a.calories_burned, 0)) as net_calories
            
        FROM nutrition_daily n
        FULL OUTER JOIN activity_daily a ON n.date = a.date
        FULL OUTER JOIN body_composition b ON COALESCE(n.date, a.date) = b.date
        ORDER BY COALESCE(n.date, a.date, b.date) ASC
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
    
    # Date filtering section
    st.subheader("📅 Filter Data")
    
    # Get min/max dates from data
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    
    # Quick preset buttons
    preset = st.radio(
        "Quick Select:",
        ["Last 7 Days", "Last 30 Days", "Last 90 Days", "All Time", "Custom"],
        horizontal=True
    )
    
    # Calculate date range based on preset
    if preset == "Last 7 Days":
        start_date = max_date - timedelta(days=7)
        end_date = max_date
    elif preset == "Last 30 Days":
        start_date = max_date - timedelta(days=30)
        end_date = max_date
    elif preset == "Last 90 Days":
        start_date = max_date - timedelta(days=90)
        end_date = max_date
    elif preset == "All Time":
        start_date = min_date
        end_date = max_date
    else:  # Custom
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", min_date)
        with col2:
            end_date = st.date_input("End Date", max_date)
    
    # Filter the dataframe
    filtered_df = df[(df['date'].dt.date >= start_date) & 
                     (df['date'].dt.date <= end_date)]
    
    # Show info about filtered data
    st.info(f"📊 Showing data from **{start_date}** to **{end_date}** ({len(filtered_df)} days)")
    
    # Display metrics organized by category
    st.subheader("📊 Summary Metrics")
    
    # Nutrition Metrics
    st.markdown("**🍽️ Nutrition**")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_intake = filtered_df['calories_intake'].mean()
        st.metric(
            label="Avg Calories",
            value=f"{avg_intake:.0f}" if pd.notna(avg_intake) else "N/A"
        )
    
    with col2:
        avg_protein = filtered_df['protein'].mean()
        st.metric(
            label="Avg Protein (g)",
            value=f"{avg_protein:.1f}" if pd.notna(avg_protein) else "N/A"
        )
    
    with col3:
        avg_fat = filtered_df['fat'].mean()
        st.metric(
            label="Avg Fat (g)",
            value=f"{avg_fat:.1f}" if pd.notna(avg_fat) else "N/A"
        )
    
    with col4:
        avg_carbs = filtered_df['carbs'].mean()
        st.metric(
            label="Avg Carbs (g)",
            value=f"{avg_carbs:.1f}" if pd.notna(avg_carbs) else "N/A"
        )
    
    # Activity Metrics
    st.markdown("**🏃 Activity**")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_burned = filtered_df['calories_burned'].mean()
        st.metric(
            label="Avg Calories Burned",
            value=f"{avg_burned:.0f}" if pd.notna(avg_burned) else "N/A"
        )
    
    with col2:
        avg_net = filtered_df['net_calories'].mean()
        st.metric(
            label="Avg Net Calories",
            value=f"{avg_net:.0f}" if pd.notna(avg_net) else "N/A"
        )
    
    with col3:
        avg_distance = filtered_df['distance'].mean()
        st.metric(
            label="Avg Distance (mi)",
            value=f"{avg_distance:.1f}" if pd.notna(avg_distance) else "N/A"
        )
    
    with col4:
        avg_steps = filtered_df['steps'].mean()
        st.metric(
            label="Avg Steps",
            value=f"{avg_steps:.0f}" if pd.notna(avg_steps) else "N/A"
        )
    
    # Body Composition Metrics
    st.markdown("**⚖️ Body Composition**")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_weight = filtered_df['weight_lb'].mean()
        st.metric(
            label="Avg Weight (lb)",
            value=f"{avg_weight:.1f}" if pd.notna(avg_weight) else "N/A"
        )
    
    with col2:
        avg_bmi = filtered_df['bmi'].mean()
        st.metric(
            label="Avg BMI",
            value=f"{avg_bmi:.1f}" if pd.notna(avg_bmi) else "N/A"
        )
    
    with col3:
        avg_body_fat = filtered_df['body_fat_pct'].mean()
        st.metric(
            label="Avg Body Fat (%)",
            value=f"{avg_body_fat:.1f}" if pd.notna(avg_body_fat) else "N/A"
        )
    
    with col4:
        avg_muscle = filtered_df['muscle_mass_lb'].mean()
        st.metric(
            label="Avg Muscle (lb)",
            value=f"{avg_muscle:.1f}" if pd.notna(avg_muscle) else "N/A"
        )

    df_melted = filtered_df.melt(id_vars=['date'], 
                        value_vars=['calories_intake', 'calories_burned', 'net_calories'],
                        var_name='calorie_type', 
                        value_name='calories')

    # Calories Over Time Chart
    st.subheader("📈 Calories Over Time")

    fig = go.Figure()
    
    # Add each trace separately with distinct styling
    fig.add_trace(go.Scatter(
        x=filtered_df['date'],
        y=filtered_df['calories_intake'],
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
        x=filtered_df['date'],
        y=filtered_df['calories_burned'],
        name='Calories Burned',
        mode='lines+markers',
        marker=dict(size=6, color='rgb(255, 127, 14)'),  # Orange
        line=dict(width=2),
        connectgaps=False
    ))
    
    fig.add_trace(go.Scatter(
        x=filtered_df['date'],
        y=filtered_df['net_calories'],
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

    st.plotly_chart(fig, width='stretch')
    
    # ========== Body Composition Trends ==========
    st.subheader("⚖️ Body Composition Trends")
    
    # Create two columns for body composition charts
    col1, col2 = st.columns(2)
    
    with col1:
        # Weight Over Time
        fig_weight = go.Figure()
        fig_weight.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=filtered_df['weight_lb'],
            name='Weight',
            mode='lines+markers',
            marker=dict(size=8, color='rgb(44, 160, 44)'),  # Green
            line=dict(width=3),
            connectgaps=False
        ))
        
        fig_weight.update_layout(
            title='Weight Over Time',
            xaxis_title='Date',
            yaxis_title='Weight (lb)',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_weight, width='stretch')
    
    with col2:
        # Body Fat % Over Time
        fig_bodyfat = go.Figure()
        fig_bodyfat.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=filtered_df['body_fat_pct'],
            name='Body Fat %',
            mode='lines+markers',
            marker=dict(size=8, color='rgb(214, 39, 40)'),  # Red
            line=dict(width=3),
            connectgaps=False,
            fill='tozeroy',
            fillcolor='rgba(214, 39, 40, 0.1)'
        ))
        
        fig_bodyfat.update_layout(
            title='Body Fat Percentage Over Time',
            xaxis_title='Date',
            yaxis_title='Body Fat (%)',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_bodyfat, width='stretch')
    
    # BMI with Healthy Range Bands
    fig_bmi = go.Figure()
    
    # Add BMI line
    fig_bmi.add_trace(go.Scatter(
        x=filtered_df['date'],
        y=filtered_df['bmi'],
        name='BMI',
        mode='lines+markers',
        marker=dict(size=8, color='rgb(148, 103, 189)'),  # Purple
        line=dict(width=3),
        connectgaps=False
    ))
    
    # Add reference bands for BMI categories
    date_range = [filtered_df['date'].min(), filtered_df['date'].max()]
    
    # Underweight: < 18.5 (light blue)
    fig_bmi.add_trace(go.Scatter(
        x=date_range,
        y=[18.5, 18.5],
        name='Underweight Threshold',
        mode='lines',
        line=dict(color='lightblue', width=1, dash='dash'),
        showlegend=False
    ))
    
    # Normal: 18.5-24.9 (green band)
    fig_bmi.add_hrect(
        y0=18.5, y1=24.9,
        fillcolor="green", opacity=0.1,
        layer="below", line_width=0,
        annotation_text="Healthy Range", annotation_position="left"
    )
    
    # Overweight: 25-29.9 (yellow band)
    fig_bmi.add_hrect(
        y0=25, y1=29.9,
        fillcolor="orange", opacity=0.1,
        layer="below", line_width=0,
        annotation_text="Overweight", annotation_position="left"
    )
    
    fig_bmi.update_layout(
        title='BMI Over Time with Healthy Range',
        xaxis_title='Date',
        yaxis_title='BMI',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig_bmi, width='stretch')
    
    # Muscle Mass vs Body Fat (stacked area)
    fig_composition = go.Figure()
    
    fig_composition.add_trace(go.Scatter(
        x=filtered_df['date'],
        y=filtered_df['muscle_mass_lb'],
        name='Muscle Mass',
        mode='lines',
        line=dict(width=0.5, color='rgb(44, 160, 44)'),  # Green
        stackgroup='one',
        fillcolor='rgba(44, 160, 44, 0.6)'
    ))
    
    # Calculate fat mass from weight and body fat %
    filtered_df['fat_mass_lb'] = filtered_df['weight_lb'] * (filtered_df['body_fat_pct'] / 100)
    
    fig_composition.add_trace(go.Scatter(
        x=filtered_df['date'],
        y=filtered_df['fat_mass_lb'],
        name='Fat Mass',
        mode='lines',
        line=dict(width=0.5, color='rgb(214, 39, 40)'),  # Red
        stackgroup='one',
        fillcolor='rgba(214, 39, 40, 0.6)'
    ))
    
    fig_composition.update_layout(
        title='Body Composition: Muscle Mass vs Fat Mass',
        xaxis_title='Date',
        yaxis_title='Mass (lb)',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig_composition, width='stretch')
    
    # ========== Metabolic Insights ==========
    st.subheader("🔥 Metabolic Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # BMR Trends
        fig_bmr = go.Figure()
        fig_bmr.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=filtered_df['bmr_kcal'],
            name='BMR',
            mode='lines+markers',
            marker=dict(size=8, color='rgb(255, 127, 14)'),  # Orange
            line=dict(width=3),
            connectgaps=False
        ))
        
        fig_bmr.update_layout(
            title='Basal Metabolic Rate Over Time',
            xaxis_title='Date',
            yaxis_title='BMR (kcal)',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_bmr, width='stretch')
    
    with col2:
        # BMR vs Calorie Intake
        fig_bmr_intake = go.Figure()
        
        fig_bmr_intake.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=filtered_df['bmr_kcal'],
            name='BMR',
            mode='lines',
            line=dict(color='rgb(255, 127, 14)', width=2),  # Orange
            connectgaps=False
        ))
        
        fig_bmr_intake.add_trace(go.Scatter(
            x=filtered_df['date'],
            y=filtered_df['calories_intake'],
            name='Calorie Intake',
            mode='markers',
            marker=dict(size=8, color='rgb(31, 119, 180)'),  # Blue
        ))
        
        fig_bmr_intake.update_layout(
            title='BMR vs Calorie Intake',
            xaxis_title='Date',
            yaxis_title='Calories (kcal)',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_bmr_intake, width='stretch')
    
    # ========== Activity Trends ==========
    st.subheader("🏃 Activity Trends")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Steps Over Time
        fig_steps = go.Figure()
        fig_steps.add_trace(go.Bar(
            x=filtered_df['date'],
            y=filtered_df['steps'],
            name='Steps',
            marker=dict(color='rgb(31, 119, 180)'),  # Blue
        ))
        
        # Add average line
        avg_steps_val = filtered_df['steps'].mean()
        if pd.notna(avg_steps_val):
            fig_steps.add_hline(
                y=avg_steps_val,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Avg: {avg_steps_val:.0f}",
                annotation_position="right"
            )
        
        fig_steps.update_layout(
            title='Daily Steps',
            xaxis_title='Date',
            yaxis_title='Steps',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_steps, width='stretch')
    
    with col2:
        # Distance Over Time
        fig_distance = go.Figure()
        fig_distance.add_trace(go.Bar(
            x=filtered_df['date'],
            y=filtered_df['distance'],
            name='Distance',
            marker=dict(color='rgb(44, 160, 44)'),  # Green
        ))
        
        # Add average line
        avg_distance_val = filtered_df['distance'].mean()
        if pd.notna(avg_distance_val):
            fig_distance.add_hline(
                y=avg_distance_val,
                line_dash="dash",
                line_color="red",
                annotation_text=f"Avg: {avg_distance_val:.1f} mi",
                annotation_position="right"
            )
        
        fig_distance.update_layout(
            title='Daily Distance',
            xaxis_title='Date',
            yaxis_title='Distance (miles)',
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_distance, width='stretch')
    
    # ========== Correlations ==========
    st.subheader("🔗 Correlations & Insights")
    
    # Net Calories vs Weight Change Scatter
    # Calculate weight change (day-to-day)
    weight_data = filtered_df[['date', 'weight_lb', 'net_calories']].dropna()
    
    if len(weight_data) > 1:
        weight_data = weight_data.sort_values('date')
        weight_data['weight_change'] = weight_data['weight_lb'].diff()
        
        # Remove first row (no previous weight)
        weight_data = weight_data.dropna(subset=['weight_change'])
        
        fig_correlation = go.Figure()
        
        fig_correlation.add_trace(go.Scatter(
            x=weight_data['net_calories'],
            y=weight_data['weight_change'],
            mode='markers',
            marker=dict(
                size=10,
                color=weight_data['weight_change'],
                colorscale='RdYlGn_r',  # Red for gain, green for loss
                showscale=True,
                colorbar=dict(title="Weight<br>Change (lb)")
            ),
            text=weight_data['date'].dt.strftime('%Y-%m-%d'),
            hovertemplate='<b>%{text}</b><br>Net Calories: %{x:.0f}<br>Weight Change: %{y:.2f} lb<extra></extra>'
        ))
        
        # Add trend line if enough data points
        if len(weight_data) >= 3 and SCIPY_AVAILABLE:
            slope, intercept, r_value, p_value, std_err = stats.linregress(
                weight_data['net_calories'].dropna(),
                weight_data['weight_change'].dropna()
            )
            
            line_x = [weight_data['net_calories'].min(), weight_data['net_calories'].max()]
            line_y = [slope * x + intercept for x in line_x]
            
            fig_correlation.add_trace(go.Scatter(
                x=line_x,
                y=line_y,
                mode='lines',
                name=f'Trend (R²={r_value**2:.3f})',
                line=dict(color='red', width=2, dash='dash')
            ))
        elif len(weight_data) >= 3 and not SCIPY_AVAILABLE:
            st.info("💡 Install scipy for trend line: `pip install scipy`")
        
        fig_correlation.update_layout(
            title='Net Calories vs Weight Change',
            xaxis_title='Net Calories (kcal)',
            yaxis_title='Weight Change (lb)',
            hovermode='closest',
            height=500
        )
        
        st.plotly_chart(fig_correlation, width='stretch')
        
        # Insight text
        correlation = weight_data['net_calories'].corr(weight_data['weight_change'])
        if pd.notna(correlation):
            st.info(f"📊 Correlation coefficient: **{correlation:.3f}** - "
                   f"{'Strong' if abs(correlation) > 0.7 else 'Moderate' if abs(correlation) > 0.4 else 'Weak'} "
                   f"{'positive' if correlation > 0 else 'negative'} correlation")
    else:
        st.warning("⚠️ Not enough weight data to show correlation analysis. Need at least 2 measurements.")
    
    # Show first few rows to verify
    st.subheader("Preview of Your Data")
    preview_df = filtered_df.head(10).copy()
    preview_df['date'] = preview_df['date'].dt.strftime('%Y-%m-%d')

    st.dataframe(preview_df, width='stretch', hide_index=True)
    
except Exception as e:
    st.error(f"❌ Error loading data: {e}")