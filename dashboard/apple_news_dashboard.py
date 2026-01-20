"""
Streamlit Dashboard for News Sentiment Analysis
Displays analyzed news data from Google BigQuery
"""

import os
import sys
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables
load_dotenv()

# Google Cloud Configuration
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BIGQUERY_DATASET = os.getenv('BIGQUERY_DATASET', 'news_sentiment')
BIGQUERY_TABLE = os.getenv('BIGQUERY_TABLE', 'news_articles')
GCP_CREDENTIALS_PATH = os.path.join('credentials', 'gcp.json')

# Dashboard Configuration
DASHBOARD_REFRESH_INTERVAL = int(os.getenv('DASHBOARD_REFRESH_INTERVAL', '300'))

# Page configuration
st.set_page_config(
    page_title="News Sentiment Analysis Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_resource
def get_bigquery_client():
    """Initialize and return BigQuery client"""
    try:
        credentials = service_account.Credentials.from_service_account_file(
            GCP_CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        
        client = bigquery.Client(
            credentials=credentials,
            project=GCP_PROJECT_ID
        )
        
        return client
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {e}")
        return None


@st.cache_data(ttl=DASHBOARD_REFRESH_INTERVAL)
def load_data(_client):
    """Load data from BigQuery with caching"""
    if not _client:
        return pd.DataFrame()
    
    query = f"""
    SELECT 
        article_id,
        title,
        description,
        content,
        url,
        source_name,
        source_url,
        published_at,
        country,
        category,
        language,
        image_url,
        keywords,
        creator,
        sentiment_polarity,
        sentiment_subjectivity,
        sentiment_label,
        processed_at,
        inserted_at
    FROM `{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}`
    ORDER BY inserted_at DESC
    LIMIT 1000
    """
    
    try:
        df = _client.query(query).to_dataframe()
        
        # Convert timestamp columns
        if not df.empty:
            df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce')
            df['processed_at'] = pd.to_datetime(df['processed_at'], errors='coerce')
            df['inserted_at'] = pd.to_datetime(df['inserted_at'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()


def create_sentiment_distribution_chart(df):
    """Create sentiment distribution pie chart"""
    sentiment_counts = df['sentiment_label'].value_counts()
    
    colors = {
        'positive': '#28a745',
        'neutral': '#6c757d',
        'negative': '#dc3545'
    }
    
    fig = px.pie(
        values=sentiment_counts.values,
        names=sentiment_counts.index,
        title="Sentiment Distribution",
        color=sentiment_counts.index,
        color_discrete_map=colors
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig


def create_sentiment_timeline(df):
    """Create sentiment over time line chart"""
    df_sorted = df.sort_values('published_at')
    
    # Group by date and sentiment
    df_sorted['date'] = df_sorted['published_at'].dt.date
    sentiment_over_time = df_sorted.groupby(['date', 'sentiment_label']).size().reset_index(name='count')
    
    colors = {
        'positive': '#28a745',
        'neutral': '#6c757d',
        'negative': '#dc3545'
    }
    
    fig = px.line(
        sentiment_over_time,
        x='date',
        y='count',
        color='sentiment_label',
        title="Sentiment Trends Over Time",
        labels={'date': 'Date', 'count': 'Number of Articles', 'sentiment_label': 'Sentiment'},
        color_discrete_map=colors
    )
    
    fig.update_layout(hovermode='x unified')
    
    return fig


def create_polarity_distribution(df):
    """Create polarity distribution histogram"""
    fig = px.histogram(
        df,
        x='sentiment_polarity',
        nbins=30,
        title="Sentiment Polarity Distribution",
        labels={'sentiment_polarity': 'Polarity Score', 'count': 'Number of Articles'},
        color_discrete_sequence=['#4e73df']
    )
    
    fig.add_vline(x=0, line_dash="dash", line_color="red", annotation_text="Neutral")
    
    return fig


def create_source_distribution(df):
    """Create top sources bar chart"""
    top_sources = df['source_name'].value_counts().head(10)
    
    fig = px.bar(
        x=top_sources.values,
        y=top_sources.index,
        orientation='h',
        title="Top 10 News Sources",
        labels={'x': 'Number of Articles', 'y': 'Source'},
        color=top_sources.values,
        color_continuous_scale='Blues'
    )
    
    fig.update_layout(showlegend=False)
    
    return fig


def display_article_cards(df, num_articles=5):
    """Display article cards with sentiment"""
    st.subheader(f"Latest {num_articles} Articles")
    
    for idx, row in df.head(num_articles).iterrows():
        # Determine sentiment color
        if row['sentiment_label'] == 'positive':
            sentiment_color = '🟢'
            sentiment_emoji = '😊'
        elif row['sentiment_label'] == 'negative':
            sentiment_color = '🔴'
            sentiment_emoji = '😞'
        else:
            sentiment_color = '⚪'
            sentiment_emoji = '😐'
        
        with st.container():
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.markdown(f"### {sentiment_color} {row['title']}")
                st.markdown(f"**Source:** {row['source_name']} | **Published:** {row['published_at'].strftime('%Y-%m-%d %H:%M') if pd.notna(row['published_at']) else 'N/A'}")
                
                if pd.notna(row['description']):
                    st.markdown(row['description'])
                
                if pd.notna(row['url']):
                    st.markdown(f"[Read full article]({row['url']})")
                
                if pd.notna(row['keywords']) and row['keywords']:
                    keywords = row['keywords'].split(',')[:5]
                    st.markdown(f"**Keywords:** {', '.join(keywords)}")
            
            with col2:
                st.metric("Sentiment", row['sentiment_label'].capitalize(), f"{sentiment_emoji}")
                st.metric("Polarity", f"{row['sentiment_polarity']:.3f}")
                st.metric("Subjectivity", f"{row['sentiment_subjectivity']:.3f}")
            
            st.divider()


def main():
    """Main dashboard function"""
    # Header
    st.title("📰 News Sentiment Analysis Dashboard")
    st.markdown("Real-time sentiment analysis of news articles powered by NewsData.io")
    
    # Initialize BigQuery client
    client = get_bigquery_client()
    
    if not client:
        st.error("Failed to connect to BigQuery. Please check your credentials.")
        return
    
    # Load data
    with st.spinner("Loading data from BigQuery..."):
        df = load_data(client)
    
    if df.empty:
        st.warning("No data available. Please run the producer and consumer to populate the database.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Sentiment filter
    sentiments = ['All'] + list(df['sentiment_label'].unique())
    selected_sentiment = st.sidebar.selectbox("Sentiment", sentiments)
    
    # Source filter
    sources = ['All'] + sorted(df['source_name'].dropna().unique().tolist())
    selected_source = st.sidebar.selectbox("Source", sources)
    
    # Date range filter
    if not df['published_at'].isna().all():
        min_date = df['published_at'].min().date()
        max_date = df['published_at'].max().date()
        
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_sentiment != 'All':
        filtered_df = filtered_df[filtered_df['sentiment_label'] == selected_sentiment]
    
    if selected_source != 'All':
        filtered_df = filtered_df[filtered_df['source_name'] == selected_source]
    
    if 'date_range' in locals() and len(date_range) == 2:
        filtered_df = filtered_df[
            (filtered_df['published_at'].dt.date >= date_range[0]) &
            (filtered_df['published_at'].dt.date <= date_range[1])
        ]
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Articles", len(filtered_df))
    
    with col2:
        positive_pct = (filtered_df['sentiment_label'] == 'positive').sum() / len(filtered_df) * 100 if len(filtered_df) > 0 else 0
        st.metric("Positive", f"{positive_pct:.1f}%")
    
    with col3:
        neutral_pct = (filtered_df['sentiment_label'] == 'neutral').sum() / len(filtered_df) * 100 if len(filtered_df) > 0 else 0
        st.metric("Neutral", f"{neutral_pct:.1f}%")
    
    with col4:
        negative_pct = (filtered_df['sentiment_label'] == 'negative').sum() / len(filtered_df) * 100 if len(filtered_df) > 0 else 0
        st.metric("Negative", f"{negative_pct:.1f}%")
    
    st.divider()
    
    # Display charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(
            create_sentiment_distribution_chart(filtered_df),
            use_container_width=True
        )
    
    with col2:
        st.plotly_chart(
            create_polarity_distribution(filtered_df),
            use_container_width=True
        )
    
    # Timeline chart
    st.plotly_chart(
        create_sentiment_timeline(filtered_df),
        use_container_width=True
    )
    
    # Source distribution
    st.plotly_chart(
        create_source_distribution(filtered_df),
        use_container_width=True
    )
    
    st.divider()
    
    # Display article cards
    num_articles = st.slider("Number of articles to display", 5, 20, 10)
    display_article_cards(filtered_df, num_articles)
    
    # Refresh button
    st.sidebar.divider()
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Footer
    st.sidebar.divider()
    st.sidebar.markdown(f"**Last updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.sidebar.markdown("**Data source:** NewsData.io")


if __name__ == "__main__":
    main()