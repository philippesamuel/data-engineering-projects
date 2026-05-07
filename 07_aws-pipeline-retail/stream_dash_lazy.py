import streamlit as st
import polars as pl
import plotly.express as px

PARQUET_PATH = "s3://retail-demo-pc/curated/**/run-1777375074596*.parquet"

# Page configuration
st.set_page_config(layout="wide", page_title="Retail Sales Dashboard")

# 1. Lazy Data Loading
@st.cache_resource # Cache the LazyFrame object (the query plan)
def get_lazy_data():
    # Use glob pattern for partitioned files
    # The scan doesn't read the files yet
    return pl.scan_parquet(PARQUET_PATH, hive_partitioning=True)

# 2. Aggregations on LazyFrame
# We create a function to get the aggregated data
@st.cache_data # Cache the result of the aggregation
def get_aggregated_data() -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    lf = get_lazy_data()
    
    # We aggregate first, then collect() to pull ONLY the small result to RAM
    # This prevents loading the 500k rows into Streamlit's memory
    monthly_trend = (
        lf.with_columns(
            pl.concat_str(
                [pl.col("year"), pl.col("month")], 
                separator="-")
            .alias("year_month")
        )
        .drop(["year", "month"])
        .group_by("year_month")
        .agg(pl.col("revenue").sum())
        .sort("year_month")
    )
    
    country_rev = (
        lf.group_by("country")
        .agg(pl.col("revenue").sum())
    )
    
    top_products = (
        lf.group_by("description")
        .agg(pl.col("revenue").sum())
        .sort("revenue", descending=True)
        .head(10)
    )
    
    return monthly_trend, country_rev, top_products

# Load the aggregated data
monthly_trend, country_rev, top_products = get_aggregated_data()
country_with_coords=country_rev
# coords_df = pl.scan_csv("./data/raw/countries_with_coords.csv")
# country_with_coords = (
#     country_rev.join(coords_df, on="country", how="right")
#     .with_columns(pl.col("revenue").log2().alias("log_revenue"))
#     )

# 3. Dashboard UI
st.title("Retail Sales Dashboard")

# (The rest of your plotting code remains the same, using the aggregated dataframes)
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Monthly Trend")
    st.line_chart(monthly_trend.collect().to_pandas().set_index("year_month"))
    
    st.subheader("Revenue per Country")
    # st.map(country_with_coords.collect().to_pandas())
    
    fig = fig = px.choropleth(
        data_frame=country_with_coords.collect().to_pandas(),
        locations="country",
        locationmode="country names",
        # size="revenue",
        hover_name="country",
        hover_data={"revenue": ":,.2f"},
        color="revenue",
        color_continuous_scale=px.colors.sequential.Tealgrn
    )
    fig.update_geos(
        visible=False,           # Hides the default frame, latitude/longitude lines
        showcountries=True,      # Shows country borders
        countrycolor="LightGrey",
        showland=True,
        landcolor="#f5f5f5",     # Very light grey land
        showocean=True,
        oceancolor="#ffffff"     # White ocean for a "floating" feel
    )

    fig.update_layout(
        margin={"r":0,"t":0,"l":0,"b":0}, # Removes all padding around the map
        coloraxis_showscale=False,       # Hides the color legend if you prefer a cleaner look
        paper_bgcolor="rgba(0,0,0,0)",   # Transparent background to blend with Streamlit
        plot_bgcolor="rgba(0,0,0,0)",
        coloraxis_colorbar=dict(
            title="Revenue (£)",
            thickness=15,          # Make it slimmer
            len=0.5,               # Make it shorter
            x=0.05,                # Move it to the left side
            y=0.2,                 # Move it near the bottom
            ticks="outside"
        )
        
    )
    
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Top Products")
    st.bar_chart(top_products.collect().to_pandas().set_index("description"))