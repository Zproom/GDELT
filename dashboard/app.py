# This is a streamlit app for viewing GDELT-based SURI scores between pairs of 
# countries.


import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from constants import DATA_PATH, COUNTRY_MAP

# -----------------------
# Functions
# -----------------------
@st.cache_data
def load_data() -> pd.DataFrame:
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Could not find data file at {DATA_PATH.resolve()}."
        )
    df = pd.read_csv(DATA_PATH, parse_dates=["year_month"])
    return df.sort_values("year_month")

# -----------------------
# Other setup
# -----------------------
df = load_data()

# Add readable country names.
df["source_country"] = df["Actor1CountryCode"].map(COUNTRY_MAP)
df["target_country"] = df["Actor2CountryCode"].map(COUNTRY_MAP)

# -----------------------------
# Sidebar controls
# -----------------------------
st.sidebar.header("Select Country Pair")

source_country = st.sidebar.selectbox(
    "Source actor (initiator)",
    sorted(df["source_country"].unique())
)

target_country = st.sidebar.selectbox(
    "Target actor",
    sorted(df["target_country"].unique())
)

# Filter data
filtered_df = df[
    (df["source_country"] == source_country) &
    (df["target_country"] == target_country)
].sort_values("year_month")

# -----------------------------
# Main page
# -----------------------------
st.title("Social Unrest Risk Index (SURI)")
st.markdown(
    """
    The **Social Unrest Risk Index (SURI)** is a **directional, monthly metric**
    that measures the intensity of unrest-related activity **from one country
    toward another**, based on global news reporting from the GDELT Project.

    This dashboard allows you to explore SURI scores over time for any pair of
    countries in the dataset, which starts in January 2025 and includes a
    subset of countries in the original dataset.
    
    To select a country pair, use the dropdown menus in the sidebar. The chart 
    will show how the SURI score evolves over time for that specific relationship.
    """
)

# -----------------------------
# Chart
# -----------------------------
st.subheader(
    f"SURI: {source_country} → {target_country}"
)

if filtered_df.empty:
    st.warning("No data available for this country pair.")
else:
    st.line_chart(
        filtered_df.set_index("year_month")["suri_score"]
    )

# -----------------------------
# Show underlying data
# -----------------------------
with st.expander("View underlying monthly data"):
    st.dataframe(
        filtered_df[
            [
                "year_month",
                "suri_score",
                "geo_unrest_score",
                "pol_involve_score",
                "total_events",
            ]
        ]
    )

# -----------------------------
# SURI explanation
# -----------------------------
with st.expander("How is the SURI score calculated?", expanded=False):
    st.markdown(
        """
        ### Step 1: Identify unrest-related events  
        From all events performed by a **source actor** 
        (Actor1CountryCode) on a **target actor** (Actor2CountryCode) in a 
        given month, I identify **unrest-related events** using GDELT CAMEO 
        event codes:
        - **10**: Demands
        - **13**: Threats
        - **14**: Protests

        The total number of these events is the **Geopolitical Unrest Score**.

        ### Step 2: Measure political involvement  
        I measure how directly governments are involved in the relationship 
        using the Actor1Type1Code and Actor2Type1Code fields, which classify 
        actors as government (GOV) vs. non-government (e.g., rebel groups, 
        protestors, etc.). I count the number of events in each of the 
        following categories:

        - Government → Government events (Actor1Type1Code = GOV and Actor2Type1Code = GOV)
        - Non-government → Government events (Actor1Type1Code != GOV and Actor2Type1Code = GOV)
        - Government → Non-government events (Actor1Type1Code = GOV and Actor2Type1Code != GOV) 

        These are summed and divided by the total number of events performed by
        the source actor on the target actor to compute the 
        **Political Involvement Score**:

        **Political Involvement Score =  
        (Gov–Gov + NonGov–Gov + Gov–NonGov) / Total Events**

        ### Step 3: Compute SURI  
        The final **SURI score** is the product of these two components:

        **SURI = Geopolitical Unrest Score × Political Involvement Score**

        This design emphasizes unrest that is both **frequent** and
        **politically salient**.
        """
    )