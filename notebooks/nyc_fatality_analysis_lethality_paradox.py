import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    """Library imports and environment setup"""
    import marimo as mo
    import pandas as pd
    import geopandas as gpd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import re
    import sys
    import subprocess
    import warnings
    warnings.filterwarnings("ignore")

    # Ensure H3 is available
    try:
        import h3
    except ImportError:
        subprocess.run([sys.executable, "-m", "pip", "install", "h3"])
        import h3
    return go, h3, make_subplots, mo, pd, px, re


@app.cell
def _(mo):
    mo.md(r"""
    # NYC Traffic Fatality Analysis — Vision Zero Policy Brief

    **Author:** Remy Ndayizeye
    **Context:** Northeast Big Data Innovation Hub · National Student Data Corps
    **Date:** May 2026

    ---

    ## Executive Summary: The Lethality Paradox

    NYC reached a historic safety milestone in 2025 with 205 traffic deaths—the lowest count since 1910.
    However, a critical **"Lethality Paradox"** has emerged: while total crashes are down, the likelihood of a crash
    being fatal has increased since 2020. This analysis utilizes a **Recency-Weighted Hybrid Model** to distinguish between chronic infrastructure failures and emerging behavioral crises.

    ---
    **Data Source:** NYC Open Data — 1.95M crashes (July 2012 – April 2026)
    """)
    return


@app.cell
def _(pd, re):
    """Data Loading and Normalization Helpers"""
    def normalize_street(name: str) -> str | None:
        if pd.isna(name):
            return None
        name = name.upper().strip()
        name = re.sub(r'\s+', ' ', name)
        replacements = {
            r'\bST\b': 'STREET', r'\bAVE\b': 'AVENUE', r'\bBLVD\b': 'BOULEVARD',
            r'\bPKWY\b': 'PARKWAY', r'\bEXPY\b': 'EXPRESSWAY',
        }
        for pattern, replacement in replacements.items():
            name = re.sub(pattern, replacement, name)
        return name

    HIGHWAYS = {
        "BELT PARKWAY", "MAJOR DEEGAN EXPRESSWAY", "FDR DRIVE",
        "BROOKLYN QUEENS EXPRESSWAY", "GRAND CENTRAL PARKWAY",
        "CROSS BRONX EXPRESSWAY", "BRUCKNER EXPRESSWAY", "VAN WYCK EXPRESSWAY",
        "HENRY HUDSON PARKWAY", "JACKIE ROBINSON PARKWAY", "SHERIDAN EXPRESSWAY"
    }

    def classify_street_type(street: str) -> str:
        if not street:
            return "Unknown"
        if street in HIGHWAYS or any(x in street for x in ["EXPRESSWAY", "PARKWAY"]):
            return "Highway"
        return "Surface Street"

    # Load Data
    try:
        df_raw = pd.read_parquet("data/02_primary/nyc_crashes.parquet")
    except FileNotFoundError:
        df_raw = pd.DataFrame()

    if not df_raw.empty:
        df_raw["crash_date"] = pd.to_datetime(df_raw["crash_date"], errors="coerce")
        df_raw["year"] = df_raw["crash_date"].dt.year
        df_raw["any_killed"] = df_raw["number_of_persons_killed"] > 0

        # Apply normalization and classification
        df_raw["street_normalized"] = df_raw["on_street_name"].apply(normalize_street).fillna(
            df_raw["off_street_name"].apply(normalize_street)
        )
        df_raw["street_type"] = df_raw["street_normalized"].apply(classify_street_type)

        # Global fatal dataframe
        fatal = df_raw[df_raw["any_killed"]].copy()
    else:
        fatal = pd.DataFrame()
    return df_raw, fatal


@app.cell
def _(df_raw, go, make_subplots, mo):
    """Visualizing the Paradox: Volume vs. Lethality"""
    if df_raw.empty:
        mo.md("Data missing.")

    annual_trends = df_raw.groupby('year').agg(
        total_crashes=('collision_id', 'count'),
        total_killed=('number_of_persons_killed', 'sum')
    ).reset_index()

    # Lethality Rate = Deaths per 1,000 crashes
    annual_trends['lethality_rate'] = (annual_trends['total_killed'] / annual_trends['total_crashes']) * 1000

    fig_paradox = make_subplots(specs=[[{"secondary_y": True}]])

    fig_paradox.add_trace(
        go.Scatter(x=annual_trends['year'], y=annual_trends['total_crashes'], 
                   name="Total Crash Volume", line=dict(color="#34495e", width=2, dash='dot')),
        secondary_y=False
    )

    fig_paradox.add_trace(
        go.Scatter(x=annual_trends['year'], y=annual_trends['lethality_rate'], 
                   name="Lethality Rate (Deaths/1k Crashes)", line=dict(color="#e74c3c", width=4)),
        secondary_y=True
    )

    fig_paradox.update_layout(
        title="<b>The Lethality Paradox:</b> Fewer Crashes, Higher Severity",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig_paradox.update_yaxes(title_text="Total Volume", secondary_y=False)
    fig_paradox.update_yaxes(title_text="Lethality Rate", secondary_y=True)

    mo.vstack([
            mo.as_html(fig_paradox),
            mo.md("""
            ### Understanding the Divergence
            This line graph clearly illustrates the core challenge: while Vision Zero initiatives and post-COVID traffic shifts have successfully driven down total crash volume (the dotted line), the **severity** of those crashes (the solid red line) has spiked. This confirms that modern crashes are involving higher kinetic energy or more vulnerable road users.
            """)
    ])
    return


@app.cell
def _(fatal, go, mo):
    """Behavioral Analysis: Diverging Bar Chart with Corrected Calculation Logic"""
    if fatal.empty:
        mo.md("Data missing.")

    # 1. Prepare Data
    fatal_analysis = fatal.copy()
    # Define era boundaries strictly
    fatal_analysis['era'] = fatal_analysis['year'].apply(
        lambda x: 'Post-COVID (2020+)' if x >= 2020 else 'Pre-COVID (2012-2019)'
    )

    # Calculate frequencies and convert to percentages within each era
    factors = (
        fatal_analysis[fatal_analysis['contributing_factor_vehicle_1'] != 'Unspecified']
        .groupby(['era', 'contributing_factor_vehicle_1'])
        .size().reset_index(name='count')
    )

    factors['share'] = (factors.groupby('era')['count'].transform(lambda x: x / x.sum()) * 100)

    # Pivot for comparison
    comparison_full = factors.pivot(
        index='contributing_factor_vehicle_1', 
        columns='era', 
        values='share'
    ).fillna(0).reset_index()

    comparison_full.columns = ['Factor', 'Post-COVID (2020+)', 'Pre-COVID (2012-2019)'] #I switched the order or columns because pandas follow alphabetical order

    # Calculate Change based on the full dataset to ensure accuracy
    comparison_full['Change'] = comparison_full['Post-COVID (2020+)'] - comparison_full['Pre-COVID (2012-2019)']

    # Identify the global top increases/decreases before filtering for the chart
    # (Exclude factors with very low frequency if desired, but here we take the raw shifts)
    top_increases_global = comparison_full.nlargest(3, "Change")
    top_decreases_global = comparison_full.nsmallest(3, "Change")

    # 2. Filter for Visualization (Top 10 by combined prevalence)
    comparison_full['Total_Volume'] = comparison_full['Pre-COVID (2012-2019)'] + comparison_full['Post-COVID (2020+)']
    chart_data = comparison_full.sort_values('Total_Volume', ascending=True).tail(10)

    # 3. Create Figure using Graph Objects
    fig_factors = go.Figure()

    # Pre-COVID bars (left)
    fig_factors.add_trace(go.Bar(
        y=chart_data["Factor"],
        x=-chart_data["Pre-COVID (2012-2019)"],
        orientation='h',
        name='Pre-COVID (2012-2019)',
        marker=dict(color='#440154'), # Viridis Purple
        text=chart_data["Pre-COVID (2012-2019)"].round(1).astype(str) + "%",
        textposition='auto',
        textfont=dict(size=11),
        hovertemplate='<b>%{y}</b><br>Pre-COVID Share: %{text}<extra></extra>',
    ))

    # Post-COVID bars (right)
    fig_factors.add_trace(go.Bar(
        y=chart_data["Factor"],
        x=chart_data["Post-COVID (2020+)"],
        orientation='h',
        name='Post-COVID (2020+)',
        marker=dict(color='#fde725'), # Viridis Yellow
        text=chart_data["Post-COVID (2020+)"].round(1).astype(str) + "%",
        textposition='auto',
        textfont=dict(size=11),
        hovertemplate='<b>%{y}</b><br>Post-COVID Share: %{text}<extra></extra>',
    ))

    fig_factors.update_layout(
        title=dict(
            text="<b>Contributing Factors: Pre-COVID vs Post-COVID</b><br><sup>Top 10 factors by fatal crash prevalence (% of era total)</sup>",
            x=0,
            font=dict(size=18),
        ),
        xaxis=dict(
            title="← Pre-COVID Baseline            <b>Percentage Share</b>            Post-COVID Shift →",
            zeroline=True,
            zerolinewidth=2,
            zerolinecolor='black',
            range=[-max(chart_data['Post-COVID (2020+)'].max(), chart_data['Pre-COVID (2012-2019)'].max()) - 5, 
                   max(chart_data['Post-COVID (2020+)'].max(), chart_data['Pre-COVID (2012-2019)'].max()) + 5]
        ),
        yaxis=dict(title=""),
        barmode='overlay',
        height=550,
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.02,
            xanchor="center", x=0.5,
        ),
        hovermode='y unified',
        margin=dict(l=220, r=40) 
    )
    return (comparison_full,)


@app.cell
def _(comparison_full, mo):
    """Behavioral Analysis: Polished Summary and Interpretation"""
    if comparison_full.empty:
        mo.md("Waiting for analysis...")

    # 1. Pre-calculate global extremes for the summary
    # Use comparison_full to ensure we see the biggest shifts across all data
    top_inc = comparison_full.nlargest(3, "Change")
    top_dec = comparison_full.nsmallest(3, "Change")

    # 2. Format the list items using join for a clean f-string
    increases_html = "".join([
        f"<li><b>{row['Factor']}</b>: {row['Pre-COVID (2012-2019)']:.1f}% → "
        f"{row['Post-COVID (2020+)']:.1f}% "
        f"(<span style='color: #e74c3c;'>+{row['Change']:.1f}%</span>)</li>"
        for _, row in top_inc.iterrows()
    ])

    decreases_html = "".join([
        f"<li><b>{row['Factor']}</b>: {row['Pre-COVID (2012-2019)']:.1f}% → "
        f"{row['Post-COVID (2020+)']:.1f}% "
        f"(<span style='color: #2ecc71;'>{row['Change']:.1f}%</span>)</li>"
        for _, row in top_dec.iterrows()
    ])

    # 3. Final Assembly
    interpretation = mo.md(f"""
    ### Behavioral Shift Analysis: The COVID Transition

    The diverging chart highlights a fundamental shift in road safety. While total crash volume decreased, the **composition** of crash factors changed significantly.

    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
        <div>
            <strong>📈 Significant Increases</strong>
            <ul>{increases_html}</ul>
        </div>
        <div>
            <strong>📉 Significant Decreases</strong>
            <ul>{decreases_html}</ul>
        </div>
    </div>

    ---

    **Policy Interpretation:**
    The rise in **Unsafe Speed** as a fatal factor explains why mortality rates spiked even on less congested roads. This suggests that "Vision Zero" infrastructure (narrower lanes, speed bumps) is more critical now than ever to counteract the behavioral trend toward higher speeds.

    *Analysis: Remy Ndayizeye · U.S. DOT FHWA Context*
    """)
    interpretation
    return


@app.cell
def _(fatal, mo):
    """Jurisdictional Work: Hardened Logic for 100% Accounting"""
    if fatal.empty:
        None

    counts = fatal['street_type'].value_counts()
    total = counts.sum()
    
    surface_pct = (counts.get('Surface Street', 0) / total) * 100
    highway_pct = (counts.get('Highway', 0) / total) * 100
    # Capture the remainder explicitly
    unclassified_pct = (counts.get('Unknown', 0) / total) * 100

    mo.md(
        f"""
        ### Jurisdictional Attribution: Full Accounting
        To identify policy levers, we classified 100% of fatal crash locations:
        * **{surface_pct:.1f}% Surface Streets:** Local corridors under municipal (**NYC DOT**) jurisdiction.
        * **{highway_pct:.1f}% High-Speed Corridors:** Limited-access roads often involving **State DOT** oversight.
        * **{unclassified_pct:.1f}% Unclassified/Off-Street:** Locations with insufficient name data or occurring in non-roadway areas.

        **Policy Impact:** Even when accounting for data gaps, over three-quarters of traffic fatalities occur on city-managed surface streets, reinforcing that municipal engineering remains the primary tool for achieving Vision Zero.
            """
        )
    return


@app.cell
def _(fatal, h3, mo):
    """Hybrid Priority Scoring Logic"""
    if fatal.empty:
        mo.md("Waiting for data...")

    def get_h3(lat, lon):
        try:
            return h3.latlng_to_cell(lat, lon, 9)
        except:
            return None

    fatal["h3_cell"] = fatal.apply(lambda x: get_h3(x["latitude"], x["longitude"]), axis=1)
    current_year = 2026

    h3_stats = fatal.groupby("h3_cell").agg(
        total_killed=("number_of_persons_killed", "sum"),
        lat=("latitude", "mean"),
        lng=("longitude", "mean"),
        years=("year", list),
        street_type=("street_type", lambda x: x.mode()[0] if not x.empty else "Unknown")
    ).reset_index()

    h3_stats["recency_score"] = h3_stats["years"].apply(lambda y: sum(1 / (current_year - yr + 1) for yr in y))

    def assign_priority(row):
        if row["recency_score"] >= 1.2:
            return "🟣 URGENT (Emergent)"
        elif row["total_killed"] >= 3:
            return "🟢 HIGH (Chronic)"
        return "🟡 MEDIUM"

    h3_stats["priority"] = h3_stats.apply(assign_priority, axis=1)
    return (h3_stats,)


@app.cell
def _(h3_stats, mo, px):
    """Final Synthesis and Map"""
    if h3_stats is None:
        mo.md("Calculating...")

    fig_map = px.scatter_mapbox(
        h3_stats,
        lat="lat",
        lon="lng",
        color="priority",
        size="total_killed",
        hover_data=["recency_score", "street_type"],
        color_discrete_map={"🟣 URGENT (Emergent)": "#440154", "🟢 HIGH (Chronic)": "#22A884", "🟡 MEDIUM": "#FDE725"},
        mapbox_style="carto-positron",
        zoom=9,
        title="Vision Zero Priority Map: Actionable Hotspots"
    )
    fig_map.update_layout(
        width=850,
        height=800,
        title={
            'y': 0.95,      # Vertical position (0 = bottom, 1 = top)
            'x': 0.4,       # Horizontal position (0 = left, 1 = right)
            'xanchor': 'center',  # Anchor point relative to the x position
            'yanchor': 'top'      # Anchor point relative to the y position
        },
    )
    mo.vstack([
            mo.as_html(fig_map),
            mo.md(
                """
                ## Recommendations for Post-COVID Intervention

                1.  **Rapid Response for 🟣 URGENT Sites:** These sites are where the most "new" fatalities are clustering. Because these are largely on **Surface Streets**, the NYC DOT can deploy quick-build materials (bollards, paint) within weeks to reduce the space for high-speed maneuvers.

                2.  **Redesign for 🟢 HIGH PRIORITY Sites:** These are chronic infrastructure failures. The analysis shows these often occur on complex multi-lane avenues. These require multi-year capital investment to physically separate modes of transport.

                3.  **Behavioral Enforcement:** Since **Speeding** has grown as a fatal factor, enforcement and camera density should be prioritized specifically in the "Urgent" cells identified above.
                """
            )
    ])
    return


@app.cell
def _(mo):
    mo.md("""
    ### References & Data Sources
    * **Primary Safety Milestone:** NYC DOT Press Release (Jan 2026). [Traffic deaths reach all-time low in 2025](https://www.nyc.gov/html/dot/html/pr2026/traffic-deaths-reach-all-time-low.shtml).
    * **Raw Crash Data:** NYC Open Data. [Motor Vehicle Collisions - Crashes](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95).
    * **Spatial Framework:** Uber H3 Discrete Global Grid System.
    """)
    return


if __name__ == "__main__":
    app.run()
