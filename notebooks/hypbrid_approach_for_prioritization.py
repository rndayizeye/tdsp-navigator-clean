import marimo

__generated_with = "0.23.5"
app = marimo.App(width="full")


@app.cell
def _():
    """Library imports"""
    import marimo as mo
    import pandas as pd
    import geopandas as gpd
    import plotly.express as px
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import re
    import warnings
    warnings.filterwarnings("ignore")
    return gpd, mo, pd, px


@app.cell
def _():
    """Install H3 for spatial binning"""
    import subprocess
    try:
        import h3
    except ImportError:
        subprocess.run(['pip', 'install', 'h3', '--break-system-packages'], 
                      capture_output=True)
        import h3
    return (h3,)


@app.cell
def _(mo):
    mo.md(r"""
    # NYC Traffic Fatality Analysis — Hybrid Priority Framework

    **Remy Ndayizeye** Northeast Big Data Innovation Hub · National Student Data Corps · U.S. DOT FHWA
    *May 2026*

    ---

    ## Executive Summary
    This analysis pivots from complex change-point detection to a **Recency-Weighted Hybrid Model**. Because fatal crashes at single intersections are statistically sparse events, traditional time-series models often fail. By weighting recent deaths more heavily and comparing Pre- vs. Post-COVID counts, we identify which "hotspots" are historical artifacts and which represent emerging behavioral crises.
    """)
    return


@app.cell
def _(pd):
    """Load and prepare crash data"""
    # Note: Ensure the parquet file exists in your local directory
    df_raw: pd.DataFrame = pd.read_parquet("data/02_primary/nyc_crashes.parquet")

    df_raw["crash_date"] = pd.to_datetime(df_raw["crash_date"], errors="coerce")
    df_raw["year"] = df_raw["crash_date"].dt.year
    df_raw["any_killed"] = df_raw["number_of_persons_killed"] > 0

    print(f"✓ Loaded {len(df_raw):,} crashes")
    return (df_raw,)


@app.cell
def _(h3):
    """Helper: Spatial Binning and Hotspot Classification"""
    def get_hotspots(df, resolution=9):
        fatal = df[
            df["any_killed"] & 
            df["latitude"].notna() & 
            df["longitude"].notna()
        ].copy()

        fatal["h3_cell"] = fatal.apply(
            lambda r: h3.latlng_to_cell(r["latitude"], r["longitude"], resolution), axis=1
        )

        stats = fatal.groupby("h3_cell").agg(
            total_killed=("number_of_persons_killed", "sum"),
            crash_count=("collision_id", "count"),
            borough=("borough", lambda x: x.mode()[0] if not x.mode().empty else "Unknown")
        ).reset_index()

        # Filter for hotspots (min 2 deaths)
        hotspots = stats[stats["total_killed"] >= 2].copy()

        # Determine if Isolated or Corridor
        h3_set = set(hotspots["h3_cell"])
        hotspots["neighbors"] = hotspots["h3_cell"].apply(
            lambda c: len([n for n in h3.grid_disk(c, 1) if n in h3_set and n != c])
        )
        hotspots["pattern"] = hotspots["neighbors"].apply(
            lambda n: "Corridor" if n >= 3 else "Isolated"
        )

        return hotspots, fatal

    return (get_hotspots,)


@app.cell
def _(df_raw: "pd.DataFrame", get_hotspots, gpd, h3):
    """Execute Hotspot Detection"""
    hotspots_df, fatal_enriched = get_hotspots(df_raw)
    isolated_top_20 = hotspots_df[hotspots_df["pattern"] == "Isolated"].nlargest(20, "total_killed")

    # Add coordinates for mapping
    isolated_top_20["lat"] = isolated_top_20["h3_cell"].apply(lambda c: h3.cell_to_latlng(c)[0])
    isolated_top_20["lng"] = isolated_top_20["h3_cell"].apply(lambda c: h3.cell_to_latlng(c)[1])

    # 2. FORCE CRS CHECK: If your raw data was in State Plane (feet), 
    # the H3 binning might have failed or shifted. 
    # Let's ensure the final plotting dataframe is clean:
    gdf = gpd.GeoDataFrame(
        isolated_top_20, 
        geometry=gpd.points_from_xy(isolated_top_20.lng, isolated_top_20.lat),
        crs="EPSG:4326"
    )
    return fatal_enriched, gdf, isolated_top_20


@app.cell
def _(fatal_enriched, isolated_top_20, pd):
    """Hybrid Priority Scoring Logic"""
    current_year = 2026
    covid_start = 2020

    results = []
    for _, row in isolated_top_20.iterrows():
        cell_id = row["h3_cell"]
        cell_crashes = fatal_enriched[fatal_enriched["h3_cell"] == cell_id]

        # 1. First Fatal Year
        first_year = cell_crashes["year"].min()

        # 2. Trend Classification
        pre_covid = len(cell_crashes[cell_crashes["year"] < covid_start])
        post_covid = len(cell_crashes[cell_crashes["year"] >= covid_start])

        if first_year >= covid_start:
            trend = "Emergent (New)"
        elif post_covid > pre_covid:
            trend = "Worsening"
        else:
            trend = "Persistent"

        # 3. Recency Score: Inverse distance from current year
        # Deaths in 2025/2026 carry significantly more weight
        recency_score = sum(1 / (current_year - cell_crashes["year"] + 1))

        # 4. Priority Logic
        if (trend in ["Emergent (New)", "Worsening"]) and (recency_score > 1.2):
            priority = "🔴 URGENT"
        elif trend == "Persistent" and row["total_killed"] >= 5:
            priority = "🟠 HIGH"
        else:
            priority = "🟡 MEDIUM"

        results.append({
            "Borough": row["borough"],
            "Total Killed": row["total_killed"],
            "Trend": trend,
            "First Fatal": int(first_year),
            "Recency Score": round(recency_score, 2),
            "Priority": priority,
            "h3_cell": cell_id
        })

    priority_table = pd.DataFrame(results).sort_values("Recency Score", ascending=False)
    return (priority_table,)


@app.cell
def _(mo, priority_table):
    mo.vstack([
        mo.md("## Final Priority Matrix: Isolated Hotspots"),
        mo.ui.table(priority_table.drop(columns=["h3_cell"])),
        mo.md(
            f"""
            ### Intervention Strategy
            - **{len(priority_table[priority_table['Priority'] == '🔴 URGENT'])} Urgent Sites**: 
              Requires immediate NYPD enforcement and NYC DOT "Quick-Build" (bollards, signal timing).
            - **{len(priority_table[priority_table['Priority'] == '🟠 HIGH'])} High Priority Sites**: 
              Long-term chronic failures requiring capital reconstruction (raised crosswalks, curb extensions).
            """
        )
    ])
    return


@app.cell
def _(gdf, priority_table, px):
    """Geospatial Visualization with Explicit CRS Projection"""

    # 1. Merge the scoring results with our GeoDataFrame
    map_data = priority_table.merge(
        gdf[['h3_cell', 'lat', 'lng', 'geometry']], on='h3_cell'
    )

    # 2. Ensure we are in WGS84 (Decimal Degrees)
    map_data = map_data.set_geometry("geometry").to_crs("EPSG:4326")

    # Update lat/lng from the transformed geometry to be safe
    map_data['lat'] = map_data.geometry.y
    map_data['lng'] = map_data.geometry.x

    fig_map = px.scatter_mapbox(
        map_data,
        lat="lat",
        lon="lng",
        color="Priority",
        size="Total Killed",
        hover_name="Borough",
        color_discrete_map={
            "🔴 URGENT": "#e74c3c",
            "🟠 HIGH": "#e67e22",
            "🟡 MEDIUM": "#f1c40f"
        },
        zoom=9.5, 
        center={"lat": 40.7128, "lon": -74.0060}, # Forced NYC Center
        height=700,
        title="<b>NYC Fatality Hotspots: Corrected Projection</b>"
    )

    fig_map.update_layout(
        mapbox_style="carto-positron", # No API key required
        margin={"r":0,"t":40,"l":0,"b":0}
    )

    return (fig_map,)


@app.cell
def _(fig_map, mo):
    mo.ui.plotly(fig_map)
    return


if __name__ == "__main__":
    app.run()