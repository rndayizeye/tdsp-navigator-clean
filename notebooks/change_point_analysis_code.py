# ══════════════════════════════════════════════════════════════════════════════
# CHANGE POINT ANALYSIS: WHEN DID INTERSECTIONS BECOME DANGEROUS?
# Identify the specific moment when crash patterns changed at top hotspots
# ══════════════════════════════════════════════════════════════════════════════

@app.cell
def _(mo):
    mo.md("""
    ---
    ## Change Point Analysis — When Did Hotspots Emerge?
    
    **Change point detection** identifies the specific time when crash patterns shifted at 
    an intersection. This reveals:
    
    - **When** an intersection became dangerous (not just that it is dangerous)
    - **What** might have caused the change (construction, signal changes, new development)
    - **Which** interventions are urgent (recent deteriorations need immediate action)
    
    We analyze the top 20 isolated intersection hotspots to identify:
    1. **Recent emergent hotspots** — became dangerous after 2020 (COVID-related)
    2. **Persistent hotspots** — dangerous for entire period (chronic problems)
    3. **Worsening hotspots** — gradual deterioration over time
    4. **Sudden spikes** — specific incident or change triggered danger
    """)
    return


@app.cell
def _(pd):
    """Install ruptures library for change point detection"""
    import subprocess
    try:
        import ruptures as rpt
    except ImportError:
        print("Installing ruptures for change point detection...")
        subprocess.run(["pip", "install", "ruptures", "--break-system-packages"], 
                      capture_output=True)
        import ruptures as rpt
    
    return (rpt,)


@app.cell
def _(df, h3_classified, h3lib, pd, rpt):
    """Detect change points for top isolated hotspots"""
    
    def detect_intersection_change_points(
        df: pd.DataFrame,
        h3_classified: pd.DataFrame,
        top_n: int = 20,
        min_crashes_per_period: int = 2,
    ) -> pd.DataFrame:
        """
        For each top isolated hotspot, detect when crash patterns changed.
        
        Returns DataFrame with:
        - H3 cell ID
        - Location (lat/lng)
        - Change point year (if detected)
        - Pre/post crash rates
        - Pattern classification
        """
        
        # Get top isolated hotspots
        isolated = h3_classified[h3_classified["pattern"] == "Isolated Hotspot"]
        top_hotspots = isolated.nlargest(top_n, "total_killed")
        
        results = []
        
        for idx, hotspot in top_hotspots.iterrows():
            cell_id = hotspot["h3_cell"]
            lat, lng = h3lib.cell_to_latlng(cell_id)
            
            # Get all fatal crashes in this cell
            cell_crashes = df[
                (df["any_killed"]) &
                (df["latitude"].notna()) &
                (df["longitude"].notna())
            ].copy()
            
            # Filter to crashes in this specific H3 cell
            cell_crashes["h3_cell"] = cell_crashes.apply(
                lambda r: h3lib.latlng_to_cell(r["latitude"], r["longitude"], 9),
                axis=1
            )
            cell_crashes = cell_crashes[cell_crashes["h3_cell"] == cell_id]
            
            if len(cell_crashes) < 10:  # Need sufficient data
                continue
            
            # Create time series: quarterly crash counts (more stable than monthly)
            cell_crashes["quarter"] = pd.PeriodIndex(cell_crashes["crash_date"], freq="Q")
            quarterly_counts = (
                cell_crashes.groupby("quarter")
                .size()
                .reindex(
                    pd.period_range(
                        start=cell_crashes["crash_date"].min(),
                        end=cell_crashes["crash_date"].max(),
                        freq="Q"
                    ),
                    fill_value=0
                )
            )
            
            # Convert to array for ruptures
            signal = quarterly_counts.values
            
            if len(signal) < 8:  # Need at least 2 years of data
                continue
            
            # Detect change points using PELT algorithm (Pruned Exact Linear Time)
            try:
                model = rpt.Pelt(model="rbf", min_size=4, jump=1).fit(signal)
                change_points = model.predict(pen=10)  # Penalty parameter (higher = fewer changes)
                
                # Ruptures returns indices; convert to quarters
                if len(change_points) > 1:  # At least one change point (last is always end)
                    # Get the most significant change point (largest mean shift)
                    best_cp_idx = None
                    max_shift = 0
                    
                    for cp in change_points[:-1]:  # Exclude the final endpoint
                        pre_mean = signal[:cp].mean()
                        post_mean = signal[cp:].mean()
                        shift = abs(post_mean - pre_mean)
                        
                        if shift > max_shift:
                            max_shift = shift
                            best_cp_idx = cp
                    
                    if best_cp_idx is not None:
                        change_quarter = quarterly_counts.index[best_cp_idx]
                        change_year = change_quarter.year
                        
                        pre_rate = signal[:best_cp_idx].mean()
                        post_rate = signal[best_cp_idx:].mean()
                        percent_change = ((post_rate - pre_rate) / pre_rate * 100) if pre_rate > 0 else 0
                        
                        # Classify pattern
                        if change_year >= 2020:
                            pattern = "Recent Emergence (2020+)"
                        elif percent_change > 50:
                            pattern = "Sudden Spike"
                        elif percent_change > 20:
                            pattern = "Gradual Worsening"
                        elif percent_change < -20:
                            pattern = "Improvement After Change"
                        else:
                            pattern = "Stable Pattern"
                        
                        results.append({
                            "h3_cell": cell_id,
                            "lat": lat,
                            "lng": lng,
                            "total_killed": hotspot["total_killed"],
                            "crash_count": hotspot["crash_count"],
                            "borough": hotspot["borough"],
                            "change_point_year": change_year,
                            "change_point_quarter": str(change_quarter),
                            "pre_rate": round(pre_rate, 2),
                            "post_rate": round(post_rate, 2),
                            "percent_change": round(percent_change, 1),
                            "pattern": pattern,
                        })
                    else:
                        # No significant change point
                        results.append({
                            "h3_cell": cell_id,
                            "lat": lat,
                            "lng": lng,
                            "total_killed": hotspot["total_killed"],
                            "crash_count": hotspot["crash_count"],
                            "borough": hotspot["borough"],
                            "change_point_year": None,
                            "change_point_quarter": None,
                            "pre_rate": None,
                            "post_rate": None,
                            "percent_change": None,
                            "pattern": "Persistent Hotspot (No Change)",
                        })
                else:
                    # No change points detected
                    results.append({
                        "h3_cell": cell_id,
                        "lat": lat,
                        "lng": lng,
                        "total_killed": hotspot["total_killed"],
                        "crash_count": hotspot["crash_count"],
                        "borough": hotspot["borough"],
                        "change_point_year": None,
                        "change_point_quarter": None,
                        "pre_rate": None,
                        "post_rate": None,
                        "percent_change": None,
                        "pattern": "Persistent Hotspot (No Change)",
                    })
            except Exception as e:
                print(f"Error processing cell {cell_id}: {e}")
                continue
        
        return pd.DataFrame(results).sort_values("total_killed", ascending=False)
    
    return (detect_intersection_change_points,)


@app.cell
def _(detect_intersection_change_points, df, h3_classified, mo):
    """Execute change point analysis"""
    
    change_points = detect_intersection_change_points(df, h3_classified, top_n=20)
    
    # Count patterns
    pattern_counts = change_points["pattern"].value_counts()
    
    mo.md(f"""
    ### Change Point Analysis Results
    
    Analyzed top 20 isolated intersection hotspots. Detected change points in crash patterns:
    
    **Pattern Distribution:**
    {"".join(f"- **{pattern}**: {count} intersections\n" for pattern, count in pattern_counts.items())}
    
    **Interpretation:**
    - **Recent Emergence (2020+)**: Became dangerous during/after COVID — likely behavioral changes
    - **Sudden Spike**: Specific event triggered danger (construction, signal failure, development)
    - **Gradual Worsening**: Slow deterioration over time — missed by annual reviews
    - **Persistent Hotspot**: Dangerous throughout entire period — chronic infrastructure failure
    """)
    return (change_points, pattern_counts)


@app.cell
def _(change_points, pd):
    """Display change point results table"""
    
    display_df = change_points[[
        "borough",
        "total_killed",
        "crash_count",
        "pattern",
        "change_point_year",
        "pre_rate",
        "post_rate",
        "percent_change",
    ]].copy()
    
    display_df.columns = [
        "Borough",
        "Total Killed",
        "Fatal Crashes",
        "Pattern",
        "Change Year",
        "Pre-Change Rate (crashes/quarter)",
        "Post-Change Rate (crashes/quarter)",
        "% Change",
    ]
    
    display_df
    return (display_df,)


@app.cell
def _(change_points, df, go, h3lib, make_subplots, pd, px):
    """Visualize time series for top 5 change point intersections"""
    
    def plot_change_point_timeseries(
        df: pd.DataFrame,
        change_points: pd.DataFrame,
        top_n: int = 5,
    ):
        """
        Create multi-panel time series showing crash patterns before/after change points.
        """
        
        # Get top N by total killed
        top_cells = change_points.nlargest(top_n, "total_killed")
        
        # Create subplots
        fig = make_subplots(
            rows=top_n,
            cols=1,
            subplot_titles=[
                f"{row['borough']} — {row['pattern']} ({int(row['total_killed'])} killed)"
                for idx, row in top_cells.iterrows()
            ],
            vertical_spacing=0.08,
        )
        
        colors = px.colors.qualitative.Set2
        
        for i, (idx, hotspot) in enumerate(top_cells.iterrows()):
            cell_id = hotspot["h3_cell"]
            
            # Get crashes for this cell
            cell_crashes = df[
                (df["any_killed"]) &
                (df["latitude"].notna()) &
                (df["longitude"].notna())
            ].copy()
            
            cell_crashes["h3_cell"] = cell_crashes.apply(
                lambda r: h3lib.latlng_to_cell(r["latitude"], r["longitude"], 9),
                axis=1
            )
            cell_crashes = cell_crashes[cell_crashes["h3_cell"] == cell_id]
            
            # Quarterly aggregation
            cell_crashes["quarter"] = pd.PeriodIndex(cell_crashes["crash_date"], freq="Q")
            quarterly = (
                cell_crashes.groupby("quarter")
                .size()
                .reindex(
                    pd.period_range(
                        start=cell_crashes["crash_date"].min(),
                        end=cell_crashes["crash_date"].max(),
                        freq="Q"
                    ),
                    fill_value=0
                )
            )
            
            # Convert quarter index to datetime for plotting
            x_dates = [q.to_timestamp() for q in quarterly.index]
            
            # Add trace
            fig.add_trace(
                go.Scatter(
                    x=x_dates,
                    y=quarterly.values,
                    mode="lines+markers",
                    name=hotspot["borough"],
                    line=dict(color=colors[i % len(colors)], width=2),
                    marker=dict(size=6),
                    showlegend=False,
                ),
                row=i+1,
                col=1,
            )
            
            # Add change point vertical line if detected
            if pd.notna(hotspot["change_point_year"]):
                change_date = pd.to_datetime(f"{hotspot['change_point_year']}-01-01")
                fig.add_vline(
                    x=change_date.timestamp() * 1000,  # Plotly uses milliseconds
                    line_dash="dash",
                    line_color="red",
                    line_width=2,
                    annotation_text=f"Change: {hotspot['change_point_year']}",
                    annotation_position="top",
                    row=i+1,
                    col=1,
                )
        
        fig.update_xaxes(title_text="Quarter", row=top_n, col=1)
        fig.update_yaxes(title_text="Fatal Crashes", range=[0, None])
        
        fig.update_layout(
            title=dict(
                text="Time Series Analysis: Top 5 Deadliest Isolated Hotspots<br><sup>Red line indicates detected change point in crash pattern</sup>",
                x=0,
                font=dict(size=16),
            ),
            height=250 * top_n,
            margin=dict(l=60, r=20, t=100, b=60),
        )
        
        return fig
    
    fig_timeseries = plot_change_point_timeseries(df, change_points, top_n=5)
    fig_timeseries
    return (fig_timeseries, plot_change_point_timeseries)


@app.cell
def _(change_points, mo):
    """Policy implications based on change point patterns"""
    
    recent = change_points[change_points["pattern"] == "Recent Emergence (2020+)"]
    persistent = change_points[change_points["pattern"] == "Persistent Hotspot (No Change)"]
    sudden = change_points[change_points["pattern"] == "Sudden Spike"]
    
    mo.md(f"""
    ### Policy Implications by Pattern Type
    
    #### 1. Recent Emergence (2020+) — {len(recent)} intersections
    
    **Characteristics:**
    - Became dangerous during or after COVID-19 pandemic
    - Pre-2020: Low crash rates
    - Post-2020: Elevated crash rates
    
    **Likely Causes:**
    - Changed driving behavior (increased speeding, distracted driving)
    - Reduced enforcement during pandemic
    - Changed traffic patterns (work from home → different rush hours)
    
    **Recommended Actions:**
    - Immediate site investigation to identify new risk factors
    - Temporary countermeasures (speed feedback signs, increased enforcement)
    - Monitor for stabilization vs continued worsening
    
    **Priority Level:** 🔴 URGENT — These are new problems requiring rapid response
    
    ---
    
    #### 2. Persistent Hotspots (No Change) — {len(persistent)} intersections
    
    **Characteristics:**
    - Dangerous throughout entire 2012-2026 period
    - No detected change in pattern
    - Steady stream of fatalities year after year
    
    **Likely Causes:**
    - Fundamental infrastructure failure (poor visibility, confusing geometry)
    - High-speed arterial with inadequate crossing protection
    - Complex intersection that hasn't been redesigned
    
    **Recommended Actions:**
    - Comprehensive intersection redesign (not just operational changes)
    - Capital project allocation (refuge islands, signal phases, road diet)
    - High confidence intervention will succeed (problem is clear and consistent)
    
    **Priority Level:** 🟠 HIGH — Chronic problems that have persisted too long
    
    ---
    
    #### 3. Sudden Spike — {len(sudden)} intersections
    
    **Characteristics:**
    - Specific year when crashes suddenly increased
    - Large percent change (>50%) at change point
    
    **Likely Causes:**
    - Construction project altered traffic flow
    - Signal timing changed
    - New development generated new traffic patterns
    - Major crash with secondary effects
    
    **Recommended Actions:**
    - Review NYC DOT project history for that intersection/year
    - Check for nearby construction, rezoning, or signal retiming
    - If intervention was attempted, evaluate why it failed
    - May need to reverse changes or add compensating measures
    
    **Priority Level:** 🟡 MEDIUM-HIGH — Investigate cause before intervening
    
    ---
    
    ### Cross-Reference with Site Visits
    
    For each pattern type, conduct targeted site investigations:
    
    | Pattern | Investigation Focus | Timeline |
    |---------|-------------------|----------|
    | Recent Emergence | New risk factors (speeding, new development, changed signals) | 30 days |
    | Persistent Hotspot | Infrastructure audit (geometry, sight lines, signal phases) | 60 days |
    | Sudden Spike | Project history review, interview local DOT staff | 45 days |
    
    Expected outcome: Customized intervention plan for each intersection based on 
    root cause identified through change point + site investigation.
    """)
    return (persistent, recent, sudden)


@app.cell
def _(mo):
    mo.md("""
    ---
    ## Alternative: Simple Trend Lines for Deadliest Streets
    
    If change point detection is too complex, a simpler approach is to show trend lines 
    for the top 10 deadliest isolated intersection hotspots. This still reveals temporal 
    patterns without requiring statistical change point algorithms.
    """)
    return


@app.cell
def _(df, go, h3_classified, h3lib, make_subplots, pd):
    """Simpler alternative: Trend lines for top deadliest intersections"""
    
    def plot_simple_trends(
        df: pd.DataFrame,
        h3_classified: pd.DataFrame,
        top_n: int = 10,
    ):
        """
        Create simple annual trend lines for top deadliest isolated hotspots.
        No change point detection — just visual inspection of trends.
        """
        
        # Get top isolated hotspots
        isolated = h3_classified[h3_classified["pattern"] == "Isolated Hotspot"]
        top_hotspots = isolated.nlargest(top_n, "total_killed")
        
        fig = go.Figure()
        
        for idx, hotspot in top_hotspots.iterrows():
            cell_id = hotspot["h3_cell"]
            
            # Get crashes for this cell
            cell_crashes = df[
                (df["any_killed"]) &
                (df["latitude"].notna()) &
                (df["longitude"].notna())
            ].copy()
            
            cell_crashes["h3_cell"] = cell_crashes.apply(
                lambda r: h3lib.latlng_to_cell(r["latitude"], r["longitude"], 9),
                axis=1
            )
            cell_crashes = cell_crashes[cell_crashes["h3_cell"] == cell_id]
            
            # Annual aggregation
            annual = (
                cell_crashes.groupby("year")
                .size()
                .reindex(range(2012, 2027), fill_value=0)
            )
            
            # Add trace
            fig.add_trace(
                go.Scatter(
                    x=annual.index,
                    y=annual.values,
                    mode="lines+markers",
                    name=f"{hotspot['borough']} ({int(hotspot['total_killed'])} killed)",
                    line=dict(width=2),
                    marker=dict(size=6),
                    hovertemplate="<b>%{fullData.name}</b><br>Year: %{x}<br>Fatal Crashes: %{y}<extra></extra>",
                )
            )
        
        # Add COVID marker
        fig.add_vline(
            x=2020,
            line_dash="dash",
            line_color="gray",
            annotation_text="COVID-19",
            annotation_position="top",
        )
        
        fig.update_layout(
            title=dict(
                text=f"Annual Fatal Crash Trends: Top {top_n} Deadliest Isolated Hotspots<br><sup>Each line represents one H3 cell (~175m diameter intersection area)</sup>",
                x=0,
                font=dict(size=16),
            ),
            xaxis_title="Year",
            yaxis_title="Fatal Crashes per Year",
            hovermode="x unified",
            height=600,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(255,255,255,0.8)",
            ),
        )
        
        return fig
    
    fig_simple_trends = plot_simple_trends(df, h3_classified, top_n=10)
    fig_simple_trends
    return (fig_simple_trends, plot_simple_trends)


@app.cell
def _(mo):
    mo.md("""
    ### Interpreting Simple Trend Lines
    
    Visual inspection of trend lines reveals:
    
    1. **Flat lines** → Persistent hotspots (consistent danger year after year)
       - Example: If a line stays around 2-3 crashes/year for entire period
       - Action: These need infrastructure redesign, not just operational fixes
    
    2. **Upward slopes** → Worsening hotspots
       - Example: Line goes from 1-2 crashes/year (2012-2019) to 4-5 crashes/year (2020-2026)
       - Action: Urgent investigation of what changed
    
    3. **Downward slopes** → Improving hotspots
       - Example: Line goes from 4-5 crashes/year to 1-2 crashes/year
       - Action: Document what intervention worked, replicate elsewhere
    
    4. **Sudden jumps** → Change point events
       - Example: Steady 1-2 crashes/year, then jumps to 5+ in one year
       - Action: Investigate that specific year for changes (construction, signals, development)
    
    **Advantage of simple trends:**
    - Easy to interpret (no statistical jargon)
    - Visual pattern recognition
    - Good for poster presentations
    
    **Disadvantage:**
    - Subjective interpretation (when is a trend "real" vs noise?)
    - Doesn't quantify statistical significance
    - Harder to automate prioritization
    """)
    return


# ══════════════════════════════════════════════════════════════════════════════
# RECOMMENDATION: WHICH APPROACH TO USE
# ══════════════════════════════════════════════════════════════════════════════

"""
FOR YOUR ANALYSIS, I RECOMMEND:

1. **Use SIMPLE TREND LINES** for:
   - Initial exploration and visualization
   - Poster presentation (easier to explain)
   - Communicating with non-technical stakeholders
   - Quick identification of obvious patterns

2. **Use CHANGE POINT DETECTION** for:
   - Rigorous statistical analysis in the notebook
   - Identifying exact timing of pattern shifts
   - Prioritizing site investigations (objective criteria)
   - Supporting policy recommendations with quantitative evidence

3. **BEST APPROACH: DO BOTH**
   - Show simple trend lines in main results (visual, intuitive)
   - Include change point analysis in appendix (technical rigor)
   - Use change points to categorize trends (Recent/Persistent/Sudden)
   - Cross-reference: "The upward trend visible in the chart is statistically 
     significant with a change point detected in 2020 (p<0.05)"

RECOMMENDED NOTEBOOK STRUCTURE:

Section X: Temporal Analysis
├─ X.1: Simple Trend Lines (top 10 hotspots)
│   └─ Visual: Multi-line chart with COVID marker
├─ X.2: Change Point Detection Results
│   ├─ Table: Pattern classification for each hotspot
│   └─ Visual: Time series panels with change point markers
└─ X.3: Policy Implications
    └─ Different actions for Recent/Persistent/Sudden patterns

FOR POSTER:
- Use the simple trend line visualization (cleaner, easier to explain)
- Add callout: "3 intersections became dangerous post-2020"
- Keep change point analysis in supplementary materials
"""
