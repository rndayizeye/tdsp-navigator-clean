# TDSP Navigator - Kedro Environment Stabilization & Troubleshooting Guide

This document serves as a post-mortem and configuration guide for the `tdsp-navigator` project after the environment stabilization on April 21, 2026. Use these protocols to avoid dependency conflicts (specifically between legacy Kedro and modern analysis tools).

---

## 1. The Core Environment Strategy: Dual-Environment Isolation

The primary pitfall encountered was **"Dependency Hell"** caused by trying to run legacy Kedro components (0.19.x) and modern reactive notebooks (Marimo) in the same space.

### **Environment A: `tdsp_env` (Production & Pipeline)**
* **Purpose:** Core Kedro pipelines, data ingestion from Socrata, and geospatial joins.
* **Kedro Version:** `0.19.15` (LTS branch).
* **Status:** Stable.
* **Critical Dependencies (Pinned):**
    * `kedro-viz==6.7.0`
    * `secure==0.3.0` (Must be pinned to 0.3.0 to avoid `'Secure' object has no attribute 'framework'` error).
    * `pydantic<2.0` (Legacy engine required by Kedro-Viz 6.x).
    * `starlette<0.28.0`
    * `fastapi<0.100.0`
    * `strawberry-graphql<0.235.0`

### **Environment B: `marimo_env` (Analysis & EDA)**
* **Purpose:** Exploratory Data Analysis, Marimo notebooks, and reactive visualizations.
* **Marimo Version:** Latest.
* **Status:** Modern.
* **Advantages:** Uses Pydantic 2.x and Starlette 0.37+, allowing for faster execution and modern Python features without breaking the Kedro UI.

---

## 2. Troubleshooting Registry: Pitfalls & Fixes

| Issue | Root Cause | Permanent Fix |
| :--- | :--- | :--- |
| `TypeError: _fetch_from_socrata() got unexpected keyword argument 'app_token'` | Function signature mismatch in `nodes.py`. | Update `_fetch_from_socrata` in `pipelines/data_ingestion/nodes.py` to accept `app_token=None`. |
| `AttributeError: 'Secure' object has no attribute 'framework'` | Package shadowing; newer versions of `secure` conflict with Kedro's internal resolution. | `pip uninstall secure` followed by `pip install secure==0.3.0`. |
| `TypeError: GraphQL.__init__() got unexpected keyword argument 'graphiql'` | Version mismatch between Kedro-Viz and Strawberry-GraphQL. | `pip install "strawberry-graphql<0.235.0"`. |
| `ModuleNotFoundError: No module named 'toposort'` | Shallow install of Kedro-Viz. | `pip install toposort`. |

---

## 3. Workflow Protocol

### **Daily Data Ingestion**
1.  Activate the pipeline environment: `conda activate tdsp_env`.
2.  Run the ingestion: `kedro run`.
3.  Verify the "Watermark": Check `data/01_raw/metadata_raw.json` to ensure the timestamp updated.
4.  Verify the Data: `ls -lh data/01_raw/nyc_crashes_raw.csv`.

### **Analysis & Notebooks**
1.  Activate the analysis environment: `conda activate marimo_env`.
2.  Launch Marimo: `marimo edit analysis_notebook.py`.
3.  **Local Saving:** Marimo saves directly to the `.py` file. Ensure you use `Cmd+S` or verify the "Saved" icon in the UI.

---

## 4. Maintenance Warning
**DO NOT** run `pip install --upgrade` or `pip install <package>` without checking Pydantic versions in `tdsp_env`. If you accidentally upgrade Pydantic to 2.x in the Kedro environment, the visualizer will break immediately.

*Document generated on 2026-04-21.*
