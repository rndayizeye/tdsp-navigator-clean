# Kedro Data Loading: Challenges & Lessons Learned

This document outlines the specific technical hurdles encountered during the refactoring of the `tdsp-navigator` ingestion pipeline and the architectural decisions made to resolve them.

---

## 1. Technical Challenges

### **A. API Integration & Parametrization**
* **The Issue:** Initial node failures occurred due to a mismatch between `parameters.yml` and the node's function signature.
* **Lesson:** Helper functions (like `_fetch_from_socrata`) must be designed to handle optional arguments or use `**kwargs` to prevent `TypeError` when configuration files are updated with new keys (e.g., `app_token`).
* **Fix:** Explicitly map YAML keys to function arguments in the node definition to ensure transparency in data flow.

### **B. Incremental Loading Logic (Watermarking)**
* **The Issue:** Ensuring the pipeline didn't re-fetch the entire NYC crash dataset (millions of rows) every time.
* **Lesson:** Using a `JSONDataset` as a "metadata" tracker is essential for scalable surveillance. 
* **Workflow:**
    1. Load `metadata_raw.json` (the watermark).
    2. Fetch records WHERE `crash_date > watermark`.
    3. Update the watermark with the `max(crash_date)` from the new data.
    4. Save updated metadata back to disk.

### **C. Schema Rigidity vs. API Flexibility**
* **The Issue:** Field names in public datasets often change or contain typos (e.g., the conflict between `on_street_name` and `off_street_name`).
* **Lesson:** Data Scientists must treat the "Raw" layer as volatile.
* **Fix:** Use the `01_raw` to `02_intermediate` transition to strictly enforce schemas (renaming columns and casting types) rather than trying to fix it at the ingestion node level.

---

## 2. Infrastructure & Environment Lessons

### **A. The "Ghost" Dependency Conflict**
* **The Issue:** The `secure` and `pydantic` packages caused the Kedro internal framework to fail with an `AttributeError`.
* **Lesson:** Modern web frameworks (FastAPI/Uvicorn) and legacy pipeline tools (Kedro 0.19.x) have fundamental versioning conflicts. 
* **Resolution:** Strategic pinning of versions (e.g., `pydantic<2.0`) is a temporary fix, but **Environment Isolation** is the only long-term scalable solution.

### **B. Tool-Specific Environments**
* **Lesson:** Don't force a single environment to do everything.
* **Outcome:** * `tdsp_env`: Optimized for stability and pipeline execution.
    * `marimo_env`: Optimized for speed, reactivity, and modern Python features.

---

## 3. Best Practices for Future Projects

1.  **Catalog-First Design:** Define every dataset in `catalog.yml` before writing the node. This enforces discipline regarding where data lives.
2.  **Verbose Logging:** When Socrata or other APIs fail, the error is often hidden. Using `logger.info` to print the watermark and API domain during the run saved hours of debugging.
3.  **Docker Readiness:** Because the environment is so specific (pinned to Kedro 0.19.15 and Pydantic 1.x), the next step should be creating a `Dockerfile` to "freeze" this state for deployment to Hugging Face or other servers.

---
*Document prepared for TDSP Navigator Project - April 2026*
