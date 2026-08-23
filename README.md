# 🏎️ End-to-End Formula 1 Data Lakehouse Pipeline (Incremental & Serverless)

An enterprise-grade, end-to-end Data Engineering pipeline built using **Azure Databricks**, **Delta Lake**, **Unity Catalog**, and **Lakeflow Jobs**. 
This project ingests, transforms, models, and analyzes Formula 1 race data (circuits, races, drivers, results, and sprints) following a modern **Medallion Architecture** (Bronze, Silver, Gold). </br>
The solution is designed around incremental processing, idempotent transformations, reusable PySpark functions, serverless compute, and automated orchestration.

---

## 🏗️ Architecture & Tech Stack

| Category | Tools |
|---|---|
| **Cloud Platform** | Azure (Storage Accounts, Containers, External Locations, Unity Catalog) |
| **Processing Engine** | Azure Databricks (Serverless Compute, Spark, Delta Lake) |
| **Programming** | Python, PySpark, SQL |
| **Orchestration** | Databricks Lakeflow Jobs (Master–Child pipelines with conditional branching & state management) |
| **Data Modeling** | Star Schema (Dimension & Fact tables) |
| **Data Governance** | Unity Catalog |
| **Analytics & Visualization** | Databricks SQL Warehouses & Databricks Dashboards |

---

## 📂 Project Structure & Workflow

```text
├── 01_environment_setup/       # Catalog, schema, volume configurations, and environment paths
├── 02_bronze/                  # Raw ingestion, schema enforcement, and metadata tracking
├── 03_silver/                  # Data quality, deduplication, standardization and incremental merges
├── 04_gold/                    # Dimensional modeling (Star Schema facts & dimensions)
├── 05_orchestration/           # Control-table logic, master-child workflows, and incremental processing
└── 06_analytics/               # Aggregated views for driver and constructor standings
```

---

## 🔄 Pipeline Design & Incremental Processing

To avoid full table re-loads and handle incoming data dynamically, the pipeline features a custom **Control Table & Incremental Batch Pattern**:

1. **Batch Identification** — A master orchestration notebook queries a control table to check for unprocessed batch IDs.
2. **State Management (In-Progress)** — If a new batch exists, its status is updated to `in_progress` inside the control table, and a boolean flag (`has_batch = true`) is triggered.
3. **Conditional Execution** — Lakeflow Jobs evaluates the branch condition:
   - `False` → Pipeline gracefully terminates and succeeds (no-op for empty runs).
   - `True` → Triggers child jobs executing the full **Bronze → Silver → Gold** pipeline.
4. **Completion** — Upon successful execution, control returns to the master pipeline, updating the batch status to `completed`.

---

## 🧱 Medallion Architecture Layers

### 1. Bronze Layer (Raw Ingestion)
- Ingests raw data files of multiple formats from landing volumes.
- **Enforces schema on write** using explicit PySpark `StructType` definitions with `FAILFAST` read mode — malformed or mismatched records fail the job immediately instead of silently corrupting downstream tables.
- Preserves source-level information.
- Appends metadata columns including ingestion timestamp and source file path.
- Implements optimized partition overwrites using `batch_id` via Delta Lake:

```python
(
  final_df.write.format('delta') 
      .mode('overwrite') 
      .partitionBy('batch_id') 
      .option('replaceWhere', f"batch_id = '{new_batch_id}'") 
      .saveAsTable(target_table)
)
```

### 2. Silver Layer (Cleaned & Standardized)
- Drops unwanted columns (e.g., URLs) and standardizes formatting.
- Performs data quality checks: filters out null primary keys and handles duplicates.
- Implements idempotent upserts (merge) using reusable Python helper functions built on Delta Lake's `DeltaTable.merge()`, with dynamic column mapping and watermark-based update tracking (`batch_id >= target.batch_id`), allowing the pipeline to safely re-run without unnecessarily creating duplicate records.

### 3. Gold Layer (Dimensional Modeling)
- Implements a **Star Schema** optimized for analytical queries and reporting.
- **Dimensions:** Circuits, Drivers, Constructors
- **Facts:** Race Results
- Driven by reusable Python helper functions ensuring atomic, repeatable table updates.

---
## 🔐 Data Governance & Lakehouse Design

The project uses **Unity Catalog** to provide centralised governance across the Lakehouse environment. The environment includes:

- Catalogs
- Schemas
- Volumes
- External Locations
- Managed Delta tables

This provides a structured approach to organising and controlling access to data and resources across the different pipeline layers.

---

## 🔁 Lakeflow Job Orchestration

The pipeline uses **Databricks Lakeflow Jobs** to orchestrate the complete workflow. The orchestration follows a `Master → Child` Job pattern.

Master Job is responsible for:

- Checking the control table.
- Identifying available batches.
- Updating batch state.
- Controlling conditional execution.
- Triggering child jobs.

Child Jobs are responsible for executing the processing layers:

- Bronze -> Silver -> Gold

This separation makes the workflow easier to maintain and allows individual components to be developed and executed independently.

---

## 📊 Analytics & Dashboards

The Gold layer feeds analytical views designed for Formula 1 reporting.

#### Driver Analytics

Examples include:

- Driver standings
- Race wins
- Points progression
- Driver performance trends
- Dominant drivers across seasons

#### Constructor Analytics

Examples include:

- Constructor standings
- Team performance
- Points progression
- Race performance
- Constructor dominance

These datasets are consumed through **Databricks SQL Warehouses** and visualized using **Databricks Dashboards**.

---

## 🚀 How to Run

1. Configure environment paths and Unity Catalog volumes using the setup scripts in `01_environment_setup/`.
2. Initialize the control table metadata.
3. Trigger the Master Lakeflow Job orchestrator to execute automated incremental loads.

---
## 🔁 Version Control

**GitHub** is used to manage the codebase, enabling version tracking and collaborative development. 
Feature branches are used for development, and changes are merged into the main branch after validation.

---
## 🔑 Key Highlights

- End-to-end infrastructure built from scratch on Azure (storage, containers, external locations, Unity Catalog).
- Strict schema enforcement and validation at ingestion (Bronze).
- Idempotent, incremental processing with full batch state tracking — not a one-off notebook run.
- Star schema modeling for analytics-ready Gold tables.
- Orchestration designed for both manual and automated (scheduled/event-based) triggers.

---
## 🏁 Outcome

This project demonstrates how a traditional batch-oriented data pipeline can be transformed into a **modern, scalable Lakehouse architecture** capable of handling incremental data ingestion, data quality, dimensional modelling, orchestration, and analytics.
</br>
The primary focus is not simply on moving data from source to destination, but on building a pipeline that is **reliable, incremental, reusable, governed, and production-oriented**.
