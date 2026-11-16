
# Odoo Sales & Risk Analytics Platform
## Complete Project Architecture

---

## 1. Architecture Objective

The architecture separates ingestion, data quality, conformance, analytics, presentation, and AI responsibilities. Every layer has a clear contract and persists enough lineage and audit information to support replay, troubleshooting, and certification.

---

## 2. Logical Architecture

```text
┌───────────────────────────────────────────────────────────────┐
│ Odoo Source Exports                                           │
│ Contacts JSONL + API CSV batches                              │
└──────────────────────────────┬────────────────────────────────┘
                               │
                               ▼
┌───────────────────────────────────────────────────────────────┐
│ Landing                                                       │
│ Immutable source files organized by family, batch, dataset    │
└──────────────────────────────┬────────────────────────────────┘
                               │ Papermill + Spark
                               ▼
┌───────────────────────────────────────────────────────────────┐
│ Bronze                                                        │
│ Accepted source-preserving CSV + corrupt CSV + file controls  │
└──────────────────────────────┬────────────────────────────────┘
                               │ Structured Streaming
                               ▼
┌───────────────────────────────────────────────────────────────┐
│ Silver                                                        │
│ Normalized latest valid Iceberg tables + quarantine           │
└──────────────────────────────┬────────────────────────────────┘
                               │ deterministic merges
                               ▼
┌───────────────────────────────────────────────────────────────┐
│ Gold                                                          │
│ Conformed dimensions + Customer SCD2 + Order facts            │
└──────────────────────────────┬────────────────────────────────┘
                               │ deterministic derivation
                               ▼
┌───────────────────────────────────────────────────────────────┐
│ Aggregation                                                   │
│ Period metrics + intelligence + risk + concentration + alerts │
└───────────────────────┬───────────────────────────────────────┘
                        │
             ┌──────────┴───────────┐
             ▼                      ▼
┌────────────────────────────┐  ┌───────────────────────────────┐
│ Streamlit KPI Dashboard    │  │ AI Analytics Assistant        │
│ DuckDB + Plotly            │  │ Guarded SQL + RAG + Vertex AI │
└────────────────────────────┘  └───────────────────────────────┘
```

Apache Airflow orchestrates the processing layers from Landing through Aggregation.

---

## 3. Technology Architecture

| Capability | Technology | Role |
|---|---|---|
| Source ingestion | Python, PySpark | Parse CSV and JSONL with explicit schemas |
| Notebook execution | Papermill, Jupyter kernels | Parameterized repeatable pipeline execution |
| Distributed processing | Apache Spark 3.5 | Transformations, streaming, joins, validation, aggregation |
| Data lake storage | HDFS | Landing, Bronze, checkpoints, Iceberg warehouse |
| Analytical table format | Apache Iceberg v2 | ACID tables, MERGE, metadata, replay-safe processing |
| Data quality | Spark expressions, Great Expectations | Row routing and batch-level evidence |
| Orchestration | Apache Airflow 3 | Parent-child DAG scheduling and dependencies |
| Dashboard query engine | DuckDB | Read-only analytical SQL over Iceberg tables |
| HDFS access from dashboard | WebHDFS and fsspec | Exposes HDFS files to DuckDB |
| Dashboard | Streamlit | Interactive business application |
| Visualizations | Plotly | KPI charts and drill-down visuals |
| AI model | Vertex AI Gemini | Routing, SQL generation, and grounded response wording |
| SQL security | sqlglot | Parses and validates generated SQL |
| Documentation retrieval | Local BM25-style RAG | Retrieves KPI definitions and project documentation |

---

## 4. Physical Storage Architecture

### 4.1 HDFS roots

```text
hdfs://localhost:9000/capstone/
├── bronze/
│   ├── data/
│   └── corrupt_records/
├── checkpoints/
│   └── bronze_to_silver/
└── warehouse/
    ├── silver/
    ├── quarantine/
    ├── gold/
    ├── aggregation/
    └── operations/
```

### 4.2 Bronze file storage

Bronze is CSV stored directly in HDFS and partitioned by dataset, batch, file ID, year, and month.

### 4.3 Iceberg warehouse

Silver, quarantine, Gold, Aggregation, and Operations tables use the Hadoop Iceberg catalog:

```text
Catalog: datalake
Warehouse: hdfs://localhost:9000/capstone/warehouse
```

Each Iceberg table contains:

```text
data/*.parquet
metadata/*.metadata.json
manifest and snapshot metadata
```

---

## 5. Data Namespace Architecture

| Namespace | Contents |
|---|---|
| `datalake.silver` | Clean latest source-level records |
| `datalake.quarantine` | Rejected Silver records |
| `datalake.gold` | Dimensions and facts |
| `datalake.aggregation` | Dashboard-ready analytical tables |
| `datalake.operations` | Audits, controls, quality, exploration, and reconciliation |

Operational tables centralize traceability across all layers.

---

## 6. Processing Component Architecture

Each pipeline uses the same separation:

```text
Airflow task
→ Bash runner
→ Papermill
→ Notebook
→ Spark session
→ HDFS/Iceberg
```

### Airflow

Controls dependency and parameter propagation.

### Bash runner

Resolves paths and environment values, creates output folders, constructs the complete Papermill command, and returns the notebook exit status.

### Papermill

Injects runtime parameters and creates an executed notebook artifact.

### Notebook

Contains the transformation, validation, reconciliation, and audit logic.

### Spark

Performs distributed file parsing, normalization, joins, MERGE operations, and aggregation.

---

## 7. Bronze Architecture

```text
Landing file
→ deterministic file ID
→ processed_files lookup
→ schema profile
→ explicit-schema parse
→ accepted/corrupt split
→ reconciliation
→ HDFS write
→ control event
```

Key characteristics:

- File-level idempotency
- Source-preserving accepted records
- Corrupt routing
- Schema drift observation
- Complete lineage
- One Spark session per Papermill file execution

Key operational tables:

```text
processed_files
schema_drift_profiles
corrupt_records
```

---

## 8. Silver Architecture

```text
Bronze dataset root
→ Structured Streaming availableNow
→ persistent checkpoint
→ foreachBatch
→ profile
→ normalize
→ validate
→ quarantine rejected
→ deduplicate valid
→ staging table
→ Iceberg MERGE
→ reconcile
→ audit
```

Key characteristics:

- Incremental by physical file checkpoint
- Latest-valid-record semantics
- Batch and row-level quality evidence
- Quarantine for invalid records
- Idempotent Iceberg upsert

Key operational tables:

```text
explorations
expectation_profiles
silver_ingestion_audit
```

---

## 9. Gold Dimensional Architecture

```text
                    ┌──────────────┐
                    │ dim_date     │
                    └──────┬───────┘
                           │
┌─────────────┐     ┌──────▼───────────┐     ┌──────────────────┐
│ dimensions  │────▶│ fact_sales_order │────▶│ fact_order_line  │
└──────┬──────┘     └──────┬───────────┘     └────────┬─────────┘
       │                   │                          │
       ▼                   ▼                          ▼
 dim_customer SCD2   Customer temporal keys      Product/category keys
```

### Dimensions

```text
dim_date
dim_company
dim_sales_team
dim_salesperson
dim_region
dim_product_category
dim_product
dim_customer
```

### Facts

```text
fact_sales_order
fact_sales_order_line
```

### Key architecture decisions

- SHA-256 deterministic surrogate keys
- Controlled Unknown members
- Customer SCD Type 2 history
- Temporal Customer resolution in Orders
- Parent Order dependency for Order Lines
- Record-hash idempotency
- Iceberg MERGE
- Rejected fact retention

---

## 10. Aggregation Architecture

```text
Gold Orders and Lines
│
├── revenue_summary
├── customer_period_metrics
│   └── customer_intelligence
│       └── customer_risk
├── product_period_metrics
│   └── product_intelligence
├── order_fulfilment_summary
├── sales_team_performance
├── salesperson_performance
├── revenue_concentration
└── business_insights
    ↓
Aggregation Quality Gate
```

Aggregation tables use stable grains and `DECIMAL` outputs. Order Lines are rolled to Order grain before joining to Order-header revenue, which prevents revenue multiplication.

Derived tables use Iceberg replacement because their outputs are completely reproducible from Gold.

---

## 11. Quality Architecture

Quality is enforced at every layer.

| Layer | Main controls |
|---|---|
| Bronze | Explicit schema, corrupt routing, source reconciliation, file control |
| Silver | Great Expectations, Spark row rules, quarantine, dedup, microbatch reconciliation |
| Gold | Key uniqueness, Unknown members, SCD2 invariants, referential checks, fact reconciliation |
| Aggregation | Grain uniqueness, ratio boundaries, counts, revenue reconciliation |
| Dashboard | Latest Aggregation Quality Gate banner |
| AI | Certified-view boundary, guarded SQL, evidence display |

### Quality gate behavior

Hard failures stop the dependent pipeline. Known source limitations are warnings. Dashboard certification requires zero hard failures.

---

## 12. Audit and Lineage Architecture

Lineage is preserved through:

```text
source file
→ batch ID
→ file ID
→ Bronze run ID
→ Silver pipeline run ID
→ Gold run ID
→ Aggregation run ID
→ dashboard refresh and quality run IDs
```

Major audit tables include:

```text
processed_files
silver_ingestion_audit
gold_processing_audit
gold_referential_quality
gold_rejected_records
gold_order_line_reconciliation
aggregation_processing_audit
aggregation_quality_gate
```

This makes it possible to trace dashboard results back to ingestion and quality events.

---

## 13. Airflow Architecture

```text
enterprise_revenue_parent_pipeline
│
├── dataset child DAGs
│   ├── Landing-to-Bronze
│   ├── Bronze-to-Silver
│   └── Silver validation
│
├── gold_pipeline
│   ├── dimensions
│   ├── facts
│   └── Gold Quality Gate
│
└── aggregation_pipeline
    ├── 11 analytical tables
    └── Aggregation Quality Gate
```

### Parameter boundary

The parent keeps typed native values. Child BashOperator environments render strings. Bash runners pass Papermill parameters into notebooks.

### Concurrency

The implementation is designed for one physical system. Independent dataset children can run in controlled two-pipeline waves using `local[2]`. Gold and Aggregation remain ordered according to dependencies. `max_active_runs=1` prevents overlapping end-to-end runs.

---

## 14. Dashboard Architecture

```text
Streamlit page
→ cached query function
→ DuckDB connection
→ registered Iceberg views
→ custom fsspec WebHDFS adapter
→ NameNode WebHDFS :9870
→ HDFS Iceberg files
```

### Dashboard runtime modules

```text
app.py
queries.py
charts.py
formatting.py
theme.py
config.py
iceberg_access.py
run_dashboard.sh
```

### Dashboard environment

The dashboard uses a separate Python environment:

```text
/home/sumith/odoo-dashboard-env
```

This isolates Streamlit, DuckDB, Google Gen AI, and `sqlglot` from Airflow and Spark dependencies.

---

## 15. DuckDB and Iceberg Access Architecture

The connector:

1. Uses WebHDFS through `fsspec`.
2. Lists the Iceberg metadata directory.
3. Selects the latest valid `.metadata.json` file.
4. Registers a custom HDFS filesystem with DuckDB.
5. Loads DuckDB's Iceberg extension.
6. Creates read-only DuckDB views using `iceberg_scan`.
7. Normalizes relative Iceberg manifest paths for the local Hadoop catalog layout.
8. Closes the connection after each query scope.

Only the application connector invokes `iceberg_scan`. AI-generated SQL is prohibited from invoking file or Iceberg scan functions directly.

---

## 16. Dashboard Page Architecture

| Page | Primary certified tables |
|---|---|
| Executive Revenue Overview | `revenue_summary`, `customer_period_metrics`, `revenue_concentration` |
| Customer Intelligence | `customer_period_metrics`, `customer_intelligence`, `customer_risk` |
| Product Intelligence | `product_period_metrics`, `product_intelligence` |
| Sales and Fulfilment | `order_fulfilment_summary`, `sales_team_performance`, `salesperson_performance` |
| Risk and Insights | `customer_risk`, `revenue_concentration`, `business_insights` |
| AI Analytics Assistant | Approved subsets of all certified analytical and quality views |

---

## 17. AI Architecture

```text
Streamlit chat
→ static scope guard
→ Vertex AI route classification
→ DATA / KNOWLEDGE / HYBRID / OUT_OF_SCOPE
```

### Data route

```text
Question
→ approved analytical area
→ live schema discovery
→ Gemini SQL generation
→ sqlglot validation
→ DuckDB EXPLAIN
→ DuckDB execution
→ result limitation
→ Gemini grounded answer
```

### Knowledge route

```text
Question
→ KPI synonym normalization
→ local BM25 retrieval
→ top Markdown passages
→ Gemini documentation-grounded answer
```

### Hybrid route

```text
Documentation evidence
+
Guarded live SQL result
→ one combined answer
```

### AI data boundary

The model never receives complete tables. It receives schemas, reporting context, selected documentation passages, and at most 20 result rows.

---

## 18. Security Architecture

### Data protection

- Read-only certified views
- No write operations
- No local file or URL access
- No arbitrary HDFS scanning
- No Bronze, Silver, or Gold access
- Area-specific table allow-lists
- Maximum row and payload limits

### SQL protection

- `sqlglot` AST parsing
- One statement
- `SELECT` only
- No `SELECT *`
- Blocked administrative commands
- Blocked scan functions
- `EXPLAIN` before execution

### Authentication

Vertex AI uses Google Application Default Credentials. Tokens and private keys are not stored in source files.

### SSL

The sandbox temporarily allows `AI_VERIFY_SSL=false`. Production must use the corporate CA bundle and restore certificate verification.

---

## 19. Deployment Architecture

Single-host development deployment:

```text
WSL Ubuntu host
├── HDFS NameNode and DataNode
├── Spark runtime
├── Airflow scheduler and API/web components
├── Papermill workers
├── Streamlit dashboard
├── DuckDB process
└── Vertex AI network client
```

All services share one physical system, so concurrency is deliberately limited.

Future scale-out can separate:

- HDFS or object storage
- Spark master and worker nodes
- Airflow scheduler and workers
- Streamlit application host
- Enterprise identity and secrets management

---

## 20. Reliability Architecture

| Failure | Protection |
|---|---|
| Bronze rerun | File ID and processed-files status |
| Silver rerun | Structured Streaming checkpoint and deterministic MERGE |
| Gold rerun | Record hashes and deterministic MERGE |
| Aggregation rerun | Deterministic replacement and audit |
| Upstream error | `set -euo pipefail`, Airflow dependency failure |
| Quality failure | Gold or Aggregation Quality Gate stops downstream processing |
| Dashboard stale cache | Cache TTL and Refresh Data |
| HDFS stopped | WebHDFS connectivity check and clear failure |
| Invalid AI SQL | Parser and allow-list rejection |
| Vertex AI failure | Dashboard pages remain deterministic and available |

---

## 21. Performance Architecture

### Spark

- Explicit schemas avoid inference cost.
- Silver uses `max_files_per_trigger`.
- DataFrames are cached only within microbatch operations when reused.
- Stable grouping keys reduce accidental duplicate groups.
- Order Lines are aggregated before joining to Orders.

### Dashboard

- DuckDB reads only required views.
- Query functions are cached with a short TTL.
- Result sets are filtered and limited.
- A dedicated Python environment avoids dependency overhead and conflicts.

### AI

- Schemas are cached.
- Documentation retrieval is local.
- Only 20 result rows are sent to Gemini.
- Chat history is bounded.

---

## 22. Environment and Configuration Architecture

Important Spark and pipeline settings:

```text
PROJECT_ROOT
SPARK_MASTER
SHUFFLE_PARTITIONS
SPARK_LOG_LEVEL
PYTHON_LOG_LEVEL
ICEBERG_SPARK_RUNTIME_JAR
HDFS_DEFAULT_FS
STREAM_TRIGGER
MAX_FILES_PER_TRIGGER
REVENUE_TOLERANCE
AS_OF_DATE
```

Important dashboard settings:

```text
HDFS_USER
WEBHDFS_HOST
WEBHDFS_PORT
HDFS_NAMENODE
ICEBERG_WAREHOUSE_ROOT
DUCKDB_THREADS
DUCKDB_MEMORY_LIMIT
CACHE_TTL_SECONDS
```

Important AI settings:

```text
GOOGLE_CLOUD_PROJECT
GOOGLE_CLOUD_LOCATION
GEMINI_MODEL
AI_VERIFY_SSL
AI_MAX_RESULT_ROWS
AI_MAX_ROWS_TO_MODEL
AI_MAX_CHAT_MESSAGES
AI_MAX_QUESTION_LENGTH
AI_MAX_RESULT_JSON_CHARACTERS
```

Secrets are excluded from source control.

---

## 23. End-to-End Trust Chain

```text
Raw file identity
→ Bronze source reconciliation
→ Silver validation and quarantine
→ Gold conformed keys and fact reconciliation
→ Aggregation stable grains and revenue reconciliation
→ Quality Gate certification
→ Dashboard certified banner
→ AI certified-view boundary and evidence
```

The architecture does not rely on a single final validation. Trust is accumulated and recorded at every layer.

---

## 24. Architecture Strengths

- Clear separation of concerns
- Replay-safe and idempotent processing
- Strong file, row, key, fact, and KPI reconciliation
- Explainable Customer history and risk
- Certified dashboard data boundary
- Transparent AI evidence
- No AI write access
- Auditable deterministic business insights
- Single-host resource controls with a scale-out path

---

## 25. Architecture Limitations and Next Improvements

Current limitations:

- One physical host
- Local or limited Spark execution
- Temporary SSL verification workaround
- Current Customer risk is not historically versioned
- Sales Team and Salesperson names may require descriptive lookup enrichment
- No forecasting model
- Local lexical RAG rather than dense embeddings

Recommended next improvements:

1. Use Spark Standalone with managed worker resources after validating every runner.
2. Replace sandbox SSL bypass with the corporate CA bundle.
3. Move secrets and environment configuration to an approved secrets manager.
4. Add monitoring for Airflow, Spark, HDFS, Streamlit, and Vertex AI calls.
5. Add historical Customer risk snapshots if trend analysis is required.
6. Enrich Sales Team and Salesperson display names.
7. Add a certified forecasting layer before enabling predictions.
8. Add user authentication and authorization for multi-user deployment.

---

## 26. Final Architecture Summary

The project follows a layered lakehouse architecture:

```text
Odoo exports
→ source-preserving Bronze
→ validated latest-record Silver
→ conformed dimensional Gold
→ certified analytical Aggregation
→ interactive Streamlit KPIs
→ guarded SQL and documentation RAG assistant
```

HDFS and Apache Iceberg provide durable analytical storage. Spark and Papermill provide repeatable processing. Airflow provides dependency orchestration. DuckDB provides low-friction dashboard queries. Vertex AI adds language interaction while SQL guards, RAG grounding, and certified-view boundaries preserve trust.


