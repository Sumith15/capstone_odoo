# Odoo Sales & Risk Analytics Platform
## Complete End-to-End Project Flow

**Tagline:** Transforming Odoo Business Data into Actionable Intelligence.

---

## 1. Project Purpose

The project converts raw Odoo business exports into a governed analytics platform for revenue, Customer, Product, sales, fulfilment, risk, and data-quality analysis.

The complete flow is:

```text
Odoo CSV and JSONL exports
        ↓
Landing
        ↓
Bronze
        ↓
Silver
        ↓
Gold dimensions and facts
        ↓
Aggregation tables and quality certification
        ↓
Streamlit KPI dashboard
        ↓
Governed AI analytics assistant
```

Apache Airflow orchestrates the processing layers. Apache Spark and Papermill execute the notebooks. Apache Iceberg stores conformed and analytical tables in HDFS. DuckDB reads certified Iceberg tables for the dashboard. Vertex AI provides guarded natural-language analysis and documentation RAG.

---

## 2. Business Data Scope

The source contains eight datasets:

| Dataset | Source family | Format | Business purpose |
|---|---|---:|---|
| Companies | Contacts | JSONL | Companies, currency, country, and contact attributes |
| Sales Teams | Contacts | JSONL | Team membership, leadership, and Company attribution |
| Users | Contacts | JSONL | User and Salesperson attributes |
| Customers | Contacts | JSONL | Customer, geography, contact, ranking, and status attributes |
| Product Categories | API | CSV | Product Category hierarchy |
| Products | API | CSV | Product, pricing, UOM, Category, and current availability attributes |
| Orders | API | CSV | Sales Order header, Customer, sales, revenue, state, and operational fields |
| Order Lines | API | CSV | Product quantities, pricing, discounts, delivery, invoicing, and line revenue |

API batches use names such as `api-00001`. Contacts batches use names such as `contacts-00001`.

---

## 3. End-to-End Layer Responsibilities

| Layer | Main responsibility | Does not do |
|---|---|---|
| Landing | Holds raw source files | Parsing, validation, conformance |
| Bronze | Preserves accepted source records and parser-corrupt records with lineage | Business normalization, dimensions, KPIs |
| Silver | Produces clean, normalized, validated, deduplicated latest source records | Surrogate keys, SCD history, facts, KPIs |
| Gold | Builds conformed dimensions, Customer history, Orders, and Order Lines | Dashboard summaries and risk KPIs |
| Aggregation | Builds reusable period, lifetime, fulfilment, risk, concentration, and insight tables | Raw-source parsing |
| Dashboard | Presents certified KPIs and drill-down analysis | Rebuilding the data pipeline during user requests |
| AI Assistant | Answers governed data, KPI-definition, and hybrid questions | Writes, forecasts, unrelated general knowledge |

---

# Part I. Landing and Bronze

## 4. Landing Layer

Landing contains the original CSV and JSONL files arranged by source family, batch, and dataset. Landing is the source boundary and is not modified by downstream transformations.

Typical paths include:

```text
contacts-00011/contact-details/customers/
api-00011/sales/orders/
api-00011/sales/order-lines/
```

The Landing layer preserves the exact received source and supports batch replay.

---

## 5. Landing-to-Bronze Flow

Each dataset has one Papermill notebook and one Bash runner. One notebook execution processes one source file from one batch.

```text
Dataset Bash runner
    ↓
Discover batches or use an explicit batch range
    ↓
Find source file
    ↓
Create Papermill output folder
    ↓
Inject runtime parameters
    ↓
Start Spark session
    ↓
Generate deterministic file ID
    ↓
Check processed-files control table
    ↓
Profile schema drift
    ↓
Read with explicit schema
    ↓
Split accepted and corrupt records
    ↓
Reconcile source = accepted + corrupt
    ↓
Write HDFS Bronze outputs
    ↓
Write control and audit records
    ↓
Stop Spark and retain executed notebook and log
```

### 5.1 Batch modes

Automatic discovery:

```bash
run_orders_landing_to_bronze.sh
```

Explicit inclusive range:

```bash
run_orders_landing_to_bronze.sh 8 10
```

The explicit range controls which Landing batches Bronze checks. It does not control Silver consumption.

### 5.2 Explicit schemas

Bronze uses canonical schema modules for all eight datasets. Accepted Bronze rows append technical fields:

```text
_run_id
_file_id
_batch_id
_source_file
_ingestion_timestamp
```

Odoo relation values such as `[58,"Customer Name"]`, arrays, mixed false-or-string values, and malformed JSON are preserved or routed safely.

### 5.3 File identity and idempotency

A file ID is generated from:

```text
normalized source URI
+ file size
+ file modified time in milliseconds
```

The combined value is hashed with SHA-256. The control table is:

```text
datalake.operations.processed_files
```

Completed states are:

```text
SUCCESS
PARTIAL_SUCCESS
ALL_RECORDS_CORRUPT
```

A previously completed file version returns:

```text
SKIPPED_ALREADY_PROCESSED
```

Failed attempts remain retryable.

### 5.4 Schema and corrupt-record profiling

Schema drift is written to:

```text
datalake.operations.schema_drift_profiles
```

Parser-corrupt audit information is written to:

```text
datalake.operations.corrupt_records
```

Schema drift is observational. Parser-corrupt rows are separated from accepted Bronze rows.

### 5.5 Reconciliation

Every source file must satisfy:

```text
source_record_count
= accepted_record_count
+ corrupt_record_count
```

A mismatch fails the notebook and records a failed control event.

### 5.6 HDFS outputs

Accepted records:

```text
hdfs://localhost:9000/capstone/bronze/data/<dataset>/
  batch_id=<batch>/
  file_id=<sha256>/
  record_year=<YYYY>/
  record_month=<MM>/
```

Corrupt records:

```text
hdfs://localhost:9000/capstone/bronze/corrupt_records/<dataset>/
  batch_id=<batch>/
  file_id=<sha256>/
  rejection_year=<YYYY>/
  rejection_month=<MM>/
```

### 5.7 Papermill artifacts

```text
outputs/papermill/<dataset>/<batch-id>/
├── executed.ipynb
└── papermill.log
```

The executed notebook includes cell output and a machine-readable pipeline result marker.

---

# Part II. Silver

## 6. Bronze-to-Silver Purpose

Silver produces clean, normalized, validated, deduplicated latest source-level records. It consumes accepted Bronze CSV files incrementally through Spark Structured Streaming and writes Apache Iceberg tables.

Silver implements:

- `availableNow` Structured Streaming
- One persistent checkpoint per dataset
- Data exploration profiling
- Great Expectations batch profiling
- Odoo relation normalization
- Deterministic row validation
- Quarantine routing
- Latest-valid-record deduplication
- Iceberg MERGE
- Microbatch reconciliation
- Silver ingestion audits

---

## 7. Silver Flow

```text
Silver Bash runner
    ↓
Resolve HDFS default authority
    ↓
Invoke Silver Papermill notebook
    ↓
Create Spark and Iceberg catalog
    ↓
Read complete Bronze dataset root as a stream
    ↓
Checkpoint selects unprocessed physical files
    ↓
availableNow creates microbatches
    ↓
foreachBatch receives a static DataFrame
    ↓
Profile and normalize
    ↓
Apply deterministic row rules
    ↓
VALID or REJECTED
    ↓
Deduplicate valid records
    ↓
Stage and MERGE into Silver
    ↓
Append rejected records to quarantine
    ↓
Reconcile and audit
    ↓
Commit checkpoint
```

### 7.1 Silver and quarantine tables

| Dataset | Valid table | Rejected table |
|---|---|---|
| Companies | `datalake.silver.companies` | `datalake.quarantine.companies_rejected` |
| Sales Teams | `datalake.silver.sales_teams` | `datalake.quarantine.sales_teams_rejected` |
| Users | `datalake.silver.users` | `datalake.quarantine.users_rejected` |
| Customers | `datalake.silver.customers` | `datalake.quarantine.customers_rejected` |
| Product Categories | `datalake.silver.product_categories` | `datalake.quarantine.product_categories_rejected` |
| Products | `datalake.silver.products` | `datalake.quarantine.products_rejected` |
| Orders | `datalake.silver.orders` | `datalake.quarantine.orders_rejected` |
| Order Lines | `datalake.silver.order_lines` | `datalake.quarantine.order_lines_rejected` |

### 7.2 Incremental checkpoints

Each dataset has a checkpoint:

```text
hdfs://localhost:9000/capstone/checkpoints/bronze_to_silver/<dataset>
```

The stream reads:

```text
hdfs://localhost:9000/capstone/bronze/data/<dataset>
```

The checkpoint determines what is new. If there is no new Bronze file, the run reports `NO_NEW_DATA`.

### 7.3 Profiling

Exploration results:

```text
datalake.operations.explorations
```

Great Expectations results:

```text
datalake.operations.expectation_profiles
```

Great Expectations provides batch-level evidence. Spark rules control row-level `VALID` or `REJECTED` routing.

### 7.4 Odoo normalization

Examples:

```text
[101,"S00101"]
→ relation_id = 101
→ relation_name = S00101

[4,5,6]
→ array<long>

false, "", "null"
→ null when the field is optional
```

Dataset-specific relation parsing converts Customer, Company, Team, User, Product, Category, UOM, and other Odoo structures into usable typed columns.

### 7.5 Deduplication

Valid records are deduplicated by stable natural keys:

```text
company_id
sales_team_id
user_id
customer_id
product_category_id
product_id
order_id
sales_order_line_id
```

Datasets with `write_date` choose the latest version by:

```text
write_date DESC
bronze_ingestion_timestamp DESC
source_batch_id DESC
source_file DESC
```

Superseded valid rows count as duplicates, not rejected records.

### 7.6 Iceberg MERGE

```text
New business key
→ INSERT

Existing key with newer source version
→ UPDATE

Older incoming version
→ no change
```

This is an idempotent latest-valid-record upsert.

### 7.7 Silver reconciliation

Every microbatch validates:

```text
source_count
= valid_before_dedup_count
+ rejected_count

valid_before_dedup_count
= valid_after_dedup_count
+ duplicate_count

valid_after_dedup_count
= incoming_keys_found_in_silver
```

Audit table:

```text
datalake.operations.silver_ingestion_audit
```

---

# Part III. Gold

## 8. Gold Purpose

Gold converts validated Silver records into a conformed dimensional model. It provides stable keys, Customer history, Sales Order and Order-Line facts, controlled Unknown members, deterministic reruns, and quality certification.

Gold contains dimensions and facts only. Dashboard summaries are created in Aggregation.

---

## 9. Gold Shared Patterns

### 9.1 Deterministic surrogate keys

Most keys use SHA-256:

```text
company_sk = SHA-256("company||" + company_id)
product_sk = SHA-256("product||" + product_id)
region_sk  = SHA-256("region||" + normalized_region_key)
```

Customer version keys include the SCD2 start timestamp:

```text
customer_sk
= SHA-256("customer||" + customer_id + "||" + effective_from)
```

### 9.2 Controlled Unknown members

Every conformed dimension includes one Unknown member, normally with natural ID `-1`.

Use Unknown members when an optional dimensional relationship is unresolved. Do not fabricate a missing parent Order business event.

### 9.3 Record hashes

Business attributes and resolved keys produce a deterministic `record_hash`. Run IDs and technical timestamps are excluded.

```text
same hash → unchanged
changed hash → update
new key → insert
```

### 9.4 Gold operational tables

```text
datalake.operations.gold_processing_audit
datalake.operations.gold_referential_quality
datalake.operations.gold_rejected_records
datalake.operations.gold_order_line_reconciliation
datalake.operations.gold_quality_gate
```

---

## 10. Gold Dimensions

### 10.1 Date

`datalake.gold.dim_date` contains 5,844 dates from 2020-01-01 through 2035-12-31.

### 10.2 Latest-state dimensions

```text
dim_company
dim_sales_team
dim_salesperson
dim_region
dim_product_category
dim_product
```

These use controlled latest overwrite with deterministic MERGE behavior.

### 10.3 Customer SCD Type 2

`datalake.gold.dim_customer` stores one row per Customer history version.

Tracked changes include Customer identity, contact, location, Company, status, ranks, and seed attributes.

Initial known versions use:

```text
effective_from = 1900-01-01 00:00:00
```

This is a technical baseline for historical warehouse events. Future versions use the Odoo source `write_date`.

SCD2 invariants:

- Exactly one current version per Customer
- No overlapping periods
- `effective_from < effective_to`
- Open-ended current period
- Unique `customer_sk`
- Exactly one Unknown Customer

Validated current Customer rows including Unknown:

```text
3,763
```

---

## 11. Gold Facts

### 11.1 Sales Order fact

Table:

```text
datalake.gold.fact_sales_order
```

Grain:

```text
One row per Sales Order
```

All Order states are retained. The business reporting flag is:

```text
is_eligible_order
```

Current source outcome:

```text
10,640 total Orders
10,600 eligible sale Orders
40 ineligible cancelled Orders
4,456,698.12 eligible Order-header net revenue
```

Customer history is resolved temporally. A missing descriptive Customer uses Unknown Customer and Unknown Region, while preserving the valid Order event.

### 11.2 Sales Order Line fact

Table:

```text
datalake.gold.fact_sales_order_line
```

Grain:

```text
One row per sales_order_line_id
```

Core calculations:

```text
gross_line_value = ordered_quantity × unit_price
discount_amount = gross_line_value × discount_percentage / 100
net_line_revenue = price_subtotal
delivery_backlog = MAX(ordered - delivered, 0)
invoice_backlog = MAX(ordered - invoiced, 0)
delivery_fill_ratio = capped delivered / ordered
invoice_quantity_ratio = capped invoiced / ordered
```

Lines are rejected when the parent Order or Product business relationship cannot be constructed safely. Missing parent Orders are not replaced with an artificial Unknown Order.

### 11.3 Header-to-line revenue reconciliation

Order-header revenue and summed line revenue are compared per Order with a tolerance of `0.01`. Mismatches are audited rather than treated as fatal because source profiling identified legitimate differences.

---

## 12. Gold Quality Gate

The Gold Quality Gate verifies:

- Dimension surrogate-key uniqueness
- One Unknown member per dimension
- Customer SCD2 validity
- Complete Silver Order representation
- Fact-key validity
- Date resolution
- Revenue reconciliation
- Accepted plus rejected line completeness
- Ratio boundaries
- Nonnegative backlog
- Source-traceable parent Order gaps

Hard failures stop Gold. Documented source limitations remain warnings.

Gold execution order:

```text
dim_date
→ dim_company
→ dim_sales_team
→ dim_salesperson
→ dim_region
→ dim_product_category
→ dim_product
→ dim_customer
→ fact_sales_order
→ fact_sales_order_line
→ Gold Quality Gate
```

---

# Part IV. Aggregation

## 13. Aggregation Purpose

Aggregation converts Gold dimensions and facts into dashboard-ready tables. It calculates period metrics, lifetime intelligence, operational performance, deterministic Customer risk, concentration, and governed business insights.

Namespace:

```text
datalake.aggregation
```

---

## 14. Shared Aggregation Rules

### Eligible Orders

```text
fact_sales_order.is_eligible_order = true
```

### Financial types

```text
Money: DECIMAL(18,2)
Quantities and ratios: DECIMAL(18,4)
HHI: DECIMAL(18,6)
```

### Revenue-multiplication prevention

Order header revenue is never joined directly to multiple Order Lines. Lines are first aggregated to one row per `order_id`, then joined to Orders.

### Stable grains

Grouping uses surrogate keys and stable identifiers. Names are derived after grouping.

### Audit

Every pipeline writes:

```text
datalake.operations.aggregation_processing_audit
```

A successful run has:

```text
status = SUCCESS
reconciliation_status = RECONCILED
error_message = NULL
```

---

## 15. Aggregation Tables

| Table | Grain/Purpose | Validated result |
|---|---|---:|
| `revenue_summary` | Daily executive and operational grain by organization and Region | 8,707 rows |
| `customer_period_metrics` | Monthly Customer performance | 7,877 rows |
| `customer_intelligence` | One current Customer with lifetime metrics | 3,763 rows |
| `product_period_metrics` | Monthly Product and Category performance | 105,058 rows |
| `product_intelligence` | One Product with lifetime metrics | 4,072 rows |
| `order_fulfilment_summary` | Monthly Company/Region fulfilment | 591 rows |
| `sales_team_performance` | Monthly Sales Team performance | 928 rows |
| `salesperson_performance` | Monthly Salesperson performance | 2,005 rows |
| `customer_risk` | One explainable risk row per Customer | 3,763 rows |
| `revenue_concentration` | Monthly Customer concentration | 11 rows |
| `business_insights` | Deterministic actionable insight events | Rule-derived |

### 15.1 Certified totals

```text
Eligible Orders: 10,600
Order-header net revenue: 4,456,698.12
Order-Line net revenue: 4,456,231.37
Current Customers: 3,763
Current Products: 4,072
```

The header-to-line revenue difference is intentional and audited.

### 15.2 Customer Intelligence

Customer period metrics roll into lifetime intelligence while retaining all current Customers, including 569 Customers without eligible activity.

### 15.3 Product Intelligence

Product period metrics reconcile to Order-Line revenue. Product Intelligence contains 3,362 Products with activity and 710 without activity.

### 15.4 Customer risk

The model is deterministic, not predictive.

Validated thresholds:

```text
P50 recency: 264 days
P75 recency: 342 days
P90 recency: 415 days
High-value threshold: 1,177.25
```

Risk bands:

```text
0-39 LOW
40-69 MEDIUM
70-100 HIGH
```

Validated distribution:

```text
LOW: 3,181
MEDIUM: 582
HIGH: 0
```

### 15.5 Revenue concentration

Metrics include Top-1, Top-5, Top-10 Customer shares and Customer Revenue HHI.

```text
HHI = SUM(customer_revenue_share²)
```

### 15.6 Business insights

Insight types:

```text
CUSTOMER_RISK
PRODUCT_DELIVERY_BACKLOG
PRODUCT_INVOICE_BACKLOG
REVENUE_CONCENTRATION
```

These are deterministic business-rule messages, not generative AI content.

---

## 16. Aggregation Quality Gate

Result table:

```text
datalake.operations.aggregation_quality_gate
```

Checks cover:

- All 11 table existence checks
- Latest pipeline audit success
- Order and revenue reconciliation
- Customer counts
- Product counts
- Risk-score boundaries
- Concentration-share boundaries
- HHI boundaries

Validated outcome:

```text
PASS / HARD: 37
PASS / WARNING: 1
FAIL / HARD: 0
```

Aggregation execution order:

```text
revenue_summary
→ customer_period_metrics
→ customer_intelligence
→ product_period_metrics
→ product_intelligence
→ order_fulfilment_summary
→ sales_team_performance
→ salesperson_performance
→ customer_risk
→ revenue_concentration
→ business_insights
→ Aggregation Quality Gate
```

---

# Part V. Airflow Orchestration

## 17. Orchestration Principle

Airflow orchestrates the already-tested Bash runners. It does not duplicate Spark or notebook logic.

```text
Parent DAG
→ parameter resolution and child ordering

Child DAG
→ task dependencies

Bash runner
→ Papermill command construction

Notebook
→ Spark processing
```

---

## 18. Parent and Child DAGs

Parent:

```text
enterprise_revenue_parent_pipeline
```

Dataset child DAGs:

```text
companies_pipeline
sales_teams_pipeline
users_pipeline
customers_pipeline
product_categories_pipeline
products_pipeline
orders_pipeline
order_lines_pipeline
```

Analytical child DAGs:

```text
gold_pipeline
aggregation_pipeline
```

Each dataset child performs:

```text
start
→ Landing-to-Bronze BashOperator
→ Bronze-to-Silver BashOperator
→ Silver validation
→ end
```

Gold performs dimensions, facts, Gold Quality Gate, and final validation. Aggregation performs all 11 tables, Aggregation Quality Gate, and final validation.

### 18.1 Parent parameter chain

```text
Parent trigger JSON
→ parent typed Params
→ TriggerDagRunOperator conf
→ child Params
→ BashOperator environment strings
→ runner variables
→ Papermill -p
→ notebook parameter cell
```

### 18.2 Controlled parallel waves

The final parent can run independent dataset children in controlled pairs using `local[2]`:

```text
Wave 1: Companies + Sales Teams
Wave 2: Users + Customers
Wave 3: Product Categories + Products
Wave 4: Orders + Order Lines
```

Each wave waits for both children before progressing. Gold starts only after all Silver validations succeed. Aggregation starts only after Gold succeeds.

```text
Dataset waves
→ silver_complete
→ Gold
→ Gold complete
→ Aggregation
→ Aggregation complete
→ end
```

`max_active_runs=1` prevents overlapping parent runs on the single system.

---

# Part VI. KPI Dashboard

## 19. Dashboard Data Access

Technology flow:

```text
Streamlit
→ queries.py
→ DuckDB
→ iceberg_scan(latest metadata JSON)
→ custom fsspec WebHDFS adapter
→ HDFS Iceberg files
```

Dashboard title:

```text
Odoo Sales & Risk Analytics Platform
```

Tagline:

```text
Transforming Odoo Business Data into Actionable Intelligence.
```

The dashboard reads only certified Aggregation and Operations tables.

---

## 20. Dashboard Pages

### 20.1 Executive Revenue Overview

Shows selected-period revenue, Orders, Average Order Value, active Customers, closure, unmet demand, fill, invoicing, lead time, monthly trends, Regions, and Sales Teams.

### 20.2 Customer Intelligence

Shows selected-period Customer performance plus current Customer risk, recency, backlog, and Customer drill-down.

### 20.3 Product Intelligence

Shows selected-period Product Order-Line revenue, quantities, discounts, fill, invoicing, backlog, Category ranking, and Product drill-down.

### 20.4 Sales and Fulfilment Performance

Provides Fulfilment Overview, Sales Team, and Salesperson tabs with commercial and operational comparisons.

### 20.5 Risk, Concentration and Business Insights

Shows current Customer risk, monthly concentration, HHI, risk reasons, insight volume, and readable recommended-action messages.

### 20.6 Reporting modes

```text
Full period
Date range
Single date
Single month
```

Period tables respond to date filters. Lifetime and current snapshot tables remain clearly identified.

### 20.7 Certification banner

The dashboard reads the latest Aggregation Quality Gate and processing audit to display:

```text
Data status
Hard checks passed
Hard failures
Quality timestamp
Latest successful refresh
```

---

# Part VII. AI Analytics Assistant

## 21. AI Purpose

The AI page supports:

- Live certified data questions
- KPI-definition questions
- Hybrid definition plus live-value questions
- Explicit out-of-scope rejection

It is a governed analytics assistant, not a general chatbot.

---

## 22. AI Routes

```text
DATA
KNOWLEDGE
HYBRID
OUT_OF_SCOPE
```

### DATA

Vertex AI generates SQL. `sqlglot` validates it. DuckDB executes it against allowed Aggregation views. Gemini summarizes only verified rows.

### KNOWLEDGE

The assistant retrieves passages from the certified dashboard plan and KPI guide using local heading-aware BM25 retrieval.

### HYBRID

The assistant combines a documented KPI definition with a live certified value.

### OUT_OF_SCOPE

The assistant rejects general knowledge, forecasting, code writing, and write actions while explaining the supported scope.

---

## 23. AI Guards

- Read-only `SELECT` or `WITH ... SELECT`
- One SQL statement
- `sqlglot` parsing
- Analytical-area table allow-lists
- No `SELECT *`
- No SQL comments
- No file, URL, HDFS, or extension scans
- Maximum 50 result rows
- Maximum 20 rows sent to Gemini
- 100 KB maximum result JSON
- 800-character maximum question
- 20-message chat history
- Current risk cannot be presented as historical risk
- No forecasts without a certified forecasting model
- SQL and supporting records are visible to the user

AI analytical areas:

```text
EXECUTIVE
CUSTOMER
PRODUCT
SALES_FULFILMENT
RISK_INSIGHTS
DATA_QUALITY
```

---

## 24. RAG Knowledge Flow

Knowledge sources:

```text
docs/final_kpi_dashboard_plan.md
docs/kpi_dashboard_kpi_guide.md
```

```text
Markdown headings
→ heading-aware chunks
→ local BM25 index
→ KPI synonym normalization
→ top documentation passages
→ grounded Vertex AI answer
```

Examples:

```text
Order fill ratio → Quantity Fill Rate
AOV → Average Order Value
HHI → Customer Revenue HHI
Pending delivery → Delivery Backlog
```

---

# Part VIII. End-to-End Operational Flow

## 25. Complete Run

```text
1. Raw Odoo files arrive in Landing batches.
2. Parent DAG triggers dataset child DAGs.
3. Bronze processes new or requested file versions.
4. Silver checkpoints consume unprocessed Bronze files.
5. Silver validations complete for all datasets.
6. Gold builds conformed dimensions and facts.
7. Gold Quality Gate certifies the dimensional model.
8. Aggregation builds period and intelligence tables.
9. Aggregation Quality Gate certifies dashboard data.
10. Streamlit reads certified Iceberg tables through DuckDB.
11. Users analyze KPIs and download filtered details.
12. AI Assistant answers guarded data and documentation questions.
```

---

## 26. Replay and Recovery

### Bronze

File IDs prevent duplicate ingestion. Failed attempts remain retryable.

### Silver

Streaming checkpoints prevent reprocessing. A failed reconciliation prevents normal checkpoint advancement.

### Gold

Deterministic MERGE supports idempotent reruns. Surrogate-key changes require controlled dependent rebuilds.

### Aggregation

Derived tables use deterministic replacement. Unchanged inputs produce unchanged business results with a new audit event.

### Dashboard

Cached query results can be cleared using Refresh Data. The data remains certified by the latest Quality Gate.

### AI

AI failures do not change data and do not affect deterministic dashboard pages.

---

## 27. Final Project Outcome

The platform provides:

- Source-preserving raw ingestion
- Incremental, validated Silver processing
- Conformed dimensions and Customer history
- Complete Order and accepted Order-Line facts
- Certified revenue and operational aggregations
- Explainable Customer risk
- Revenue concentration and deterministic business insights
- Parent-child Airflow orchestration
- Interactive KPI analysis
- Guarded AI SQL analysis
- Documentation RAG
- Full audit, reconciliation, and quality evidence

The result is a reproducible, idempotent, explainable analytics platform that transforms Odoo operational exports into certified business intelligence.


