# FinShield 🛡️
### Financial Transaction Intelligence Platform

> An end-to-end financial fraud analytics pipeline built on Databricks using the Medallion Architecture — processing 6.3M+ PaySim transactions through incremental loading, CDC, SCD Type 2, and Gold-layer fraud risk scoring.

---

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Pipeline Layers](#pipeline-layers)
  - [Bronze Layer](#-bronze-layer)
  - [Silver Layer](#-silver-layer)
  - [Gold Layer](#-gold-layer)
- [Databricks Table Structure](#databricks-table-structure)
- [Notebook Flow](#notebook-flow)
- [Data Quality & Validation](#data-quality--validation)
- [Key Engineering Concepts](#key-engineering-concepts)
- [Real-World Mapping](#real-world-mapping)
- [How to Run](#how-to-run)
- [Future Enhancements](#future-enhancements)

---

## Overview

A transaction alone doesn't tell the full story of fraud. You also need account status, KYC information, customer risk tier, and behavioral history.

**FinShield** combines all of these into a structured lakehouse pipeline:

```
Raw PaySim Transactions
         ↓
   Bronze Layer   →   Raw Delta Table
         ↓
   Silver Layer   →   Incremental Txns + Account CDC + Customer SCD2
         ↓
    Gold Layer    →   Daily Summaries + Customer 360 + Fraud Risk Scores
```

The project models how financial data engineering works in real banking/fintech environments — with proper incremental processing, history tracking, and audit-ready validation at each stage.

---

## Architecture

```
                    ┌─────────────────────────┐
                    │   PaySim CSV Dataset     │
                    │   ~6.3M transactions     │
                    └───────────┬─────────────┘
                                │
                    ┌───────────▼─────────────┐
                    │      Bronze Layer        │
                    │  Raw Delta Table         │
                    │  (close to source)       │
                    └───────────┬─────────────┘
                                │
          ┌─────────────────────▼──────────────────────┐
          │               Silver Layer                  │
          │                                            │
          │  ① Incremental Transactions  (watermark)   │
          │  ② Account Dimension         (CDC)         │
          │  ③ Customer Dimension        (SCD Type 2)  │
          └─────────────────────┬──────────────────────┘
                                │
          ┌─────────────────────▼──────────────────────┐
          │                Gold Layer                   │
          │                                            │
          │  ① Daily Transaction Summary               │
          │  ② Customer 360                            │
          │  ③ Fraud Risk Scores                       │
          └────────────────────────────────────────────┘
```

---

## Tech Stack

| Area | Technology |
|---|---|
| Platform | Databricks |
| Processing | PySpark, Spark SQL |
| Storage Format | Delta Lake |
| Architecture | Medallion Architecture |
| Dataset | PaySim Synthetic Financial Transactions |
| Language | Python, SQL |

---

## Dataset

**PaySim Synthetic Financial Transaction Dataset**
- ~6.3 million rows
- File: `paysim_transactions.csv`

| Column | Description |
|---|---|
| `step` | Simulation time step — used as watermark |
| `type` | Transaction type (TRANSFER, PAYMENT, etc.) |
| `amount` | Transaction amount |
| `nameOrig` | Origin account ID |
| `nameDest` | Destination account ID |
| `isFraud` | Actual fraud label |
| `isFlaggedFraud` | Existing rule-based fraud flag |

> PaySim provides only transaction-level data. Account and customer master feeds were synthetically derived from transaction account IDs and processed through CDC and SCD2 logic to model a realistic banking data architecture.

---

## Pipeline Layers

### 🥉 Bronze Layer

The Bronze layer is the raw landing zone. The PaySim CSV is loaded as-is into a Delta table, preserving the source format.

**Table:** `data_engineering.bronze_layer.finshield_bronze`

---

### 🥈 Silver Layer

The Silver layer is where data gets cleaned, enriched, and modelled for downstream use.

#### 1. Incremental Transactions

Transactions are processed **incrementally** using the `step` column as a watermark. A control table tracks the last processed step.

```
Run 1 → step 1  to 50
Run 2 → step 51 to 100
Run 3 → step 101 to 150
```

Since PaySim has no native transaction ID, a **deterministic SHA-256 hash** is generated over transaction attributes to enable idempotent MERGE processing.

**Table:** `data_engineering.silver_layer.finshield_silver`

**Key patterns:**
- Watermark-based incremental loading
- MERGE-based idempotency
- Duplicate-safe processing

---

#### 2. Account Dimension — CDC

Account entities are derived from transaction account IDs and enriched with account-level attributes including account type, status, credit score, KYC flag, and open date.

**CDC operations handled:**
- `U` — Update account attributes
- `D` — Soft delete (status → `CLOSED`, `is_deleted = true`)

Soft deletes preserve the account record for historical analysis instead of physically removing it.

**Table:** `data_engineering.silver_layer.dim_accounts`

**Rule:** One `account_id` = one latest-state row

---

#### 3. Customer Dimension — SCD Type 2

Customer history is maintained using **SCD Type 2** to preserve the customer profile *at the time of each transaction* — critical for accurate fraud analysis.

**Tracked fields:**
- `risk_tier`
- `address`

When either field changes:
1. The current row is expired (`is_current = false`, `effective_end_date` set)
2. A new current row is inserted (`is_current = true`, `effective_end_date = null`)

```
Before:
C123 | LOW  | Address_1         | is_current=true  | effective_end_date=null

After change:
C123 | LOW  | Address_1         | is_current=false | effective_end_date=<date>
C123 | MED  | Address_1_UPDATED | is_current=true  | effective_end_date=null
```

A **staging table** is used before applying SCD2 updates to avoid Spark lazy evaluation issues when reading and writing to the same Delta table.

**Table:** `data_engineering.silver_layer.dim_customers`

**Validated SCD2 output:**

| Metric | Count |
|---|---|
| Total customer rows | 2,404,682 |
| Distinct accounts | 2,380,863 |
| Current records | 2,380,863 |
| Historical records | 23,819 |

---

### 🥇 Gold Layer

The Gold layer contains business-ready analytical tables for fraud monitoring and reporting.

#### Table 1: `daily_txn_summary`

Aggregates transaction activity by `step` + `transaction_type`.

**Metrics:** transaction count, total/avg/min/max amount, fraud count, flagged count, fraud rate %

**Use cases:** transaction trend analysis, fraud rate monitoring, volume comparison by type

---

#### Table 2: `customer_360`

One analytical profile per account/customer joining Silver transactions + latest account state + current customer profile.

**Metrics:** total transactions, total/avg/max amount, fraud count, flagged count, fraud rate %

**Use cases:** customer-level fraud monitoring, high-risk account identification, behavioral analysis by risk tier and KYC status

---

#### Table 3: `fraud_risk_scores`

One risk score per transaction using a rule-based scoring model.

**Risk signals:**
- Transaction amount
- Account status (active / suspended / closed)
- Soft-deleted account flag
- KYC verification status
- Customer risk tier
- Existing system fraud flag

**Risk distribution:**

| Risk Category | Transaction Count | Percentage |
|---|---|---|
| LOW | 1,591,388 | 66.80% |
| MEDIUM | 698,501 | 29.32% |
| HIGH | 91,237 | 3.83% |
| CRITICAL | 1,056 | 0.04% |

This distribution is realistic — the vast majority of transactions are low risk, with only a small fraction flagged as critical.

**Final Gold output counts:**

| Gold Table | Row Count |
|---|---|
| `daily_txn_summary` | 726 |
| `customer_360` | 2,380,863 |
| `fraud_risk_scores` | 2,382,182 |

---

## Databricks Table Structure

```
data_engineering/
│
├── bronze_layer/
│   └── finshield_bronze
│
├── silver_layer/
│   ├── finshield_silver
│   ├── dim_accounts
│   └── dim_customers
│
├── gold_layer/
│   ├── daily_txn_summary
│   ├── customer_360
│   └── fraud_risk_scores
│
└── table_metadata/
    ├── finshield_control
    ├── account_cdc_events_audit
    ├── customer_scd2_changes_audit
    └── changed_customers_scd2_stage
```

---

## Notebook Flow

| # | Notebook | Purpose |
|---|---|---|
| 01 | `bronze_ingest` | Load raw PaySim CSV into Bronze Delta table |
| 02 | `silver_transactions_incremental_merge` | Incrementally load transactions using watermark + MERGE |
| 03 | `generate_dimensions` | Generate account and customer master dimensions |
| 04 | `accounts_cdc` | Apply CDC logic on account dimension |
| 05 | `accounts_cdc_validation` | Validate account CDC output |
| 06 | `customers_scd2` | Apply SCD Type 2 on customer dimension |
| 07 | `customers_scd2_validation` | Validate SCD2 history rules |
| 08 | `gold_aggregations` | Build Gold analytics tables |

---

## Data Quality & Validation

Validation checks are added at key stages rather than a centralized framework, keeping it lightweight but effective.

**CDC Validation**
- Account ID uniqueness check
- Soft delete rule verification
- Update event reflection check
- Latest-state table structure validation

**SCD2 Validation**
- One current record per customer
- Historical records preserved
- `effective_end_date = null` on current rows
- `effective_end_date` populated on expired rows
- Multi-version check for changed customers

**Gold Validation**
- Row count checks per Gold table
- Fraud risk score coverage
- Risk category distribution check
- Customer-level aggregation output validation

> In a production implementation, these checks can be centralized into a reusable DQ framework with an audit table: `data_engineering.table_metadata.dq_audit_log`

---

## Key Engineering Concepts

| Concept | Implemented In |
|---|---|
| Medallion Architecture | Bronze → Silver → Gold |
| Incremental Loading | Silver transactions |
| Watermarking | Control table (`finshield_control`) |
| MERGE / Idempotency | Silver transactions (SHA-256 ID) |
| Change Data Capture (CDC) | Account dimension |
| Soft Deletes | Account CDC |
| SCD Type 2 | Customer dimension |
| Gold Aggregations | Fraud analytics tables |
| Validation | CDC, SCD2, and Gold outputs |

---

## Real-World Mapping

In a real banking environment, each data feed would come from a different source system:

| Data | Real-World Source |
|---|---|
| Transactions | Payment processor / transaction system |
| Account master | Core banking system |
| Customer master | CRM / KYC system |
| CDC events | GoldenGate / Debezium / ADF CDC / database logs |
| Fraud flags | Fraud rules engine / AML system |

PaySim provides only transaction data. To model a realistic financial architecture, account and customer master change feeds were synthetically created from transaction account IDs and processed through CDC and SCD2 logic.

---

## How to Run

Run notebooks in the following order:

```
1. 01_bronze_ingest
2. 02_silver_transactions_incremental_merge
3. 03_generate_dimensions
4. 04_accounts_cdc
5. 05_accounts_cdc_validation
6. 06_customers_scd2
7. 07_customers_scd2_validation
8. 08_gold_aggregations
```

**Recommended clean run:**
1. Load Bronze data
2. Run Silver incremental transaction pipeline (multiple runs to simulate batches)
3. Generate account and customer dimensions
4. Apply and validate account CDC
5. Apply and validate customer SCD2
6. Build Gold analytics tables

---

## Future Enhancements

- [ ] GraphFrames-based fraud ring detection
- [ ] Dashboard screenshots and sample outputs

---

*Built using Databricks, PySpark, Delta Lake, and the PaySim dataset.*
