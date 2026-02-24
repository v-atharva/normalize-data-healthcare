# Healthcare Data Normalization Project

## Introduction

**Dataset: `legacy_healthcare_data`**

In this repository, I have transformed a legacy flat-structured healthcare dataset that contains information about doctor's appointments into a structured **Snowflake Schema**. The raw CSV data is normalized into **10 dimension tables** and **1 fact table**, producing an optimized structure that supports efficient querying and data management.

The transformation is implemented entirely in Python using only standard libraries (no `pandas` library was used), processes data in configurable batches for memory efficiency, and outputs each table as a separate CSV file.

---

## How I approached the Transformation

The `DataProcessor` class reads the flat 47-column legacy CSV and decomposes it into a normalized Snowflake schema through a single streaming pass over the data, using batch processing.

### Step-by-Step Process

#### 1. Initialization
The `DataProcessor` is created with empty dictionaries for each dimension table and a list for the fact table. Lookup dictionaries are initialized for efficient deduplication of entities that don't have natural keys in the source data (providers, locations, diagnoses, treatments).

#### 2. Batch Reading (`process_data`)
The source CSV is read row-by-row using `csv.DictReader`. Rows are accumulated into batches (default size: 5000) and processed together. This controls memory usage for large datasets.

#### 3. Row Processing (`_process_row`)
Each row is decomposed into its constituent dimension entities:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Single Source Row (47 cols)                     │
└─────────────┬───────────────────────────────────────────────────────┘
              │
              ▼
   ┌──────────────────┐
   │  Dedup check:    │──── Already seen visit_id? → Skip
   │  visit_id        │
   └────────┬─────────┘
            │
            ▼
  ┌─────────────────────────────────────────────────────────────┐
  │              Extract & Normalize Dimensions                 │
  │                                                             │
  │  Patient ──► DimPatient       (primary key: patient_id)     │
  │  Insurance ► DimInsurance     (primary key: insurance_id)   │
  │  Billing ──► DimBilling       (primary key: billing_id)     │
  │  Provider ─► DimProvider      (primary key: name|title|dept)│
  │  Location ─► DimLocation      (primary key: clinic|room)    │
  │  Primary Dx -► DimPrimaryDx     (primary key: code|desc)    │
  │  Secondary -► DimSecondaryDx    (primary key: code|desc)    │
  │  Treatment -► DimTreatment      (primary key: code|desc)    │
  │  Rx ──────► DimPrescription   (primary key: prescription_id)│
  │  Lab ─────► DimLabOrder       (primary key: lab_order_id)   │
  │                                                             │
  └──────────────────────────┬──────────────────────────────────┘
                             │
                             ▼
                   ┌───────────────────┐
                   │    FactVisit      │
                   │  (all FK refs +   │
                   │  visit_datetime,  │
                   │  visit_type)      │
                   └───────────────────┘
```

#### 4. Deduplication Strategy
- **Natural key tables** (Patient, Insurance, Billing, Prescription, Lab Order): Deduplicated by their source ID — if the ID has been seen before, the row is skipped.
- **Composite key tables** (Provider, Location, Diagnoses, Treatment): A composite string key is built from the entity's fields (e.g. `doctor_name|doctor_title|doctor_department`). A lookup dictionary maps this key to an auto-incremented integer ID. New unique combinations get new IDs.

#### 5. Date Parsing (`parse_date`)
All date fields (`patient_date_of_birth`, `billing_date`, `lab_result_date`) are normalized to `YYYY-MM-DD` format. The parser handles multiple input formats:
- `YYYY-MM-DD` (ISO standard)
- `MM/DD/YYYY` (US format)
- `DD-MM-YYYY` (Day-first)
- `YYYY/MM/DD` (Slash-separated)
- Datetime strings with time components (e.g. `2022-12-28 16:17:45.753446`) the time portion is stripped off.

#### 6. Patient Status Classification (`_update_patient_statuses`)
After all rows are processed, each patient's visit history is reviewed:
- **Active**: Has at least one visit on or after **January 1, 2022**
- **Inactive**: All visits are before January 1, 2022 (i.e., has not visited since end of 2021)

#### 7. CSV Output (`write_csv_files`)
Each dimension table and the fact table are written to separate CSV files in the `output/` directory using `csv.DictWriter`. Column order is enforced by the `SCHEMAS` dictionary to match the required schema exactly.

---

## Running the Code

1. **Clone the repository**
2. **Navigate to the project root** in your terminal.
3. **Run the transformation:**

   ```bash
   python src/main.py
   ```
---

## Output Summary

| # | Requirement | Details |
|---|---|---|
| 1 | **11 CSV output tables** | Column sequence and names match Snowflake schema |
| 2 | **Primary keys** | Primary keys can be either string or integer type |
| 3 | **Patient status** | Set to `Active` if the patient has visited on or after January 1, 2022; else `Inactive` |
| 4 | **No pandas** | Data processing uses only Python standard libraries |

---

The script will:
- Create the `output/` directory if it doesn't exist
- Print processing status to the console (column listing, batch progress, patient status summary)
- Generate **11 CSV files** in the `output/` directory:

| File | Description | Key Type |
|---|---|---|
| `DimPatient.csv` | Patient demographics + Active/Inactive status | String |
| `DimInsurance.csv` | Insurance policies linked to patients | String |
| `DimBilling.csv` | Billing records linked to insurance | String |
| `DimProvider.csv` | Unique doctor name / title / department combos | Integer |
| `DimLocation.csv` | Unique clinic name / room number combos | Integer |
| `DimPrimaryDiagnosis.csv` | Unique primary diagnosis code + description | Integer |
| `DimSecondaryDiagnosis.csv` | Unique secondary diagnosis code + description | Integer |
| `DimTreatment.csv` | Unique treatment code + description | Integer |
| `DimPrescription.csv` | Prescription details (drug, dosage, frequency, duration) | String |
| `DimLabOrder.csv` | Lab test results (code, name, value, units, date) | String |
| `FactVisit.csv` | Central fact table with all FK references + visit datetime/type | String |

---

## Key Design Decisions

- **Batch Processing**: Data is processed in configurable batches (default 5,000 rows) to handle large datasets without loading everything into memory at once.
- **Streaming Deduplication**: Lookup dictionaries allow O(1) dedup checks during the single-pass read, avoiding the need for a second pass or sorting.
- **Schema-Driven Output**: The `SCHEMAS` dictionary acts as the single source of truth for column order, ensuring CSV headers always match the required schema.
- **No External Dependencies**: The entire pipeline runs on Python's standard library.

## Project Structure

```
root/
│
├── dataset/
│   └── legacy_healthcare_data.csv    # Source: flat legacy data (47 columns)
│
├── src/
│   ├── main.py                       # Core transformation logic (DataProcessor class)
│
├── output/                           # Generated CSV files (11 tables)
│   ├── DimPatient.csv
│   ├── DimInsurance.csv
│   ├── DimBilling.csv
│   ├── DimProvider.csv
│   ├── DimLocation.csv
│   ├── DimPrimaryDiagnosis.csv
│   ├── DimSecondaryDiagnosis.csv
│   ├── DimTreatment.csv
│   ├── DimPrescription.csv
│   ├── DimLabOrder.csv
│   └── FactVisit.csv
│
└── README.md
```
