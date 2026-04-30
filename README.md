# Bludot Pipeline

Automated business record matching pipeline. Matches city business license records against the Bludot database using a two-pass LLM-assisted approach with human-in-the-loop review gates.

**Stack:** FastAPI · SQLite · Groq (Llama 3.3 70B) · React · Docker

---

## Table of Contents

1. [Project Structure](#project-structure)
2. [Setup & Installation](#setup--installation)
3. [Environment Variables](#environment-variables)
4. [Running the App](#running-the-app)
5. [Full Pipeline — Step by Step](#full-pipeline--step-by-step)
   - [Step 0 — Deduplication (LSH)](#step-0--deduplication-lsh)
   - [Human Review 0 — Cluster Review](#human-review-0--cluster-review-soft)
   - [Step 1 — Reformat + Merge Columns](#step-1--reformat--merge-columns)
   - [Step 2 — City vs Bludot Matching (Pass 1)](#step-2--city-vs-bludot-matching-pass-1)
   - [Human Review 1 — Match Review Pass 1](#human-review-1--match-review-pass-1-soft)
   - [Step 3 — Split Records](#step-3--split-records)
   - [Step 4 — Second-Pass Matching](#step-4--second-pass-matching-pass-2)
   - [Human Review 2 — Match Review Pass 2](#human-review-2--match-review-pass-2-soft)
   - [Step 5 — Generate Output Sheets](#step-5--generate-output-sheets)
   - [Step 6 — Contacts Deduplication](#step-6--contacts-deduplication)
6. [src/ Support Library](#src-support-library)
7. [File Outputs](#file-outputs)
8. [Frontend Pages](#frontend-pages)
9. [API Endpoints](#api-endpoints)
10. [Key Design Decisions](#key-design-decisions)
11. [Changelog](#changelog)
12. [Troubleshooting](#troubleshooting)

---

## Project Structure

```
bludot_pipeline/
├── .env                                     ← GROQ_API_KEY goes here
├── docker-compose.yml
├── README.md
├── backend/
│   ├── core/
│   │   ├── step0_dedup.py                   ← LSH dedup + bludot concat + DB ingest
│   │   ├── step1_format.py                  ← Pivot clusters + merge numbered columns
│   │   ├── step2_match.py                   ← Pass 1 matching (thin wrapper)
│   │   ├── step3_split.py                   ← Split matched / unmatched records
│   │   ├── step4_match.py                   ← Pass 2 matching (thin wrapper)
│   │   ├── step5_output.py                  ← Business + Custom + Contact Excel output
│   │   ├── step6_contacts.py                ← Contact dedup + append to final Excel
│   │   ├── matching_orchestrator.py         ← Candidate generation + rule + LLM judge
│   │   ├── llm_judge.py                     ← Groq API (1 batched call per step)
│   │   └── rule_filter.py                   ← Pre-LLM rule filter (Stage 1)
│   ├── db/
│   │   ├── models.py                        ← SQLAlchemy ORM models
│   │   └── session.py                       ← DB engine + init_db()
│   ├── pipeline/
│   │   └── pipeline.py                      ← Orchestrator — runs all steps in order
│   ├── src/                                 ← Legacy support library (required)
│   │   ├── __init__.py
│   │   ├── bludot_concat.py                 ← Original bludot sheet concatenation
│   │   ├── contact_formatting.py            ← Contact dedup + clean (used by Step 6)
│   │   ├── country_state_mapping.json       ← State abbreviation lookup (used by Step 5)
│   │   ├── de_duplication.py                ← Original pandas-dedupe clustering
│   │   ├── deduplicate_phonenumbers.py      ← Phone deduplication by Excel column range
│   │   ├── final_name_matching.py           ← 6-filter fuzzy name matching pipeline
│   │   ├── final_sheet_creation.py          ← Business/Custom/Contact sheet builder (used by Step 5)
│   │   ├── fuzzy_matching.py                ← Fuzzy match orchestration utilities
│   │   └── updated_pickle.pkl               ← Abbreviation expansion dictionary
│   ├── main.py                              ← FastAPI app + all API endpoints
│   ├── requirements.txt
│   └── uploads/                             ← All city data + results live here
│       └── {CityName}/
│           ├── {city_sheet}.xlsx
│           ├── {bludot_export}.xlsx
│           ├── city_schema.json             ← Required for Step 5 + 6
│           └── results/
│               ├── city_data/
│               │   ├── manual_dedup_records.xlsx
│               │   └── de_duplication_merged.xlsx
│               ├── bludot_data/
│               │   └── bludot_concatenated_records.xlsx
│               └── output/
│                   ├── final_result/
│                   │   ├── final_matched_records_for_{city}.xlsx
│                   │   ├── additional_city_records_for_{city}.xlsx
│                   │   └── additional_bludot_records_for_{city}.xlsx
│                   ├── final_excel/
│                   │   ├── Business_Matched_Records.xlsx
│                   │   ├── Additional_Business_Matched_Records.xlsx
│                   │   ├── Custom_Matched_Records.xlsx
│                   │   ├── Additional_Custom_Matched_Records.xlsx
│                   │   ├── Contact_Matched_Records.csv
│                   │   └── Additional_Contact_Matched_Records.csv
│                   └── final_output/
│                       ├── {city}_Business_Matched_Records.xlsx
│                       └── Additional_Matched_Records_Of_{city}.xlsx
└── frontend/
    └── src/
        ├── App.jsx
        ├── App.css
        ├── main.jsx
        ├── hooks/useApi.js
        └── pages/
            ├── CitiesPage.jsx
            ├── NewCityPage.jsx
            ├── CityDetailPage.jsx
            ├── ReviewPage.jsx
            ├── ClusterReviewPage.jsx
            ├── MatchesPage.jsx
            └── DedupResultsPage.jsx
```

---

## Setup & Installation

### Prerequisites

- Docker + Docker Compose
- Groq API key — free at https://console.groq.com

### First-Time Setup

```bash
# 1. Navigate to project folder
cd ~/bludot_pipeline

# 2. Create uploads folder (required — avoids permission error on startup)
mkdir -p backend/uploads

# 3. Create .env file with your Groq key
echo "GROQ_API_KEY=gsk_your_key_here" > .env

# 4. Fix any locked folders (if you had an old version running as root)
sudo chown -R $USER:$USER backend/uploads/

# 5. Build and start
docker-compose build --no-cache backend
docker-compose up
```

| Service      | URL                        |
|--------------|----------------------------|
| Frontend     | http://localhost:3000      |
| Backend API  | http://localhost:8000      |
| API Docs     | http://localhost:8000/docs |

---

## Environment Variables

| Variable       | Required | Description                                                     |
|----------------|----------|-----------------------------------------------------------------|
| `GROQ_API_KEY` | Yes      | Groq API key from console.groq.com (free tier: 14,400 req/day) |
| `DATABASE_URL` | Auto-set | `sqlite:////app/backend/bludot_pipeline.db`                     |

---

## Running the App

```bash
# Start everything
docker-compose up

# Restart backend only (after copying new .py files)
docker-compose restart backend

# Full rebuild (required after changes to requirements.txt)
docker-compose build --no-cache backend && docker-compose up

# Watch logs
docker-compose logs -f backend
```

---

## Full Pipeline — Step by Step

> All **Human Review** gates are **SOFT** — if nothing needs review, the pipeline continues automatically.

---

### Step 0 — Deduplication (LSH)

**File:** `backend/core/step0_dedup.py`

Runs as 5 internal sub-steps:

**0.1 — LSH clustering**
- Reads the city sheet and applies the saved column mapping from DB
- Normalises addresses before comparison: strips unit/suite suffixes (`# 1/2`, `STE`, `APT`, `UNIT`, `PMB`, `BLDG`, `FL`, range designators like `2710-3040`)
- Runs LSH + TF-IDF to cluster duplicate records by Business Name + Address; assigns a `cluster id` to every row
- Writes `results/city_data/manual_dedup_records.xlsx` and bulk-ingests city records into DB

**0.2 — Intra-cluster verification**
- Checks low-confidence pairs within the same cluster

  | Case | Decision |
  |------|----------|
  | Same name + same normalised address | Auto-merge (no LLM) |
  | Different street numbers | Auto-split (no LLM) |
  | One/both addresses blank | LLM decides |
  | Unit suffix difference (e.g. `# 1/2`) | LLM decides |
  | LLM uncertain | Human cluster review |

**0.3 — Cross-cluster near-miss scan**
- Vectorised rapidfuzz `cdist` scan finds records in different clusters with ≥ 92% name similarity

**0.4 — Near-miss verification**

  | Case | Decision |
  |------|----------|
  | Same normalised address | Auto-merge (no LLM) |
  | Ambiguous | LLM decides |
  | LLM uncertain or NOT_DUPLICATE | Human cluster review |

**0.5 — Bludot concatenation**
- Reads the Bludot export and merges Business, Custom, and Contact sheets on UUID into one table
- Writes `results/bludot_data/bludot_concatenated_records.xlsx` and bulk-ingests Bludot records into DB

**Returns:** `{city_records, deduped_records, clusters, bludot_records, intra_pairs, near_miss_pairs}`

---

### Human Review 0 — Cluster Review *(soft)*

**UI page:** `/city/{id}/cluster-review`

Shows near-miss pairs the LLM could not decide on:

- **Merge All** — confirm as duplicates, merge into the same cluster
- **Keep Separate** — confirm they are different businesses

> If 0 uncertain pairs → pipeline continues automatically to Step 1

---

### Step 1 — Reformat + Merge Columns

**File:** `backend/core/step1_format.py`

1. Reads `manual_dedup_records.xlsx`
2. Pivots clusters — keeps the most complete row per `cluster id` (fewest NaN values)
3. Merges numbered columns (`_1`, `_2`, `_3`…):

   | Column | Rule |
   |--------|------|
   | Business Name | Picks the longest non-empty value |
   | Address1/2, City, State, Zipcode, Website | Takes the first non-empty value |
   | Phone number | Collects all unique numbers, deduplicates (handles prefix/suffix overlap), joins with `, ` |

4. Ensures `city_index` column exists

**Output:** `results/city_data/de_duplication_merged.xlsx`

**Returns:** `{input_records, output_records}`

---

### Step 2 — City vs Bludot Matching (Pass 1)

**File:** `backend/core/step2_match.py` → delegates to `matching_orchestrator.py`

**Candidate generation** (`generate_candidates`):
- Name-prefix blocking (first 2–3 chars) creates candidate pairs across all city × Bludot records
- Each pair passes through `rule_filter.py`; `DEFINITE_NO_MATCH` pairs are dropped immediately
- Scores (`name_score`, `address_score`, `street_num_match`) are stored on each candidate

**LLM judge** (`run_llm_judge`) — two stages:

Stage A — pure rules, no API call, auto-decides ~80% of pairs:

| Condition | Decision |
|-----------|----------|
| Both street numbers present AND different | AUTO_REJECT |
| Name similarity < 50% | AUTO_REJECT |
| Both addresses blank + name ≥ 90% similar | AUTO_MATCH |
| Street numbers match + name ≥ 88% + address ≥ 75% | AUTO_MATCH |
| Exact normalised name match + one address blank | AUTO_MATCH |

Stage B — Groq LLM (1 batched call for all remaining ambiguous pairs):
- MATCH → `AUTO_MATCH`
- NO_MATCH → `AUTO_NO_MATCH`
- UNCERTAIN → `NEEDS_REVIEW`

**Column mapping:** When creating a new city, map city sheet columns to the standard schema. Map all name and address columns if multiple exist — all are considered during matching.

**Returns:** `{candidates, auto_match, auto_no_match, needs_llm, needs_review}`

---

### Human Review 1 — Match Review Pass 1 *(soft)*

**UI page:** `/city/{id}/review`

Shows UNCERTAIN pairs side-by-side (city record left, Bludot record right, LLM reason below). Submit accept/reject individually or in bulk.

> If 0 uncertain pairs → pipeline continues automatically to Step 3

---

### Step 3 — Split Records

**File:** `backend/core/step3_split.py`

Reads all confirmed pass-1 matches (`AUTO_MATCH` + `HUMAN_ACCEPTED`) from DB and writes:

| File | Contents |
|------|----------|
| `final_result/final_matched_records_for_{city}.xlsx` | Matched pairs side by side, reordered to align row-by-row |
| `final_result/additional_city_records_for_{city}.xlsx` | City records not yet matched |
| `final_result/additional_bludot_records_for_{city}.xlsx` | Bludot records not yet matched |

> If you find additional matches manually, create `filter_matches/city_bludot_index.xlsx` (columns: `city_index`, `bludot_index`) and resume — Step 4 will pick these up.

**Returns:** `{matched, additional_city, additional_bludot}`

---

### Step 4 — Second-Pass Matching (Pass 2)

**File:** `backend/core/step4_match.py` → delegates to `matching_orchestrator.py`

Same two-stage rule + LLM matching as Step 2, but scoped only to records not matched in pass 1. Results are stored with `match_pass=2` and shown in the UI separately. Exits early if no pass-2 candidates exist.

**Returns:** `{candidates, auto_match, auto_no_match, needs_llm, needs_review}`

---

### Human Review 2 — Match Review Pass 2 *(soft)*

**UI page:** `/city/{id}/review?pass=2`

Same review UI as pass 1, scoped to second-pass uncertain pairs.

> If 0 uncertain pairs → pipeline continues automatically to Step 5

---

### Step 5 — Generate Output Sheets

**File:** `backend/core/step5_output.py`  
**Requires:** `city_schema.json` at `uploads/{city}/city_schema.json` and `src/final_sheet_creation.py`

Reads the schema's field-mapping config and calls `src/final_sheet_creation.py` to produce three sheet types for both matched and additional records:

| Sheet type | Matched output | Additional output |
|------------|---------------|-------------------|
| Business | `Business_Matched_Records.xlsx` | `Additional_Business_Matched_Records.xlsx` |
| Custom | `Custom_Matched_Records.xlsx` | `Additional_Custom_Matched_Records.xlsx` |
| Contact | `Contact_Matched_Records.xlsx` | `Additional_Contact_Matched_Records.xlsx` |

All six files land in `output/final_excel/`. Then two combined multi-sheet Excel files are assembled in `output/final_output/`:

- `{city}_Business_Matched_Records.xlsx` — sheets: Business + Custom + Contact
- `Additional_Matched_Records_Of_{city}.xlsx` — sheets: Additional Business + Custom + Contact

UUID prefix for additional records: `{CITY_PREFIX}{YYYYMMDD}`.  
State abbreviations resolved via `src/country_state_mapping.json`.  
`largest_num_list` and `earliest_date_list` from `src/final_sheet_creation.py` are monkey-patched to handle blank/text values safely.

**Returns:** `{business_matched, business_additional, custom_matched, contact_matched}`

---

### Step 6 — Contacts Deduplication

**File:** `backend/core/step6_contacts.py`  
**Requires:** `CONTACT_CONFIG` in `city_schema.json` and `src/contact_formatting.py`

Processes contact data (owner names, emails, phones) from the Step 3 split files and appends the results as new sheets to the final Excel output.

**Flow:**

1. Reads `CONTACT_CONFIG` from `city_schema.json` for column definitions (person name, contact value, title, role, type)
2. Processes two input files:
   - `additional_city_records_for_{city}.xlsx` → `Additional_Contact_Matched_Records.csv`
   - `final_matched_records_for_{city}.xlsx` → `Contact_Matched_Records.csv`
3. For each file: extracts contact rows (skips rows with no person name), handles dynamic `_1/_2/_3` column suffixes, runs `format_contact_data()` from `src/contact_formatting.py` for dedup and cleaning, replaces `BLK_\d+` placeholder values with `-`
4. Appends each CSV as a new sheet to the appropriate final Excel file:
   - `Additional_Contact_Matched_Rec` → `Additional_Matched_Records_Of_{city}.xlsx`
   - `Contact_Matched_Records` → `{city}_Business_Matched_Records.xlsx`

If `CONTACT_CONFIG` is empty the step exits early. Missing input files are skipped with a warning.

**Returns:** `{contact_rows}`

---

## src/ Support Library

The `backend/src/` folder is a **legacy support library** that the pipeline imports directly. All files here must be present for the pipeline to run past Step 4.

### `bludot_concat.py`

Original Bludot sheet concatenation logic. Reads Business, Custom, and Contact sheets from a Bludot export Excel file, handles duplicate column names introduced by pandas (`.1`, `.2` suffixes), converts datetime columns to `MM/DD/YYYY` strings, and merges all three sheets on UUID/Custom Data Name/ID. The `step0_dedup.py` reimplements this logic internally but the original file is retained for reference and backward compatibility.

### `contact_formatting.py`

Provides `format_contact_data()` and `clean_column_names()`, called by Step 6. Deduplicates contact records and standardises column names before the contact CSV is written.

### `country_state_mapping.json`

JSON lookup used by Step 5 to convert full state names to two-letter abbreviations, covering all 50 US states and 13 Canadian provinces/territories.

### `de_duplication.py`

Original city deduplication module. Uses `pandas_dedupe` for cluster assignment and the `updated_pickle.pkl` abbreviation dictionary for name normalisation. Produces a `pivot_table.xlsx` summary alongside the deduplicated output. The modern `step0_dedup.py` (LSH + rapidfuzz) replaces this for new pipeline runs, but this file is kept for reference.

Key functions:
- `pivot_table()` — clusters records using `pandas_dedupe` + street-number blocking
- `fuzzy_deduplication()` — deduplicates by `cluster id`, writes pivot summary, returns merged DataFrame
- `city_de_duplication()` — top-level entry point used by the original pipeline

### `deduplicate_phonenumbers.py`

Standalone utility for deduplicating phone numbers across a range of Excel columns (e.g. `AG` through `AJ`) by Excel column letter. Strips formatting and country codes before comparing, then joins unique numbers with `, `. Not called by the current pipeline directly — used as a manual post-processing tool when needed.

### `final_name_matching.py`

Six-filter fuzzy name matching pipeline used to classify candidate pairs as `True_Match` or `Manual_Match`. Filters are applied in cascade order, each operating on the records not matched by the previous filter:

| Filter | Method | Threshold |
|--------|--------|-----------|
| 1 | `process.extractOne` + `fuzz.ratio` (word-level) | 88% per word |
| 2 | `fuzz.partial_ratio` | ≥ 92 |
| 3 | `fuzz.token_sort_ratio` | ≥ 90 |
| 4 | `fuzz.ratio` | ≥ 84 |
| 5 | `fuzz.WRatio` | ≥ 90 |
| 6 | `process.extractOne` + `fuzz.partial_ratio` (word-level) | 95% per word |

All filters normalise names through `string_filter1()`, which lowercases, strips punctuation, and expands abbreviations using `updated_pickle.pkl`. Results are written to `Exact_Matched_Records.xlsx`, `Manualy_Check_Matched_Records.xlsx`, and `Complete_Fuzzy_Matched_Records.xlsx`. The current pipeline uses `matching_orchestrator.py` + Groq instead, but this module is retained for manual review workflows.

### `final_sheet_creation.py`

Core output builder called by Step 5. Provides:
- `get_Business_Matched_Records()` — maps city + Bludot columns to the standard business schema
- `get_custom_matched_records()` — maps custom fields; returns default-value row indexes for formatting
- `get_contact_matched_records()` — extracts contact rows from matched/additional datasets
- `format_custom_subsheet()` — applies cell formatting to custom sheet rows that used default values

### `fuzzy_matching.py`

Fuzzy match orchestration utilities used by the original pipeline. Wraps the filters in `final_name_matching.py` and handles cross-dataset candidate generation. Retained for reference.

### `updated_pickle.pkl`

Binary pickle file containing an abbreviation expansion dictionary. Loaded by `rule_filter.py`, `de_duplication.py`, and `final_name_matching.py` to normalise business name tokens before comparison (e.g. `st` → `street`, `ave` → `avenue`, common business-type abbreviations).

---

## File Outputs

```
uploads/{CityName}/results/
├── city_data/
│   ├── manual_dedup_records.xlsx              ← After Step 0 (LSH dedup)
│   └── de_duplication_merged.xlsx             ← After Step 1 (column merge)
├── bludot_data/
│   └── bludot_concatenated_records.xlsx       ← After Step 0 (Bludot concat)
└── output/
    ├── final_result/                          ← Written by Step 3
    │   ├── final_matched_records_for_{city}.xlsx
    │   ├── additional_city_records_for_{city}.xlsx
    │   └── additional_bludot_records_for_{city}.xlsx
    ├── final_excel/                           ← Written by Steps 5 + 6
    │   ├── Business_Matched_Records.xlsx
    │   ├── Additional_Business_Matched_Records.xlsx
    │   ├── Custom_Matched_Records.xlsx
    │   ├── Additional_Custom_Matched_Records.xlsx
    │   ├── Contact_Matched_Records.csv
    │   └── Additional_Contact_Matched_Records.csv
    └── final_output/                          ← MAIN OUTPUT (multi-sheet Excel)
        ├── {city}_Business_Matched_Records.xlsx
        └── Additional_Matched_Records_Of_{city}.xlsx
```

---

## Frontend Pages

| Page | URL | Description |
|------|-----|-------------|
| Cities List | `/` | All cities with pipeline status |
| New City | `/city/new` | Upload files + configure column mapping |
| City Detail | `/city/{id}` | Pipeline tracker, stats, file links, action buttons |
| Dedup Results | `/city/{id}/dedup-results` | View + edit all LLM dedup decisions |
| Cluster Review | `/city/{id}/cluster-review` | Review uncertain near-miss clusters |
| Match Review Pass 1 | `/city/{id}/review` | Side-by-side match review (pass 1) |
| Match Review Pass 2 | `/city/{id}/review?pass=2` | Side-by-side match review (pass 2) |
| Matches Viewer | `/city/{id}/matches` | View all confirmed matched pairs |

---

## API Endpoints

### Cities

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/cities/` | List all cities |
| POST | `/cities/` | Create new city |
| GET | `/cities/{id}` | City details |
| GET | `/cities/{id}/status` | Current pipeline step + status |
| GET | `/cities/{id}/stats` | Match counts + breakdown |
| POST | `/cities/{id}/start` | Start pipeline |
| POST | `/cities/{id}/resume` | Resume after human review |

### Column Mapping

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/cities/{id}/suggest-mapping` | LLM suggests column mappings |
| POST | `/cities/{id}/column-mapping` | Save column mappings |

### Match Review

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/cities/{id}/review` | Get review queue (`?match_pass=2` for pass 2) |
| POST | `/cities/{id}/review/bulk` | Submit bulk decisions |
| POST | `/cities/{id}/review/{candidate_id}` | Submit single decision |

### Dedup Review

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/cities/{id}/dedup-review` | Pairs waiting for human decision |
| POST | `/cities/{id}/dedup-review/bulk` | Submit bulk dedup decisions |
| GET | `/cities/{id}/dedup-results` | All dedup pairs + decisions (viewer) |

### Cluster Review

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/cities/{id}/cluster-review` | Near-miss cluster groups |
| POST | `/cities/{id}/cluster-review/merge` | Merge clusters |
| POST | `/cities/{id}/cluster-review/keep-separate` | Mark as separate |

### Results

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/cities/{id}/matches` | All confirmed matched pairs |

---

## Key Design Decisions

### All LLM calls are batched (1 call per step)

All pairs for a given step are sent in a single prompt to Groq, which returns a JSON array of decisions. The full pipeline uses at most 4 Groq calls per city: up to 2 in Step 0 (intra-cluster + cross-cluster), 1 in Step 2, 1 in Step 4.

### Three-stage matching engine

Each candidate pair passes through three gates in order, stopping at the first decision:

1. **Rule filter** (`rule_filter.py`) — drops definite non-matches using street numbers and name/address similarity thresholds. Loads abbreviation expansions from `src/updated_pickle.pkl` if present.
2. **Pre-LLM rules** (`matching_orchestrator.py`) — auto-decides ~80% of remaining candidates with no API call.
3. **Groq LLM** — one batched call for genuinely ambiguous pairs only.

### src/ is the legacy support layer

The `src/` folder contains the original pipeline scripts that predated the FastAPI rewrite. `final_sheet_creation.py` and `contact_formatting.py` are still actively imported by Steps 5 and 6. The others (`de_duplication.py`, `final_name_matching.py`, `fuzzy_matching.py`, `bludot_concat.py`) are retained for reference and manual workflows but are no longer called by the automated pipeline.

### Soft human review gates

No step hard-locks the pipeline. Each gate checks if anything is pending; if nothing is, the pipeline auto-continues. Human action is only required when the LLM was genuinely uncertain.

### Step files are thin wrappers

`step2_match.py` and `step4_match.py` each contain a single function that calls into `matching_orchestrator.py`. All matching logic lives in the orchestrator and can be updated in one place.

### Address normalisation (Step 0)

Unit/suite identifiers are stripped before any comparison:

- `1011 W 4TH ST # 1/2` → `W 4TH ST`
- `3432 HILLCREST AVE STE 100` → `HILLCREST AVE`
- `3130 BALFOUR RD STE D PMB 277` → `BALFOUR RD`
- Street ranges collapsed: `2710-3040` → `2710`

---

## Changelog

### New: Step 6 — Contacts Deduplication (`step6_contacts.py`)

- Processes contact data (names, emails, phones) from Step 3 split files
- Reads `CONTACT_CONFIG` from `city_schema.json` for column definitions
- Handles dynamic `_1/_2/_3` column suffixes for multi-value fields
- Calls `format_contact_data()` + `clean_column_names()` from `src/contact_formatting.py`
- Replaces `BLK_\d+` placeholder values with `-`
- Appends results as new sheets to the existing final Excel files

### New: `src/` folder documented

The full `src/` support library is now part of the repo and documented. `src/final_sheet_creation.py` and `src/contact_formatting.py` are actively used by the pipeline. The remaining files (`de_duplication.py`, `final_name_matching.py`, `fuzzy_matching.py`, `bludot_concat.py`, `deduplicate_phonenumbers.py`) are legacy reference scripts retained for manual workflows.

### Refactored: Core step files are now separate modules

Each step (`step0` through `step6`) exposes a single `run_stepN()` entry point called by `pipeline.py`. Previously several steps were combined in one file.

### Updated: Step 3 now writes `final_matched_records_for_{city}.xlsx`

This matched-pairs file is now produced in Step 3 and consumed by Steps 5 and 6.

### Updated: Step 5 output structure

- Produces three sheet types (Business, Custom, Contact) for both matched and additional records
- Writes individual files to `final_excel/` and combined multi-sheet files to `final_output/`
- Resolves state abbreviations via `src/country_state_mapping.json`
- Monkey-patches `largest_num_list` and `earliest_date_list` to handle blank/text values without crashing

### Updated: Step 0 sub-step structure

- Logged as sub-steps 0.1–0.5 with per-step record counts
- Bulk insert helpers replace row-by-row `db.add()` for faster ingest
- Cross-cluster near-miss scan uses vectorised rapidfuzz `cdist`
- `NOT_DUPLICATE` LLM decisions are stored for human review alongside `UNCERTAIN`

### LLM: Gemini → Groq

- Replaced `google-generativeai`, `langgraph`, `langchain-core` with `groq>=0.11.0`
- Removed `prefect` — replaced with plain Python orchestration
- Model: `llama-3.3-70b-versatile` (free tier)
- All N pairs sent as 1 API call instead of N calls

### `rule_filter.py` — Expanded rule set

- Loads abbreviation dict from `src/updated_pickle.pkl` if present
- 6 explicit ordered rules: street-number conflict → name+address both dissimilar → both addresses blank → one address blank → street number missing → street numbers match
- `street_num_match` stored as `None` (indeterminate) when one or both numbers are absent

### `matching_orchestrator.py`

- Pre-LLM Stage A auto-decides ~80% of candidates before any API call
- `generate_candidates` returns `int` (count) instead of a list
- Pass 2 excludes records already matched in pass 1

### `docker-compose.yml`

- `user: "${UID:-1000}:${GID:-1000}"` — container runs as your local user, no locked folders
- `DATABASE_URL` points inside the mounted volume: `sqlite:////app/backend/bludot_pipeline.db`
- `./backend/uploads:/app/uploads` volume mount added

### `requirements.txt`

- Removed: `google-generativeai==0.5.4`, `langgraph==0.1.4`, `langchain-core==0.2.5`, `prefect==2.19.3`
- Added: `groq>=0.11.0`

---

## Troubleshooting

**Folders appear locked in the file manager**
```bash
sudo chown -R $USER:$USER ~/bludot_pipeline/backend/uploads/
```

**Backend won't start — PermissionError on uploads folder**
```bash
mkdir -p backend/uploads
docker-compose down && docker-compose up
```

**Backend won't start — unable to open database file**
```bash
# Verify docker-compose.yml has:
# DATABASE_URL=sqlite:////app/backend/bludot_pipeline.db
docker-compose down && docker-compose up
```

**ModuleNotFoundError: No module named 'groq'**
```bash
docker-compose build --no-cache backend
docker-compose up
```

**Step 5 or Step 6 raises ImportError for `src.*`**
- Ensure the entire `backend/src/` folder is present with all files listed in [src/ Support Library](#src-support-library)
- The `src/` folder must be at `backend/src/`, not inside `backend/core/`

**Step 5 raises FileNotFoundError for `city_schema.json`**
- Run `step5_support_generate_city_details.py` once for this city before the pipeline reaches Step 5
- The schema must be at `uploads/{CityName}/city_schema.json`

**Step 5 crashes on `largest_num_list` or `earliest_date_list`**
- This happens when a column contains blank or text values where numbers/dates are expected
- The monkey-patch in `step5_output.py` handles this — confirm you are running the latest version of that file

**Step 6 produces 0 contact rows**
- Check that `CONTACT_CONFIG` is populated in `city_schema.json`
- Verify that `person_col` values in the config match actual column names in your city sheet

**`updated_pickle.pkl` causes errors on load**
- The file must be at `backend/src/updated_pickle.pkl`
- If it is corrupted, `rule_filter.py` and `de_duplication.py` will fall back gracefully (empty abbreviation dict) but name normalisation quality will be reduced

**Review page shows "No items to review" even when pipeline is paused**
- Check you have the latest `ReviewPage.jsx` — old versions always loaded pass 1
- Pass 2 review URL must be `/city/{id}/review?pass=2`

**Output file has 0 matched records**
- Delete the city entry and create a new one — old DB data may reference wrong field names
- Confirm `final_matched_records_for_{city}.xlsx` was written by Step 3 before Step 5 runs

**Groq rate limit or quota exhausted**
- Free tier: 14,400 requests/day, 500,000 tokens/day
- All quota-exhausted pairs are marked UNCERTAIN and routed to human review
- Resume after completing human review — no pairs are re-sent to Groq
- Quota resets daily at midnight UTC