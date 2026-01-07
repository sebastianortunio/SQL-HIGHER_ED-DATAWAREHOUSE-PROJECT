# SQL Higher Education Data Warehouse (Bronze → Silver → Gold)

This project implements a SQL Server data warehouse for higher education analytics using a **Bronze → Silver → Gold** architecture.

The pipeline ingests operational datasets from CSV files into a **Bronze (raw)** layer, cleans and conforms data into a **Silver (trusted)** layer, and publishes analytics-ready **Gold star-schema views** (dimensions + facts) for reporting and BI tools.

# Architecture

**Database:** `College_DataWarehouse` (SQL Server)

**Schemas / Layers**
- **bronze:** raw landing tables loaded from CSVs via `BULK INSERT`
- **silver:** cleansed and conformed tables with standardized values and constraints
- **gold:** analytics layer built as star-schema **views** over Silver

**Load Flow**
1. Run Bronze DDL (create tables)
2. Run `bronze.load_bronze` (CSV → Bronze)
3. Run Silver DDL (create tables)
4. Run `silver.load_silver` (Bronze → Silver)
5. Run Gold views script (publish star schema)

# Data Model

## Gold Dimensions (Views)
- `gold.dim_students`
- `gold.dim_staff`
- `gold.dim_terms`
- `gold.dim_courses`
- `gold.dim_programs`
- `gold.dim_program_learning_outcomes`

## Gold Facts (Views)
- `gold.fact_enrollments`
- `gold.fact_course_grades`
- `gold.fact_assessment_results`
- `gold.fact_advising_interactions`
- `gold.fact_survey_responses`

# Layers

## Bronze Layer

**Purpose:** Store source datasets as-is.

**Bronze Tables**
- `bronze.staff`
- `bronze.terms`
- `bronze.course`
- `bronze.programs`
- `bronze.program_learning_outcomes`
- `bronze.students`
- `bronze.enrollments`
- `bronze.advising_interactions`
- `bronze.assessment_results`
- `bronze.course_grades`
- `bronze.survey`

**Load Procedure**
- `bronze.load_bronze`
  - truncates Bronze tables
  - reloads data from CSV files using `BULK INSERT`

## Silver Layer

**Purpose:** Clean and conform data for reliable downstream reporting.

**Transformations Applied**
- Whitespace cleanup using `TRIM`
- Deduplication with `ROW_NUMBER()`
- Type normalization (e.g., BIT flags)
- Value standardization (e.g., gender normalization)
- Term code normalization using `CASE` mappings

**Silver Tables**
- `silver.staff`
- `silver.terms`
- `silver.course`
- `silver.programs`
- `silver.program_learning_outcomes`
- `silver.students`
- `silver.enrollments`
- `silver.advising_interactions`
- `silver.assessment_results`
- `silver.course_grades`
- `silver.survey`

**Load Procedure**
- `silver.load_silver`
  - truncates Silver tables
  - reloads from Bronze with transformations

## Gold Layer

**Purpose:** Provide analytics-ready star schema outputs.

**Implementation:** Views built on Silver tables.

**Key Features**
- Enriched dimension attributes (e.g., full names, term season/year parsing)
- Facts joined to dimensions using business keys
- Date-based term mapping for survey and advising facts via `OUTER APPLY` (using term start/end dates)

# Datasets

The Bronze load procedure expects CSV files with headers:

- `advising_interactions.csv`
- `assessment_results.csv`
- `courses.csv`
- `course_grades.csv`
- `enrollments.csv`
- `program_learning_outcomes.csv`
- `programs.csv`
- `staff.csv`
- `students.csv`
- `surveys.csv`
- `terms.csv`

# Setup & Requirements

## Requirements
- Microsoft SQL Server
- SSMS (SQL Server Management Studio)
- SQL Server file access to the CSV directory (permissions required)
- Database created: `College_DataWarehouse`

## Schemas
The scripts create schemas automatically if missing:
- `bronze`
- `silver`
- `gold`

# How to Run

## 1. Create database (if not already created)

```sql
CREATE DATABASE College_DataWarehouse;
