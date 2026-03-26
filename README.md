# Metadata-Driven Ingestion Framework (Databricks PoC)

## Overview

This project demonstrates a simple metadata-driven ingestion framework built using Azure Databricks and ADLS Gen2.

The solution dynamically ingests multiple data sources into a Raw layer and then loads them into a Data Hub layer in Delta format.

The goal is to showcase a reusable ingestion pattern where new data sources can be onboarded by updating metadata rather than modifying code.

---

## Architecture

The solution follows a simplified data lake architecture:

- **Landing**: Simulated source systems (CSV, JSON)
- **Metadata**: Delta table defining ingestion configuration
- **Raw**: Stores ingested data as-is
- **Data Hub**: Stores curated data in Delta format

Flow:

Landing → Metadata-driven ingestion → Raw → Data Hub (Delta)

---

## Metadata-Driven Ingestion

The ingestion logic is driven by a metadata table stored in Delta format.

Each row defines:
- Source name
- File format (CSV, JSON)
- Source path
- Target Raw path
- Data Hub target
- Activation flag

This allows new sources to be added without changing the ingestion code.

---

## Notebooks

- **00_setup_storage**  
  Configures access to ADLS Gen2 using Spark configuration.

- **01_create_sample_data**  
  Generates sample datasets and stores them in the Landing layer.

- **02_create_metadata**  
  Creates the metadata configuration as a Delta table.

- **03_ingestion_framework**  
  Implements the metadata-driven ingestion into the Raw layer.

- **04_datahub_load**  
  Loads data from Raw into the Data Hub in Delta format.

---

## Technologies

- Azure Databricks (PySpark)
- Azure Data Lake Storage Gen2 (ADLS)
- Delta Lake

---

## Design Decisions

- Metadata-driven approach to enable scalability and flexibility
- Delta format in Data Hub for reliability and performance
- Direct ADLS access (`abfss://`) instead of mounting for simplicity and modern practice
- Full overwrite strategy used for simplicity in this PoC

---

## Limitations
- Authentication uses storage account keys (simplified for PoC)
- Minimal transformation logic in Data Hub

---
## How to run: 

### Prerequisites

- Azure subscription
- Azure Data Lake Storage Gen2 account
- Azure Databricks workspace

### Storage Setup

Create the following containers in ADLS Gen2:

- landing
- raw
- metadata
- datahub

### Databricks Setup

1. Import the notebooks into your Databricks workspace:
   - 00_setup_storage
   - 01_create_sample_data
   - 02_create_metadata
   - 03_ingestion_framework
   - 04_datahub_load

2. Configure storage access in `00_setup_storage`:
   - Set your storage account name
   - Use a storage account key or secret scope

3. Attach a cluster to the notebooks

### Execution

You can run the solution in two ways:

**Option 1 — Manual execution:**
1. Run `00_setup_storage`
2. Run `01_create_sample_data`
3. Run `02_create_metadata`
4. Run `03_ingestion_framework`
5. Run `04_datahub_load`

**Option 2 — Databricks Workflow:**
- Trigger the configured job to execute the full pipeline automatically
